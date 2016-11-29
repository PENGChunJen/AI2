import cPickle
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
# Define a defualt Elasticsearch client
indexName = 'ai2_v2.0'
'''
hosts = ['192.168.1.1:9200',
         '192.168.1.2:9200',
         '192.168.1.3:9200',
         '192.168.1.4:9200',
         '192.168.1.5:9200',
         '192.168.1.6:9200',
         '192.168.1.10:9200']
'''
hosts = ['localhost:9200']
maxThread = 100

def generateUserData(user):
    userData = {
        'user':user,
        'services':{},
        'IPs':{},
        'devices':{},
        'cities':{},
        'counties':{},
        'nations':{},
        'timestamps':{}
    }
    return userData

def getUserData(user):
    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    res = es.get( index = indexName, 
                  doc_type = 'userData',
                  id = user,
                  ignore = [404]
          )
    if not res['found']:
        userData = generateUserData(user)
    else:
        userData = cPickle.loads(str(res['_source']['blob']))
        
    return userData

def update( Dict, key, value ):
    if key in Dict:
        Dict[key].append(value)
    else:
        Dict[key] = [value]

def computeTimesFeatures(userData, log):
    presentTimePoint = log['timestamp']
    day1TimePoint = presentTimePoint- timedelta(days=1)
    day3TimePoint = presentTimePoint- timedelta(days=3)
    day7TimePoint = presentTimePoint- timedelta(days=7)
    day1Count, day3Count, day7Count = 0, 0, 0

    for dayKey in userData['timestamps'].keys():
        day = datetime.strptime(dayKey, "%Y-%m-%d")
        dayTime = presentTimePoint.replace(year=day.year, month=day.month, day=day.day)

        if dayTime > day7TimePoint:
            day7Count += 1
            for pastLog in userData['timestamps'][dayKey]:
                time = pastLog['timestamp']
                if time > day3TimePoint:
                    day3Count += 1
                    if time > day1TimePoint:
                        day1Count += 1

    delta = 0.000001
    past1dayMean = day1Count
    past3daysMean = day3Count / 3.0
    past7daysMean = day7Count / 7.0

    return ((past1dayMean - past3daysMean) / (past3daysMean + delta)), ((past1dayMean - past7daysMean) / (past7daysMean + delta)), ((past3daysMean - past7daysMean) / (past7daysMean + delta))

def generateData(log, userData):
    featureVector = [0.0 for x in xrange(24)]
    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':{},
        'label':log['label']
    }

    featureVector[:3] = computeTimesFeatures(userData, log)
    featureVector[3] = 0.0 if log['device'] in userData['devices'] else 1.0
    featureVector[4] = 0.0 if log['IP'] in userData['IPs'] else 1.0
    featureVector[5] = 0.0 if log['city'] in userData['cities'] else 1.0
    featureVector[6] = 0.0 if log['county'] in userData['counties'] else 1.0
    featureVector[7] = 0.0 if log['nation'] in userData['nations'] else 1.0
     
    update(userData['services'], log['service'], log['timestamp'])
    update(userData['IPs'], log['IP'], log['timestamp'])
    update(userData['devices'], log['device'], log['timestamp'])
    update(userData['cities'], log['city'], log['timestamp'])
    update(userData['counties'], log['county'], log['timestamp'])
    update(userData['nations'], log['nation'], log['timestamp'])
    update(userData['timestamps'], log['timestamp'].date().isoformat(), log)

    return userData, data

def generateDataFromJob(userDataTuple):
    userId = userDataTuple[0]
    logList = userDataTuple[1]
    
    userData = getUserData(userId)
    
    dataList = []
    for log in logList:
        userData, data = generateData(log, userData)
        dataList.append(data)

    return userData, dataList
