import cPickle, json

import config
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

def generateUserData(user):
    userData = {
        'user':user,
        'services':{},
        'IPs':{},
        'devices':{},
        'cities':{},
        'counties':{},
        'nations':{},
        'timestamps':{},
        'attackTimestamp': None,
        'stableTraingTimestamp': None
    }
    return userData

def getUserData(user):
    es = Elasticsearch(hosts=config.hosts, maxsize=config.maxThread)
    res = es.get( index = config.indexName, 
                  doc_type = 'userData',
                  id = user,
                  ignore = [404]
          )
    if not res['found']:
        userData = generateUserData(user)
    else:
        userData = cPickle.loads(str(res['_source']['blob']))
        
    return userData
'''
def update( Dict, key, value ):
    if key in Dict:
        Dict[key].append(value)
    else:
        Dict[key] = [value]

def computeTimesFeatures(userData, log):
    presentTimePoint = log['timestamp']
    day1TimePoint = presentTimePoint - timedelta(days=1)
    day3TimePoint = presentTimePoint - timedelta(days=3)
    day7TimePoint = presentTimePoint - timedelta(days=7)
    day8TimePoint = presentTimePoint - timedelta(days=7)
    day1Count, day3Count, day7Count = 0, 0, 0

    for dayKey in userData['timestamps'].keys():
        day = datetime.strptime(dayKey, "%Y-%m-%d")
        dayTime = presentTimePoint.replace(year=day.year, month=day.month, day=day.day)

        if dayTime > day8TimePoint:
            for pastLog in userData['timestamps'][dayKey]:
                time = pastLog['timestamp']
                if time > day7TimePoint:
                    day7Count += 1
                    if time > day3TimePoint:
                        day3Count += 1
                        if time > day1TimePoint:
                            day1Count += 1

    delta = 0.000001
    past1dayMean = day1Count
    past3daysMean = day3Count / 3.0
    past7daysMean = day7Count / 7.0

    return ((past1dayMean - past3daysMean) / (past3daysMean + delta)), ((past1dayMean - past7daysMean) / (past7daysMean + delta)), ((past3daysMean - past7daysMean) / (past7daysMean + delta))
'''

def generateTimeRange( log, start, end ):
    req_body = {
        "query": {
            "bool": {
                "must":[
                    {"match": { "user": log["user"]} },
                    {"match": { "service" : log["service"]} },
                ],
                "filter": [
                    {
                        "range": {
                            "timestamp": {
                                "from": start,
                                "to"  : end 
                            }
                        }
                    }
                ]
            }
        }
    }
    return req_body

def countTimesFeatures(log, es):
    request = []
    head = { 'index':config.indexName, 'type':'log' }

    timestamp = log['timestamp']
    for d in [1, 3, 7]:
        start = timestamp - timedelta(days = d)
        body = generateTimeRange(log, start, timestamp)
        request.extend([head, body])

    responses = es.msearch(body=request)
    pastCounts = [ float(response["hits"]["total"]) for response in responses["responses"] ]
    
    past1daysCount = pastCounts[0]
    past3daysCount = pastCounts[1]
    past7daysCount = pastCounts[2]

    delta = 1e-6
    past1dayMean = past1daysCount
    past3daysMean = past3daysCount/3.0
    past7daysMean = past7daysCount/7.0


    return [((past1dayMean - past3daysMean) / (past3daysMean + delta)), 
            ((past1dayMean - past7daysMean) / (past7daysMean + delta)), 
            ((past3daysMean - past7daysMean) / (past7daysMean + delta))]


def generateRequestBody(user, field, match_string):
    body = {
        "query": {
            "bool": {
                "must": [
                    { "match": { field: match_string } }
                ],
                "filter": [ 
                    { "term": { "user":user } },
                ]
            }
        },
        "size":1,
        "terminate_after":1
    }
    return body

def checkExistenceFeatures(log, es):
    checkFeatures = ['device', 'IP', 'city', 'county', 'nation']
    request = []
    head = { 'index':config.indexName, 'type':'log' }
    
    for feature in checkFeatures:
        body = generateRequestBody( log['user'], feature, log[feature] )
        request.extend([head, body])

    responses = es.msearch(body=request)
    features = [ float(response["hits"]["total"] > 0) for response in responses["responses"] ]
    return features

def generateData(log):
    es = Elasticsearch(hosts=config.hosts, maxsize=config.maxThread)
    featureVector = [0.0 for x in xrange(24)]
    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':{},
        'label':log['label']
    }

    featureVector[:3] = countTimesFeatures(log, es)
    featureVector[3:8] = checkExistenceFeatures(log, es)

    return data

def generateDataFromJob(userDataTuple):
    userId = userDataTuple[0]
    logList = userDataTuple[1]
    
    dataList = []
    for log in logList:
        data = generateData(log)
        dataList.append(data)

    return dataList
