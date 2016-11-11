import marshal
from elasticsearch import Elasticsearch
# Define a defualt Elasticsearch client
indexName = 'ai2_v2.0'
hosts = ['192.168.1.1:9200',
         '192.168.1.2:9200',
         '192.168.1.3:9200',
         '192.168.1.4:9200',
         '192.168.1.5:9200',
         '192.168.1.6:9200',
         '192.168.1.10:9200']
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
        userData = res['_source']
        
    return userData

def update( Dict, key, value ):
    if key in Dict:
        Dict[key].append(value)
    else:
        Dict[key] = [value]

#TODO
def generateData(log, userData):
    featureVector = [0.0 for x in xrange(24)]
    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':{},
        'label':log['label']
    }
     
    update(userData['services'], log['service'], log['timestamp'])
    update(userData['IPs'], log['IP'], log['timestamp'])
    update(userData['devices'], log['device'], log['timestamp'])
    update(userData['cities'], log['city'], log['timestamp'])
    update(userData['counties'], log['county'], log['timestamp'])
    update(userData['nations'], log['nation'], log['timestamp'])
    update(userData['timestamps'], log['timestamp'].date().isoformat(), log) #??

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
