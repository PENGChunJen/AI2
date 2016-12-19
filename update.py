import cPickle
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from log2data import computeTimesFeatures

indexName = 'ai2_v2.0'
hosts = ['localhost:9200']
maxThread = 100

userRecordsFrom = 0
totalUserRecords = 1 
recordsPerQuery = 5000

updateTime = datetime.now()

def genUserQueryDSL(fromNum):
    queryDSL = {
        "from": fromNum,
        "size": recordsPerQuery,
        "query": {
            "match_all": {}
        }
    }
    return queryDSL

def getUserRecords():
    global userRecordsFrom, totalUserRecords
    if userRecordsFrom <= totalUserRecords:
        print "retrieving userData from %d to %d" % (userRecordsFrom, userRecordsFrom + recordsPerQuery)
        es = Elasticsearch(hosts=hosts)
        res = es.search(index=indexName, doc_type="userData", body=genUserQueryDSL(userRecordsFrom))
        totalUserRecords = res['hits']['total']
        userRecordsFrom += recordsPerQuery
        return res['hits']['hits']
    else:
        return None

def updateUser(userData):
    es = Elasticsearch(hosts=hosts)
    es.index(index=indexName, doc_type='userData', id=userData['user'], body={ 'blob': cPickle.dumps(userData) })

def correctDatum(userData, log):
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

    idStr = '%s_%s_%s'%(data['log']['timestamp'].isoformat(), data['log']['user'],data['log']['service'])
    es = Elasticsearch(hosts=hosts)
    es.index(index=indexName, doc_type='data', id=idStr, body=data)

def correctData(userData, attackTimestamp):
    for dayKey in userData['timestamps'].keys():
        day = datetime.strptime(dayKey, "%Y-%m-%d")
        dayTime = attackTimestamp.replace(year=day.year, month=day.month, day=day.day)
        if dayTime >= attackTimestamp:
            for log in userData['timestamps'][dayKey]:
                if log['timestamp'] > attackTimestamp:
                    correctDatum(userData, log)

if __name__ == '__main__':
    userRecords = getUserRecords()
    while userRecords:
        for userRecord in userRecords:
            userData = cPickle.loads(str(userRecord['_source']['blob']))
            if userData['attackTimestamp'] != None:
                correctData(userData, userData['attackTimestamp'])
                userData['attackTimestamp'] = None
            userData['stableTraingTimestamp'] = updateTime
            updateUser(userData)

        userRecords = getUserRecords()
