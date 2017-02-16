import cPickle, json
import numpy as np

import config
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

def getUserData( user ):
    return []

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
                ],
                "must_not": {
                    "match": {
                        "label.analyst": "abnormal",
                    }
                }
            }
        }
    }
    return req_body

def countTimesFeatures(log, es, sameBatchList):
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

    past1daysCount += sameBatchList['service'].count( log['service'] ) + 1.0

    delta = 1e-6
    past1dayMean = past1daysCount
    past3daysMean = past3daysCount/3.0
    past7daysMean = past7daysCount/7.0

    timeArray = np.zeros(3)

    if past3daysMean > 0:
        timeArray[0] = ((past1dayMean - past3daysMean) / past3daysMean )
    else:
        timeArray[0] = past1dayMean

    if past7daysMean > 0:
        timeArray[1] = ((past1dayMean - past7daysMean) / past7daysMean ) 
        timeArray[2] = ((past3daysMean - past7daysMean) / past7daysMean ) 
    else:
        timeArray[1] = past1dayMean
        timeArray[2] = past3daysMean 

    norm = np.linalg.norm(timeArray) 
    if not ( np.isnan( norm ) or (norm == 0) ):
        timeArray = timeArray / norm

    return timeArray


def generateRequestBody(user, field, match_string):
    body = {
        "query": {
            "bool": {
                "must": [
                    { "match": { field: match_string } }
                ],
                "filter": [ 
                    { "term": { "user":user } },
                ],
                "must_not": {
                    "match": {
                        "label.analyst": "abnormal",
                    }
                }
            }
        },
        "size":1,
        "terminate_after":1
    }
    return body

def checkExistenceFeatures(log, es, sameBatchList):
    checkFeatures = ['device', 'IP', 'city', 'county', 'nation']
    request = []
    head = { 'index':config.indexName, 'type':'log' }
    
    for feature in checkFeatures:
        body = generateRequestBody( log['user'], feature, log[feature] )
        request.extend([head, body])

    responses = es.msearch(body=request)
    features = [ float(response["hits"]["total"] > 0) for response in responses["responses"] ]


    for i in xrange(len(checkFeatures)):
        feature = checkFeatures[i]
        if log[feature] not in sameBatchList[feature]:
            features[i] = 1.0

    return features

def generateData(log, sameBatchList):
    es = Elasticsearch(hosts=config.hosts, maxsize=config.maxThread)
    featureVector = [0.0 for x in xrange(24)]

    featureVector[:3] = countTimesFeatures(log, es, sameBatchList)
    featureVector[3:8] = checkExistenceFeatures(log, es, sameBatchList)
    #featureVector[1] = featureVector[0]

    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':{},
        'label':log['label']
    }

    return data


def generateDataFromJob(userDataTuple):
    userId = userDataTuple[0]
    logList = userDataTuple[1]
    
    sameBatchList = {} 
    for k in logList[0]:
        sameBatchList[k] = [] 

    dataList = []
    for log in logList:
        data = generateData(log, sameBatchList)
        dataList.append(data)

        for k in log:
            sameBatchList[k].append( log[k] )

    return dataList

if __name__ == '__main__':
    timeArray = np.array([100000, 100000, 0])
    print timeArray / np.linalg.norm(timeArray)

