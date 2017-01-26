import cPickle
import time
import multiprocessing
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from sys import stdout

import config
from log2data import computeTimesFeatures
from data2score import generateScore, generateScoreList
from run import bulkIndex

recordsPerQuery = 1000

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

def correctData(userData, attackTimestamp):
    dataList = []
    for dayKey in userData['timestamps'].keys():
        day = datetime.strptime(dayKey, "%Y-%m-%d")
        dayTime = attackTimestamp.replace(year=day.year, month=day.month, day=day.day)
        if dayTime >= attackTimestamp:
            for log in userData['timestamps'][dayKey]:
                if log['timestamp'] > attackTimestamp:
                    featureVector = [0.0 for x in xrange(24)]
                    datum = {
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

                    dataList.append(datum)
    return dataList

def updateUserDataAndData(userRecord):
    userData = cPickle.loads(str(userRecord['_source']['blob']))
    if userData['attackTimestamp'] != None:
        dataList = correctData(userData, userData['attackTimestamp'])
    else:
        dataList = []
    userData['attackTimestamp'] = None
    userData['stableTraingTimestamp'] = updateTime

    return userData, dataList

def parallelUpdate(userRecords):
    poolSize = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes = poolSize)
    results = pool.map_async(updateUserDataAndData, userRecords, chunksize=1)
    start_time = time.time()
    jobNum = len(userRecords)
    while (True):
        if(results.ready()): break
        stdout.write('Used %.2f seconds, %5d/%d jobs left ... %10s \r'
            % (time.time()-start_time, results._number_left, jobNum, ''))
        stdout.flush()
        time.sleep(0.5)
    elapsed_time = time.time()-start_time
    stdout.write('Used %.2f seconds, Processed %5d users, Avg: %.2f logs/sec %50s \n'
        %(elapsed_time, jobNum, jobNum/float(elapsed_time), ''))

    pool.close()
    pool.join()

    userDataList = []
    allDataList = []
    for userData, dataList in results.get():
        userDataList.append(userData)
        allDataList.extend(dataList)

    if allDataList:
        allDataList = generateScoreList(allDataList)
    bulkIndex([], userDataList, allDataList)

if __name__ == '__main__':
    print "retrieving userData..."
    es = Elasticsearch(hosts=config.hosts)
    res = es.search(index=config.indexName, doc_type="userData", body=genUserQueryDSL(0), scroll="1m")
    scroll_id = res['_scroll_id']
    userRecords = res['hits']['hits']
    totalRecords = res['hits']['total']
    currentUserRecords = recordsPerQuery if recordsPerQuery < totalRecords else totalRecords
    parallelUpdate(userRecords)
    while userRecords and currentUserRecords < totalRecords:
        print "retrieving userData %d/%d" % (currentUserRecords, totalRecords)
        res = es.scroll(scroll_id=scroll_id, scroll="1m")
        userRecords = res['hits']['hits']
        currentUserRecords += recordsPerQuery

        parallelUpdate(userRecords)
