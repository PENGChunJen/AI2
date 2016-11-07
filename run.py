import json, csv, codecs
import time
from datetime import date, timedelta
from collections import deque

from file2log import generateLogs, generateJobs
from log2data import generateUserData, generateData 
from data2score import generateScore

from elasticsearch import Elasticsearch, helpers

# Global vairables
indexName = 'ai2_v2.0'
#indexName = 'ai2_test'
startDate = date(2016,6,1) 
endDate = date(2016,6,3) 
whiteList = ['140.112.*', '209,85,*']
logPath = 'rawlog'

def dateGenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

def generateFileNameList(startDate, endDate):
    filename_list = []
    for d in dateGenerator(startDate, endDate):
        filename = '%s/all-%s-geo.log'%(logPath,d.strftime('%Y%m%d'))
        filename_list.append(filename)
    return filename_list 

def getUserData(user, es):
    res = es.get( index = indexName, 
                  doc_type = 'userData',
                  id = user,
                  ignore = [404]
          )
    newUser = False
    if not res['found']:
        print 'new User'
        newUser = True
        userData = generateUserData(user)
    else:
        print ''
        userData = res['_source']
        
    return userData, newUser

def bulkUpdate( log, userData, newUser, data, es ):
    actions = []
    idStr = '%s_%s_%s'%(log['timestamp'].isoformat(),
                        log['user'],log['service'])
    actions.append({
        '_op_type': 'index',
        '_index'  : indexName,
        '_type'   : 'log',
        '_id'     : idStr, 
        '_source' : log
    })

    #op = 'index' if newUser else 'update'
    actions.append({
        '_op_type': 'index',
        '_index'  : indexName,
        '_type'   : 'userData',
        '_id'     : log['user'], 
        '_source' : userData,
        
    })

    actions.append({
        '_op_type': 'index',
        '_index'  : indexName,
        '_type'   : 'data',
        '_id'     : idStr, 
        '_source' : data 
    })

    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread))
    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread)), maxlen=100000)
    
    helpers.bulk(es,actions)
    #es.indices.refresh(index=indexName)

def run(es):
    fileNameList = generateFileNameList(startDate, endDate)
    for fileName in fileNameList:
        print('Loading %s ... (ETA:30s)'%(fileName))
        inputFile = codecs.open(fileName, 'r', 
                        encoding='ascii', errors='ignore') 
        logLists = list(csv.reader(inputFile))
        print('Total Number of logs: %d'%(len(logLists)))
        
        logs = generateLogs(logLists) 
        #jobList = generateJobs(logLists)

        for log in logs:
            print log['timestamp'].isoformat(), log['user'],

            userData, newUser = getUserData( log['user'], es )
            #print json.dumps(userData, indent=4)
            data, userData = generateData(log, userData) 

            data = generateScore(data)

            bulkUpdate( log, userData, newUser, data, es )


if __name__ == '__main__':
    # Define a defualt Elasticsearch client
    hosts = ['192.168.1.1:9200',
             '192.168.1.2:9200',
             '192.168.1.3:9200',
             '192.168.1.4:9200',
             '192.168.1.5:9200',
             '192.168.1.6:9200',
             '192.168.1.10:9200']
    maxThread = 100
    #client = connections.create_connection(hosts=hosts, maxsize=maxThread)
    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    es.indices.create(index=indexName,ignore=[400])
    
    run(es)

