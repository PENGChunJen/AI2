import json, csv, codecs
import time
from datetime import date, timedelta
from collections import deque

from file2log import generateLogs 
from log2data import generateData, generateDataFromJob 
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

def generateJobs(logs):
    usersLog = defaultdict(list)
    #Split by User
    for log in logs:
        usersLog[ log['user'] ].append(log)
    jobList = usersLog.values()
    print('Split into %d jobs for multiprocessing ...'%(len(jobList)))
    return usersLog 

def bulkUpdate( logList, userDataList, dataList ):
    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    
    actions = []
    for log in logList:
        idStr = '%s_%s_%s'%(log['timestamp'].isoformat(),
                            log['user'],log['service'])
        action = {
            '_op_type': 'index',
            '_index'  : indexName,
            '_type'   : 'log',
            '_id'     : idStr, 
            '_source' : log
        }
        actions.append(action)
        if len(actions) == 5000:
            helpers.bulk(es,actions)
            del actions[0:len(actions)]
    
    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread))
    helpers.bulk(es,actions)
    del actions[0:len(actions)]
  

    for userData in userDataList:
        action = {
            '_op_type': 'index',
            '_index'  : indexName,
            '_type'   : 'userData',
            '_id'     : userData['user'], 
            '_source' : userData,
        }
        actions.append(action)
        if len(actions) == 5000:
            helpers.bulk(es,actions)
            del actions[0:len(actions)]

    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread))
    helpers.bulk(es,actions)
    del actions[0:len(actions)]

    for data in dataList:
        idStr = '%s_%s_%s'%(log['timestamp'].isoformat(),
                            log['user'],log['service'])
        action = {
            '_op_type': 'index',
            '_index'  : indexName,
            '_type'   : 'data',
            '_id'     : idStr, 
            '_source' : data 
        }
        actions.append(action)
        if len(actions) == 5000:
            helpers.bulk(es,actions)
            del actions[0:len(actions)]
    
    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread))
    helpers.bulk(es,actions)
    del actions[0:len(actions)]

    #es.indices.refresh(index=indexName)


def run():
    fileNameList = generateFileNameList(startDate, endDate)
    for fileName in fileNameList:
        print('Loading %s ... (ETA:30s)'%(fileName))
        
        logs = generateLogs(fileName) 
        userDataList = []
        dataList = []
        for log in logs:
            print('%s %s'%(log['timestamp'], log['user']))
            userData, data = generateData(log) 
            data = generateScore(data)

            userDataList.append( userData )
            dataList.append( data )

        bulkUpdate( logs, userDataList, dataList )


def doJob(job):
    userData, dataList = generateDataFromJob(job) 
    dataList = [generateScore(data) for data in dataList]
    return userData, dataList

def runParallel():
    fileNameList = generateFileNameList(startDate, endDate)
    for fileName in fileNameList:
        print('Loading %s ... (ETA:30s)'%(fileName))
        
        logs = generateLogs(fileName) 
        jobList = generateJobs(logs)
        
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes = pool_size)
        results = pool.map_async(doJob, jobList, chunksize=1)
        pool.close()
        pool.join()       

        
        bulkUpdate( logs, userDataList, dataList )


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
    
    run()

