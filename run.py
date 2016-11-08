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

def generateJobs(logs):
    usersLog = defaultdict(list)
    #Split by User
    for log in logs:
        usersLog[ log['user'] ].append(log)
    jobList = usersLog.values()
    print('Split into %d jobs for multiprocessing ...'%(len(jobList)))
    return usersLog 

def bulkUpdate( logList, userDataList, dataList, es ):
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
        
        logs = generateLogs(fileName) 
        #jobList = generateJobs(logLists)

        for log in logs:
            data, userData = generateData(log) 
            data = generateScore(data)
            bulkUpdate( log, userData, newUser, data, es )

def doWork(job):
    userData, dataList = generateDataFromJob(job) 
    dataList = [generateScore(data) for data in dataList]
    return userData, dataList

def doJob(jobList):
    return [doWork(job) for job in jobList] 

def runParallel(es):
    fileNameList = generateFileNameList(startDate, endDate)
    for fileName in fileNameList:
        print('Loading %s ... (ETA:30s)'%(fileName))
        
        logs = generateLogs(fileName) 
        jobList = generateJobs(logs)

        
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes = pool_size)
        results = pool.map_async(doJob, jobList, chunksize=1)
        pool.close()
       
        results.get() 
        data, userData = generateData(log) 

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

