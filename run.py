import json, csv, codecs
import time
from datetime import date, timedelta
from collections import deque
from sys import stdout

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

def generateBulkActions( logList, userDataList, dataList ):
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
    for userData in userDataList:
        action = {
            '_op_type': 'index',
            '_index'  : indexName,
            '_type'   : 'userData',
            '_id'     : userData['user'], 
            '_source' : userData,
        }
        actions.append(action)
    for data in dataList:
        idStr = '%s_%s_%s'%(data['log']['timestamp'].isoformat(),
                            data['log']['user'],data['log']['service'])
        action = {
            '_op_type': 'index',
            '_index'  : indexName,
            '_type'   : 'data',
            '_id'     : idStr, 
            '_source' : data 
        }
        actions.append(action)
    
    return actions 

def printStatus( actionsNum, totalActions, docNum ):
    #stdout.write('Bulk indexing... %7d actions left, doc_count:%d \n'
    stdout.write('Bulk indexing... %7d/%7d actions left, doc_count:%d \r'
                %(actionsNum, totalActions, docNum))
    stdout.flush()

def bulkIndex( logList, userDataList, dataList ):
    print('\nBulk Indexing ...') 
    print('number of logs    :%d'%len(logList))
    print('number of userData:%d'%len(userDataList))
    print('number of data    :%d'%len(dataList))

    actions = generateBulkActions( logList, userDataList, dataList )
    totalActions = len(actions)

    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    setting = {"index":{"refresh_interval":"-1"}}
    es.indices.put_settings(index=indexName, body=setting)

    while len(actions) > chunkSize:
        docNum = es.count(index=indexName)['count']
        printStatus( len(actions), totalActions, docNum )
        successNum, item = helpers.bulk(es,actions[:chunkSize], refresh='false', request_timeout=60)
        del actions[:chunkSize]
 
    
    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread, chunk_size=1000, request_timeout=30))
    successNum, item = helpers.bulk(es,actions)
    del actions[:len(actions)]

    setting = {"index":{"refresh_interval":"1s"}}
    es.indices.put_settings(index=indexName, body=setting)
    
    time.sleep(2)
    docNum = es.count(index=indexName)['count']
    stdout.write('Bulk indexing finished, doc_count:%d %40s\n' % (docNum, ''))
    

def run():
    fileNameList = generateFileNameList(startDate, endDate)
    for fileName in fileNameList:
        print('\nLoading %s ... (ETA:20s)'%(fileName))
        logs = generateLogs(fileName) 
        
        logsNum = len(logs) 
        print('Total Number of logs: %d'%(logsNum))
        print('\nGenerating data ... ')
        
        start_time = time.time()
        userDataDict = {}
        dataList = []
        for i in xrange(len(logs)):
            log = logs[i]
            
            if i%1000 == 0:
                stdout.write('Used %.2f seconds, Processing %5d/%d log: "%s_%s_%s_%s" %10s \r'
                    % (time.time()-start_time, i, logsNum, log['timestamp'].isoformat(), 
                       log['user'], log['service'], log['IP'], '' ))
                stdout.flush()

            userData, data = generateData(log) 
            data = generateScore(data)

            userDataDict[ log['user'] ] = userData
            userDataList = userDataDict.values()
            dataList.append( data )

        elapsed_time = time.time()-start_time
        stdout.write('Used %.2f seconds, Processed %5d logs, Avg: %.2f logs/sec %50s \n'
            %(elapsed_time, logsNum, logsNum/float(elapsed_time), ''))
       

        
        start_time = time.time()
        bulkIndex( logs, userDataList, dataList )
        elapsed_time = time.time()-start_time
        indexNum = len(logs)+len(userDataList)+len(dataList)
        print('Used %.2f seconds, Processed %7d indexs, Avg: %.2f indexs/sec %50s'
            %(elapsed_time, indexNum, indexNum/float(elapsed_time), ''))


def generateJobs(logs):
    usersLog = defaultdict(list)
    #Split by User
    for log in logs:
        usersLog[ log['user'] ].append(log)
    print('Split into %d jobs for multiprocessing ...'%(len(jobList)))
    return usersLog 

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

        #TODO
        ans = results.get()
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
    maxThread = 500000
    chunkSize = 500
    
    #client = connections.create_connection(hosts=hosts, maxsize=maxThread)
    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    es.indices.create(index=indexName,ignore=[400])
    
    run()

