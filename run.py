import json, csv, codecs
import time
import multiprocessing
import cPickle
from datetime import date, timedelta
from collections import deque
from sys import stdout

from file2log import generateLogs 
from log2data import getUserData, generateData, generateDataFromJob 
from data2score import generateScore

from elasticsearch import Elasticsearch, helpers

# Global vairables
indexName = 'ai2_v2.0_lschyi'
#indexName = 'ai2_test'
startDate = date(2016,6,1) 
endDate = date(2016,6,1) 
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
    userDataActions = []
    
    for userData in userDataList:
        action = {
            '_op_type': 'index',
            '_index'  : indexName,
            '_type'   : 'userData',
            '_id'     : userData['user'], 
            '_source' : { 'blob': cPickle.dumps(userData)},
        }
        userDataActions.append(action)
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
    
    return actions, userDataActions 

def doBulk( chunkSize, actions, timeOut ):
    actionNum = len(actions)
    start_time = time.time()
    
    while len(actions) > chunkSize:
        stdout.write('Used %.2f seconds, %7d/%7d actions left \r'
                    %(time.time()-start_time, len(actions), actionNum))
        stdout.flush()
        successNum, item = helpers.bulk(es,actions[:chunkSize], refresh='false', request_timeout=timeOut)
        del actions[:chunkSize]
    
    successNum, item = helpers.bulk(es,actions, refresh='false', request_timeout=timeOut)
    del actions[:len(actions)]
    
    #deque(helpers.parallel_bulk(es,actions, thread_count=maxThread, chunk_size=1000, request_timeout=30))
        
    elapsed_time = time.time()-start_time
    print('Used %.2f seconds, Processed %7d actions, Avg: %.2f actions/sec %50s'
        %(elapsed_time, actionNum, actionNum/float(elapsed_time), ''))

def bulkIndex( logList, userDataList, dataList ):
    print('\nBulk Indexing ...') 


    actions, userDataActions = generateBulkActions( logList, userDataList, dataList )


    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    setting = {"index":{"refresh_interval":"-1"}}
    es.indices.put_settings(index=indexName, body=setting)

    print('number of userData:%8d'%len(userDataList))
    doBulk( 500, userDataActions, 180 ) #TODO: Needed to be tuned accordingly

    print('number of logs    :%8d'%len(logList))
    print('number of data    :%8d'%len(dataList))
    doBulk( 500, actions, 30 )


    setting = {"index":{"refresh_interval":"1s"}}
    es.indices.put_settings(index=indexName, body=setting)

    time.sleep(2)
    docNum = es.count(index=indexName)['count']
    print('Bulk indexing finished, doc_count:%d %40s\n' % (docNum, ''))

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
            
            if i%100 == 0:
                stdout.write('Used %.2f seconds, Processing %5d/%d log: "%s_%s_%s_%s" %10s \r'
                    % (time.time()-start_time, i, logsNum, log['timestamp'].isoformat(), 
                       log['user'], log['service'], log['IP'], '' ))
                stdout.flush()

            userData = getUserData( log['user'] )
            userData, data = generateData(log, userData) 
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
    usersLog = {}
    #Split by User
    for log in logs:
        user = log['user']
        if user in usersLog:
            usersLog[user].append(log)
        else:
            usersLog[user] = [ log ]
    print('Split into %d jobs for multiprocessing ...'%(len(usersLog.values())))
    return usersLog 

def doJob(userDataTuple):
    userData, dataList = generateDataFromJob(userDataTuple) 
    dataList = [generateScore(data) for data in dataList]
    return userData, dataList

def runParallel():
    fileNameList = generateFileNameList(startDate, endDate)
    for fileName in fileNameList:
        print('\nLoading %s ... (ETA:20s)'%(fileName))
        
        logs = generateLogs(fileName) 
        jobList = generateJobs(logs)
        
        print('\nGenerating data ... ')
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes = pool_size)
        results = pool.map_async(doJob, jobList.items(), chunksize=1)

        start_time = time.time()
        jobNum = len(jobList)
        while (True):
            if(results.ready()): break
            stdout.write('Used %.2f seconds, %5d/%d jobs left ... %10s \r'
                % (time.time()-start_time, results._number_left, jobNum, ''))
            stdout.flush()
            time.sleep(0.5)
        
        elapsed_time = time.time()-start_time
        stdout.write('Used %.2f seconds, Processed %5d logs, Avg: %.2f logs/sec %50s \n'
            %(elapsed_time, len(logs), len(logs)/float(elapsed_time), ''))
        
        pool.close()
        pool.join()       
       
        userDataList = []
        allDataList = []
        for userData, dataList in results.get():
            userDataList.append(userData)
            allDataList.extend(dataList)
        
        bulkIndex( logs, userDataList, allDataList )


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
    
    es = Elasticsearch(hosts=hosts, maxsize=maxThread)
    es.indices.create(index=indexName,ignore=[400])
    
    #run()
    runParallel()

