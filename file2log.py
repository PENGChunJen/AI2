import csv, codecs, string, time, fnmatch
from sys import stdout
from datetime import datetime
from collections import defaultdict
from operator import itemgetter

import config

table = string.maketrans('.', '_')

def handleException( logList ):
    #Filter by services
    if logList[0] not in config.services: 
        return True 

    #csv not haveing 10 columns
    if len(logList) != 10:
        print('Format Error: Need 10 colums "%s"'%(','.join(logList)))
        return True

    #empty user name
    if logList[3] == '':
        print('Format Error: user cannot be empty  "%s"'%(','.join(logList)))
        return True

    return False

# Avoid dot(.), since dot cannot be in ES name field
def translate(s):
    return s.translate(table)
    #return s.translate(string.maketrans('.', '_'))

def generateLog(logList):
    timestampStr = logList[1]+'T'+logList[2]
    timestamp = datetime.strptime(timestampStr, '%Y-%m-%dT%H:%M:%S')
    log = {
        'service'  : logList[0],
        'timestamp': timestamp,
        'user'     : translate(logList[3]),
        'server'   : translate(logList[4]),
        'IP'       : translate(logList[5]),
        'device'   : translate(logList[6]),
        'city'     : logList[7],
        'county'   : logList[8],
        'nation'   : logList[9],
        'label'    : {'analyst':None} 
    }
    return log

def getData(fileName):
    with codecs.open(fileName, 'r', encoding='ascii', errors='ignore') as csvFile:
        dataReader = csv.reader(csvFile)
        for row in dataReader:
            if handleException( row ): continue
            yield list(row)

def inWhiteList(log):
    for IP in config.whiteList:
        if fnmatch.fnmatch(log['IP'], IP):
            return True
    return False

def generateLogs(fileName):
    print('\nGenerating logs of services: %s... '%(', '.join(config.services)))
    start_time = time.time()
    
    logs = []
    count = 0
    for logList in getData(fileName):
        log = generateLog(logList)
        if inWhiteList(log):
            log['label']['analyst'] = 'whiteList'
        logs.append(log)
        
        count += 1
        if count % 1000 == 0:
            stdout.write('Used %.2f seconds, Generated %7d logs ... %10s \r'
                % (time.time()-start_time, count, ''))
            stdout.flush()
        
    
    elapsed_time = time.time()-start_time
    print('Used %.2f seconds, Generated %7d logs, Avg: %.2f logs/sec' 
            % ( elapsed_time, count, count/elapsed_time))
    #Sort by timestamp 
    print('Sorting logs by timestamps ...')
    logsSortByTime = sorted(logs, key = itemgetter('timestamp'))
    return logsSortByTime

if __name__ == '__main__':
    generateLogs('rawlog/all-20160601-geo.log')
