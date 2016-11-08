import csv, codecs
from datetime import datetime
from collections import defaultdict
from operator import itemgetter

services = ['SMTP'] 
#services = ['SMTP', 'VPN', 'Exchange', 'POP3', 'OWA']

def generateLog(logList):
    timestampStr = logList[1]+'T'+logList[2]
    timestamp = datetime.strptime(timestampStr, '%Y-%m-%dT%H:%M:%S')
    log = {
        'service'  : logList[0],
        'timestamp': timestamp,
        'user'     : logList[3],
        'server'   : logList[4],
        #'IP'       : logList[5],
        'IP'       : '0,0,0,0',
        'device'   : logList[6],
        'city'     : logList[7],
        'county'   : logList[8],
        'nation'   : logList[9],
        'label'    : {'analyst':None, 'randomForest':None} 
    }
    return log

def generateLogs(fileName):
    inputFile = codecs.open(fileName, 'r', 
                    encoding='ascii', errors='ignore') 
    logLists = list(csv.reader(inputFile))
    #print('Total Number of logs: %d'%(len(logLists)))
    
    logs = []
    for logList in logLists:
        #Filter by services
        if logList[0] not in services: continue
        
        log = generateLog(logList)
        logs.append(log)
    
    #Sort by timestamp 
    logsSortByTime = sorted(logs, key = itemgetter('timestamp'))
    return logsSortByTime
