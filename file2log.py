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
        #'server'   : logList[4],
        #'IP'       : logList[5],
        'device'   : logList[6],
        'city'     : logList[7],
        'county'   : logList[8],
        'nation'   : logList[9],
        'label'    : None
    }
    return log

def generateLogs(logLists):
    logs = []
    for logList in logLists:
        #Filter by services
        if logList[0] not in services: continue
        
        log = generateLog(logList)
        logs.append(log)
    
    #Sort by timestamp 
    logsSortByTime = sorted(logs, key = itemgetter('timestamp'))
    return logsSortByTime

def generateJobs(logLists):
    logs = generateLogs(logLists)
    
    usersLog = defaultdict(list)
    #Split by User
    for log in logs:
        usersLog[ log['user'] ].append(log)
    jobList = usersLog.values()
    print('Split into %d jobs for multiprocessing ...'%(len(jobList)))
    return jobList
