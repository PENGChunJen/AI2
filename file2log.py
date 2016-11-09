import csv, codecs, string
from datetime import datetime
from collections import defaultdict
from operator import itemgetter

services = ['SMTP'] # 0.5%
#services = ['SMTP', 'VPN', 'Exchange'] # 2%
#services = ['SMTP', 'VPN', 'Exchange', 'POP3'] # 20%
#services = ['SMTP', 'VPN', 'Exchange', 'POP3', 'OWA'] # 100% Don't touch this unless certain
table = string.maketrans('.', '_')

def handleException( logList ):
    #Filter by services
    if logList[0] not in services: 
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

def generateLogs(fileName):
    inputFile = codecs.open(fileName, 'r', 
                    encoding='ascii', errors='ignore') 
    logLists = list(csv.reader(inputFile))
    print('Total Number of raw logs: %d'%(len(logLists)))
    print('\nGenerating logs ... (ETA:%ds)'%(len(logLists)/45000))
    
    logs = []
    for logList in logLists:
        if handleException( logList ): continue
        
        log = generateLog(logList)
        logs.append(log)
    
    #Sort by timestamp 
    print('Sorting logs by timestamps ...')
    logsSortByTime = sorted(logs, key = itemgetter('timestamp'))
    return logsSortByTime
