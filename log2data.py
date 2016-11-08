from collections import defaultdict

def generateUserData(user):
    userData = {
        'services':{},
        #'IPs':{},
        'devices':{},
        'cities':{},
        'counties':{},
        'nations':{},
        'timestamps':{}
    }
    return userData

def getUserData(user, es):
    res = es.get( index = indexName, 
                  doc_type = 'userData',
                  id = user,
                  ignore = [404]
          )
    if not res['found']:
        userData = generateUserData(user)
    else:
        userData = res['_source']
        
    return userData, newUser

def update( Dict, key, value ):
    if key in Dict:
        Dict[key].append(value)
    else:
        Dict[key] = [value]

#TODO
def generateData(log):
    # es =  
    userData = getUserData( log['user'], es )
    featureVector = [0.0 for x in xrange(24)]
    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':{},
        'label':{}
    }
    
     
    update( userData['services'], log['service'], log['timestamp'] )
    #update( userData['IPs'], log['IP'], log['timestamp'] )
    update( userData['devices'], log['device'], log['timestamp'] )
    update( userData['cities'], log['city'], log['timestamp'] )
    update( userData['counties'], log['county'], log['timestamp'] )
    update( userData['nations'], log['nation'], log['timestamp'] )
    update( userData['timestamps'], log['timestamp'].date().isoformat(), log ) #??
    return userData, data

#TODO
def generateDataFromJob(job):
    # es =  
    userData = getUserData( log['user'], es )
    dataList = []
    return userData, dataList
