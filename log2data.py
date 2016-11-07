from collections import defaultdict

def generateUserData(user):
    userData = {
        'services':{},
        #'IPs':defaultdict(list),
        'devices':{},
        'cities':{},
        'counties':{},
        'nations':{},
        'timestamps':{}
    }
    return userData

def update( Dict, key, value ):
    if key in Dict:
        Dict[key].append(value)
    else:
        Dict[key] = [value]


def generateData(log, userData):
    featureVector = [0.0 for x in xrange(24)]
    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':defaultdict(float),
        'label':None
    }
    
     
    update( userData['services'], log['service'], log['timestamp'] )
    #update( userData['IPs'], log['IP'], log['timestamp'] )
    update( userData['devices'], log['device'], log['timestamp'] )
    update( userData['cities'], log['city'], log['timestamp'] )
    update( userData['counties'], log['county'], log['timestamp'] )
    update( userData['nations'], log['nation'], log['timestamp'] )
    update( userData['timestamps'], log['timestamp'].date().isoformat(), log ) #??
    return data, userData
