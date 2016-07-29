import os, logging, json, csv, pickle
import collections, codecs
from operator import itemgetter 
from datetime import date, datetime, timedelta
from multiprocessing import Process, Queue, Pool

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, DocType, String, Date, Integer, Boolean
from elasticsearch_dsl import Search, Q 
from elasticsearch_dsl.connections import connections 


class Record(DocType):
    user    = String(analyzer='snowball', fields={'raw': String(index='not_analyzed')})
    time    = Date()
    service = String(index='not_analyzed') 
    server  = String(index='not_analyzed')
    ip      = String(index='not_analyzed')
    device  = String(index='not_analyzed')
    city    = String(index='not_analyzed')
    county  = String(index='not_analyzed')
    nation  = String(index='not_analyzed')

    class Meta:
        index = 'ai2'
        doc_type = 'log'

    def save(self, *args, **kwargs):
        return super(Record, self).save(*args, **kwargs)


def newRecord(rawlog_list, id_num):
    #record = Record() 
    record = Record(meta={'id': id_num}) 
    record.service = rawlog_list[0]
    record.time    = datetime.strptime(rawlog_list[1]+'T'+rawlog_list[2], '%Y-%m-%dT%H:%M:%S')
    #record.date   = rawlog_list[1]
    #record.time   = rawlog_list[2]
    record.user    = rawlog_list[3]
    record.server  = rawlog_list[4]
    record.ip      = rawlog_list[5]
    record.device  = rawlog_list[6]
    record.city    = rawlog_list[7]
    record.county  = rawlog_list[8]
    record.nation  = rawlog_list[9]
    try: 
        record.save()
    except KeyError:
        pass

def rawlog2json(rawlog_list):
    data = collections.OrderedDict([
        ('user', rawlog_list[3]),
        ('time', rawlog_list[1]+'T'+rawlog_list[2]),
        ('service', rawlog_list[0]),
        ('device',rawlog_list[6]),
        ('ip',rawlog_list[5]),
        ('server',rawlog_list[4]),
        ('city',rawlog_list[7]),
        ('county',rawlog_list[8]),
        ('nation',rawlog_list[9])
    ]) 
    return json.dumps(data, ensure_ascii=False, indent=4) 


def countPastLogMean(rawlog_list):
    timestamp = datetime.strptime(rawlog_list[1]+'T'+rawlog_list[2], '%Y-%m-%dT%H:%M:%S')
    justnow   = timestamp - timedelta(seconds=1)
    past1days = timestamp - timedelta(days=1)
    past3days = timestamp - timedelta(days=3)
    past7days = timestamp - timedelta(days=7)
    #print '\ntimestamp:', timestamp
    #print rawlog2json(rawlog_list)
    
    q = Q(
        'bool',
        must=[
            Q('match', user=rawlog_list[3]),
            Q('match', service=rawlog_list[0])
        ]
    )
    s = Search(using=client, index="ai2", doc_type="log")
    
    s = s.filter('range', **{'time':{'from':past7days, 'to':timestamp}}).query(q)
    #s.aggs.bucket('distinct_device', 'terms', field='device')
    #s.aggs.bucket('distinct_ip', 'terms', field='ip')
    response = s.execute()
    try: 
        past7daysCount = response.hits.total
    except KeyError:
        past7daysCount = response.hits.total 
    #print json.dumps(response, indent=4)
    #print 'past7days:',past7daysCount 
    #for item in response.hits:
    #    print item.time
        #print json.dumps(item.to_dict(), indent=4)
 
    #distinctDevice = len(response.aggregations.distinct_device.buckets)
    #print 'number of device:', len(response.aggregations.distinct_device.buckets)
    #for device in response.aggregations.distinct_device.buckets:
    #    print 'distince_device:', device.key, device.doc_count
    #distinctIp = len(response.aggregations.distinct_ip.buckets)
    #print 'number of ips:', len(response.aggregations.distinct_ip.buckets)
    #for ip in response.aggregations.distinct_ip.buckets:
    #    print 'distince_ip:', ip.key, ip.doc_count

    s = s.filter('range', **{'time':{'from':past3days, 'to':justnow}})
    response = s.execute()
    try: 
        past3daysCount = response.hits.total
    except KeyError:
        past3daysCount = response.hits.total
    #past3daysCount = 0 if not response['found'] else response.hits.total
    #print 'past3days', response.hits.total
    #for item in response.hits:
    #    print item.time
        #print json.dumps(item.to_dict(), indent=4)

    s = s.filter('range', **{'time':{'from':past1days, 'to':justnow}})
    response = s.execute()
    try: 
        past1daysCount = response.hits.total
    except KeyError:
        past1daysCount = response.hits.total
    #print 'past1days', response.hits.total
    #for item in response.hits:
    #    print item.time
        #print json.dumps(item.to_dict(), indent=4)

    #return [ past1daysCount, past3daysCount/3.0, past7daysCount/7.0, distinctDevice, distinctIp ]
    return past1daysCount, past3daysCount/3.0, past7daysCount/7.0

def checkIsNewItem(List, item):
    if item in List:
        return 0.0
    if ADD_DATA:
        #print 'ADD_DATA'
        List.append(item)
    return 1.0

def generateFeatures(rawlog_list):
    service = rawlog_list[0]
    date    = rawlog_list[1]
    time    = rawlog_list[2]
    user    = rawlog_list[3]
    server  = rawlog_list[4]
    ip      = rawlog_list[5]
    device  = rawlog_list[6]
    city    = rawlog_list[7]
    county  = rawlog_list[8]
    nation  = rawlog_list[9]
    
    past1daysMean, past3daysMean, past7daysMean = countPastLogMean(rawlog_list)

    es = Elasticsearch()
    res = es.get(index='ai2', doc_type='data', id=user, ignore=[400, 404])
    if not res['found']:
        doc = {
            'device':[device],
            'ip':[ip],
            'city':[city],
            'county':[county],
            'nation':[nation]
        }
        res = es.index(index='ai2',doc_type='data', id=user, body=doc)
        newDevice = newIp = newCity = newCounty = newNation = 1.0
    else:
        #print json.dumps(res, indent=4)
        doc = res['_source']
        newDevice = checkIsNewItem(doc['device'], device) 
        newIp     = checkIsNewItem(doc['ip'], ip) 
        newCity   = checkIsNewItem(doc['city'], city) 
        newCounty = checkIsNewItem(doc['county'], county) 
        newNation = checkIsNewItem(doc['nation'], nation) 
        #print 'doc', json.dumps(doc, indent=4)
        res = es.index(index='ai2', doc_type='data', id=user, body=doc, refresh=True)        
  
    delta = 0.000001
    #TODO compare with same day(e.g. Monday) of the past 4 weeks 
    f0 = (past1daysMean-past3daysMean)/(past3daysMean+delta)
    f1 = (past1daysMean-past7daysMean)/(past7daysMean+delta)
    f2 = (past3daysMean-past7daysMean)/(past7daysMean+delta)
    
    #features = [past1daysMean, past3daysMean, past7daysMean, newDevice, newIp, newCity, newCounty, newNation]
    features = [f0, f1, f2, newDevice, newIp, newCity, newCounty, newNation]
    return features

def doWork(rawlog_list):        
    if len(rawlog_list) != 10:
        print 'Format Error At line '+str(rawlog_lists.index(rawlog_list))+': '+str(rawlog_list)
        return 

    service = rawlog_list[0]
    date    = rawlog_list[1]
    time    = rawlog_list[2]
    user    = rawlog_list[3]
    server  = rawlog_list[4]
    ip      = rawlog_list[5]
    device  = rawlog_list[6]
    city    = rawlog_list[7]
    county  = rawlog_list[8]
    nation  = rawlog_list[9]
    ''' 
    doc = {
        'device':[device],
        'ip':[ip],
        'city':[city],
        'county':[county],
        'nation':[nation]
    }
    es.index(index='ai2',doc_type='data', id=user, body=doc, refresh=True, ignore=[400])
    '''
    #if user != 'r04921039':
    #    return  
    
    print date, time, user, service 
    # save log into elasticsearch and refresh elasticsearch
    if ADD_RECORD:
        #print 'ADD_RECORD'
        es = Elasticsearch()
        id_str = date+'T'+time+'_'+user+'_'+service 
        newRecord(rawlog_list, id_str)
        res = es.indices.refresh(index='ai2') 
   
    # features = [past1daysMean, past3daysMean, past7daysMean, newDevice, newIp, newCity, newCounty, newNation]
    features = generateFeatures(rawlog_list)
    #print rawlog2json(rawlog_list)
    #print features
    #print >> outFile, features
    return features
    #del es
    #if features is not None: 
    #    return features

def doWork_mp(args):
    try:
        return doWork(args)
    except Exception:
        #logging.exception("f(%r) failed" % (args,))
        pass

def getViolationList():
    user_list = []
    inputFile = codecs.open('rawlog/violation-201606.txt')
    rawlog_lists = csv.reader(inputFile)
    for rawlog_list in rawlog_lists:
        user = rawlog_list[3]
        if user not in user_list:
            user_list.append(user)
    return user_list

def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

if __name__ == '__main__':
    # Define a defualt Elasticsearch client
    client = connections.create_connection(hosts=['localhost:9200'])
    
    es = Elasticsearch()
    #es.indices.delete(index='ai2',ignore=[400,404])
    Record.init()

    violationList = getViolationList()

    path = 'rawlog/'
    TEST = False 
    if TEST:
        #filename = 'testInput.log'
        filename = 'violation-201606.txt'
        ADD_RECORD = False 
        ADD_DATA = False 
        inputFile = codecs.open(path+filename, 'r',encoding='ascii', errors='ignore') 
        rawlog_lists = csv.reader(inputFile)
        rawlog_lists = sorted(rawlog_lists, key =itemgetter(2))
        output = []
        for rawlog_list in rawlog_lists:
            feature = doWork(rawlog_list)
            if feature is not None:
                output.append((feature, rawlog_list))
        pickle.dump(output, open('output/'+filename+'.feature', 'wb'))
    else: 
        start_date = date(2016,6,1)
        end_date = date(2016,6,30)
        ##for filename in os.listdir(path):
        for d in dategenerator(start_date, end_date):
            filename = 'all-'+d.strftime('%Y%m%d')+'-geo.log'
            ADD_RECORD = True 
            ADD_DATA = True 
            
            print 'Processing ',filename, ' ...'
            
            #inputFile = open('rawlog/testInput.log', 'rb') 
            inputFile = codecs.open(path+filename, 'r',encoding='ascii', errors='ignore') 
            rawlog_lists = csv.reader(inputFile)
            rawlog_lists = sorted(rawlog_lists, key =itemgetter(2))
            #for l in rawlog_lists: print l
            
            for violationUser in violationList: 
                output = []
                for rawlog_list in rawlog_lists:
                    if violationUser == rawlog_list[3]:
                        features = doWork(rawlog_list)
                        if features is not None:
                            output.append((features, rawlog_list))
                outputPath = 'output/'+violationUser+'/'
                if not os.path.exists(outputPath):
                    os.makedirs(outputPath)
                pickle.dump(output, open(outputPath+filename+'.feature', 'wb'))
    
                '''
                # multiprocess  
                pool = Pool(processes=24)
                output = [pool.apply(doWork, args=(rawlog_list,violationUser)) for rawlog_list in rawlog_lists]
                #print(output)
                pickle.dump(output, open('output/'+violationUser+'_'+filename+'.feature', 'wb'))
                pool.close()
                pool.join()
                '''
        #del Record
