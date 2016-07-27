import os, json, csv, pickle
import collections, codecs
from operator import itemgetter 
from datetime import date, datetime, timedelta
from multiprocessing import Process, Queue, Pool

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, DocType, String, Date, Integer, Boolean
from elasticsearch_dsl import Search, Q 
from elasticsearch_dsl.connections import connections 


class Record(DocType):
    user = String(analyzer='snowball', fields={'raw': String(index='not_analyzed')})
    time = Date()
    service = String(index='not_analyzed')
    device = String(index='not_analyzed')
    ip = String(index='not_analyzed')
    ip2 = String(index='not_analyzed')
    #new_service = Boolean()
    #new_device = Boolean()
    #new_city = Boolean()
    #new_ip = Boolean()

    class Meta:
        index = 'ai2'
        doc_type = 'log'

    def save(self, *args, **kwargs):
        return super(Record, self).save(*args, **kwargs)


def newRecord(rawlog_list, id_num):
    record = Record(meta={'id': id_num}) 
    #record = Record() 
    record.user = rawlog_list[3] 
    record.time = datetime.strptime(rawlog_list[1]+'T'+rawlog_list[2], '%Y-%m-%dT%H:%M:%S')
    record.service = rawlog_list[0] 
    record.device = rawlog_list[6] 
    record.ip = rawlog_list[5] 
    record.ip2 = rawlog_list[4] 
    record.save()

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
    past7daysCount = response.hits.total
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
    past3daysCount = response.hits.total
    #print 'past3days', response.hits.total
    #for item in response.hits:
    #    print item.time
        #print json.dumps(item.to_dict(), indent=4)

    s = s.filter('range', **{'time':{'from':past1days, 'to':justnow}})
    response = s.execute()
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
    List.append(item)
    return 1.0

def generateFeatures(rawlog_list):
    past1daysMean, past3daysMean, past7daysMean = countPastLogMean(rawlog_list)

    res = es.get(index='ai2', doc_type='data', id=user, ignore=[400,404])
    if not res['found']:
        doc = {
            'device':[device],
            'ip':[ip],
            'city':[city],
            'county':[county],
            'nation':[nation]
        }
        es.index(index='ai2',doc_type='data', id=user, body=doc)
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
        es.index(index='ai2', doc_type='data', id=user, body=doc, refresh=True)        
  
    delta = 0.000001
    #TODO compare with same day(e.g. Monday) of the past 4 weeks 
    f0 = (past1daysMean-past3daysMean)/(past3daysMean+delta)
    f1 = (past1daysMean-past7daysMean)/(past7daysMean+delta)
    f2 = (past3daysMean-past7daysMean)/(past7daysMean+delta)
    features = [f0, f1, f2, newDevice, newIp, newCity, newCounty, newNation]
    return features

def doWork(rawlog_list):        
    # save log into elasticsearch and refresh elasticsearch
    id_str = date+'T'+time+'_'+user+'_'+service 
    newRecord(rawlog_list, id_str)
    es.indices.refresh(index='ai2') 
   
    features = generateFeatures(rawlog_list)
    print rawlog2json(rawlog_list)
    print features
    #print >> outFile, features
    return features

def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

if __name__ == '__main__':
    # Define a defualt Elasticsearch client
    client = connections.create_connection(hosts=['localhost:9200'])
    
    es = Elasticsearch()
    es.indices.delete(index='ai2',ignore=[400,404])
    Record.init()
    
    path = 'rawlog/'
    start_date = date(2016,6,1)
    end_date = date(2016,6,30)
    
    #for filename in os.listdir(path):
    for d in dategenerator(start_date, end_date):
        #filename = 'testInput.log'
        filename = 'all-'+d.strftime('%Y%m%d')+'-geo.log'
        print 'Processing ',filename
        
        #inputFile = open('rawlog/testInput.log', 'rb') 
        inputFile = codecs.open(path+filename, 'r',encoding='ascii', errors='ignore') 
        rawlog_lists = csv.reader(inputFile)
        rawlog_lists = sorted(rawlog_lists, key =itemgetter(2))
        #for l in rawlog_lists: print l
        
        
        #TODO multicore jobs
    
        outFile = open('output/'+filename+'.feature', 'wb')
        lists = []
        for rawlog_list in rawlog_lists:
            if len(rawlog_list) != 10:
                print 'Format Error At line '+str(rawlog_lists.index(rawlog_list))+': '+str(rawlog_list)
                continue
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
            
            if user == 'r04921039':
                # features = [past1daysMean, past3daysMean, past7daysMean, newDevice, newIp, newCity, newCounty, newNation]
                features = doWork(rawlog_list)
                lists.append(features)
            
        pickle.dump(lists, outFile)
