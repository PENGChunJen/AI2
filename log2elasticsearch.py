import os, json, csv, pickle
import collections, codecs
from operator import itemgetter 
from datetime import date, datetime, timedelta

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, DocType, String, Date, Integer, Boolean
from elasticsearch_dsl import Search, Q 
from elasticsearch_dsl.connections import connections 

# Define a defualt Elasticsearch client
client = connections.create_connection(hosts=['localhost:9200'])

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
        #('city','Taipei'),
        ('ip',rawlog_list[5]),
        ('ip2',rawlog_list[4])
    ]) 
    return json.dumps(data, ensure_ascii=False, indent=4) 


def generateVector(rawlog_list):
    timestamp = datetime.strptime(rawlog_list[1]+'T'+rawlog_list[2], '%Y-%m-%dT%H:%M:%S')
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

    s = s.filter('range', **{'time':{'from':past3days, 'to':timestamp}})
    response = s.execute()
    past3daysCount = response.hits.total
    #print 'past3days', response.hits.total
    #for item in response.hits:
    #    print item.time
        #print json.dumps(item.to_dict(), indent=4)

    s = s.filter('range', **{'time':{'from':past1days, 'to':timestamp}})
    response = s.execute()
    past1daysCount = response.hits.total
    #print 'past1days', response.hits.total
    #for item in response.hits:
    #    print item.time
        #print json.dumps(item.to_dict(), indent=4)

    #return [ past1daysCount, past3daysCount/3.0, past7daysCount/7.0, distinctDevice, distinctIp ]
    return [ past1daysCount, past3daysCount/3.0, past7daysCount/7.0, 0.0, 0.0]

def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

es = Elasticsearch()
#es.indices.delete(index='ai2',ignore=[400,404])
Record.init()
path = 'rawlog/'
start_date = date(2016,3,6)
end_date = date(2016,3,7)

deviceDict = {}
ipDict = {}
pickle.dump(deviceDict, open('output/deviceDict', 'wb'))
pickle.dump(ipDict, open('output/ipDict', 'wb'))

#for filename in os.listdir(path):
for d in dategenerator(start_date, end_date):
    filename = 'all-'+d.strftime('%Y%m%d')+'.log'
    print filename
    lists = []
    #with open('inputfile.txt', 'rb') as inputFile:
    with codecs.open(path+filename, 'r',encoding='ascii', errors='ignore') as inputFile:
        outFile = open(filename+'.feature', 'wb')
        deviceDict = pickle.load(open('output/deviceDict', 'rb'))
        ipDict = pickle.load(open('output/ipDict', 'rb'))
        rawlog_lists = csv.reader(inputFile)
        rawlog_lists = sorted(rawlog_lists, key =itemgetter(2))
        i = 0
        total = str(len(rawlog_lists))
        for rawlog_list in rawlog_lists:
            #print rawlog2json(rawlog_list)
            if i%1000 == 0: 
                print filename+' '+str(i)+'/'+total, rawlog_list[2]
            i = i+1
            service = rawlog_list[0]
            date = rawlog_list[1]
            time = rawlog_list[2]
            user = rawlog_list[3]
            ip = rawlog_list[5]
            device = rawlog_list[6]
            
            id_str = date+'T'+time+'_'+user+'_'+service 
            #print id_str
            newRecord(rawlog_list, id_str)

            es.indices.refresh(index='ai2') 
            features = generateVector(rawlog_list)
           
            
            if user in deviceDict:
                if device not in deviceDict[user]:
                    features[3] = 1.0
            else:
                deviceDict[user] = [device]
                features[3] = 1.0

            if user in ipDict:
                if ip not in ipDict[user]:
                    features[4] = 1.0
            else:
                ipDict[user] = [ip]
                features[4] = 1.0
            print >> outFile, features
            
            #lists.append(features)
    #pickle.dump(lists, open('output/'+d.strftime('%Y%m%d')+'.features', 'wb'))
    pickle.dump(deviceDict, open('output/deviceDict', 'wb'))
    pickle.dump(ipDict, open('output/ipDict', 'wb'))
