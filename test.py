from elasticsearch import Elasticsearch
import codecs, csv, ast
from operator import itemgetter
import cPickle as pickle

path = 'rawlog/'
#filename = 'testInput.log'
#filename = 'violation-201606.txt'
filename = 'all-20160601-geo.log'
services = ['Exchange', 'OWA', 'POP3', 'SMTP', 'VPN']

inputFile = codecs.open(path+filename,'r', encoding='ascii', errors='ignore')
rawlog_lists = csv.reader(inputFile)
sorted_rawlog_lists = sorted(rawlog_lists, key = itemgetter(2)) # sorted by time
for rawlog_list in sorted_rawlog_lists:
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
    
    if service != 'VPN': continue

    # save log into elasticsearch and refresh elasticsearch
    hosts = ['192.168.1.1:9200','192.168.1.2:9200','192.168.1.3:9200',
             '192.168.1.4:9200','192.168.1.5:9200','192.168.1.6:9200','192.168.1.10:9200']
    es = Elasticsearch(hosts, maxsize=100)
    id_str = date+'T'+time+'_'+user+'_'+service
    res = es.get(index="ai2", doc_type="features", id=id_str)
    if not res['found']:
        print 'Cannot find id', id_str
    else:
        raw_log = ast.literal_eval(res['_source']['raw_log'])
        raw_log = [n.strip() for n in raw_log]
        features = ast.literal_eval(res['_source']['features'])
        features = [n for n in features]


        print 'log: ', raw_log 
        print 'vec: ', features 


