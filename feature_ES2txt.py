from elasticsearch import Elasticsearch
import codecs, csv, ast
from datetime import date, datetime, timedelta
from operator import itemgetter
import cPickle as pickle

path = 'rawlog/'
#filename = 'testInput.log'
#filename = 'violation-201606.txt'
#filename = 'all-20160601-geo.log'

filename_list = []
start_date = date(2016,7,1)
end_date = date(2016,7,31)
from log2elasticsearch import dategenerator
for d in dategenerator(start_date, end_date):
    filename = 'all-'+d.strftime('%Y%m%d')+'-geo.log'
    filename_list.append(filename)
    
for filename in filename_list:
    print '\nReading', filename, '...'
    inputFile = codecs.open(path+filename,'r', encoding='ascii', errors='ignore')
    rawlog_lists = csv.reader(inputFile)
    sorted_rawlog_lists = sorted(rawlog_lists, key = itemgetter(2)) # sorted by time
    log_pair_Exchange = []
    log_pair_SMTP = []
    log_pair_VPN = []

    print 'Generating log_pairs...'
    for rawlog_list in sorted_rawlog_lists:
        if len(rawlog_list) != 10:
            print 'Format Error At line : '+repr(rawlog_list)
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
        
        #services = ['Exchange', 'OWA', 'POP3', 'SMTP', 'VPN']
        services = ['Exchange', 'SMTP', 'VPN']
        if service not in services: continue
        #if service != 'SMTP': continue
    
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
            
            if service == 'Exchange':
                log_pair_Exchange.append((features, raw_log))
            elif service == 'SMTP':
                log_pair_SMTP.append((features, raw_log))
            elif service == 'VPN':
                log_pair_VPN.append((features, raw_log))
    
            #print 'log: ', raw_log 
            #print 'vec: ', features 
    
    pickle.dump(log_pair_Exchange, open('output/'+filename+'.Exchange.feature', 'wb'))
    pickle.dump(log_pair_SMTP, open('output/'+filename+'.SMTP.feature', 'wb'))
    pickle.dump(log_pair_VPN, open('output/'+filename+'.VPN.feature', 'wb'))
