import time, os, logging, json, csv, pickle
import collections, codecs
from operator import itemgetter 
from datetime import date, datetime, timedelta
import multiprocessing
import progressbar as pb

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
    record.user    = rawlog_list[3]
    record.server  = rawlog_list[4]
    record.ip      = rawlog_list[5]
    record.device  = rawlog_list[6]
    record.city    = rawlog_list[7]
    record.county  = rawlog_list[8]
    record.nation  = rawlog_list[9]
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
    response = s.execute()
    past7daysCount = response.hits.total
    
    s = s.filter('range', **{'time':{'from':past3days, 'to':justnow}})
    response = s.execute()
    past3daysCount = response.hits.total

    s = s.filter('range', **{'time':{'from':past1days, 'to':justnow}})
    response = s.execute()
    past1daysCount = response.hits.total
    
    return past1daysCount, past3daysCount/3.0, past7daysCount/7.0

def checkIsNewItem(List, item):
    if item in List:
        return 0.0
    if ADD_DATA:
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

    es = Elasticsearch(hosts, maxsize=max_thread)
    res = es.get(index='ai2', doc_type='data', id=user, ignore=[400, 404])
    if not res['found']:
        doc = {
            'device':[device],
            'ip':[ip],
            'city':[city],
            'county':[county],
            'nation':[nation]
        }
        if ADD_DATA:
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
        if ADD_DATA:
            res = es.index(index='ai2', doc_type='data', id=user, body=doc, refresh=True)        
  
    delta = 0.000001
    #TODO compare with same day(e.g. Monday) of the past 4 weeks 
    f0 = (past1daysMean-past3daysMean)/(past3daysMean+delta)
    f1 = (past1daysMean-past7daysMean)/(past7daysMean+delta)
    f2 = (past3daysMean-past7daysMean)/(past7daysMean+delta)
    
    #features = [past1daysMean, past3daysMean, past7daysMean, newDevice, newIp, newCity, newCounty, newNation]
    #features = [f0, f1, f2, newDevice, newIp, newCity, newCounty, newNation]
    features = [f0, f0, f2, newDevice, newIp, newCity, newCounty, newNation]
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
    
    # save log into elasticsearch and refresh elasticsearch
    es = Elasticsearch(hosts, maxsize=max_thread)
    id_str = date+'T'+time+'_'+user+'_'+service 
    if ADD_RECORD:
        newRecord(rawlog_list, id_str)
        res = es.indices.refresh(index='ai2') 
   
    features = generateFeatures(rawlog_list)
    
    if ADD_FEATURES:
        doc = {
            'raw_log':repr(rawlog_list),
            'features':repr(features)
        }
        res = es.index(index='ai2', doc_type='features', id=id_str, body=doc, refresh=True)        
    
    #print rawlog2json(rawlog_list)
    #print features
    #print >> outFile, features
    #print date, time, user, service 
    return (rawlog_list, features)

def doJob(rawlog_lists):
    return [doWork(rawlog_list) for rawlog_list in rawlog_lists]


def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

def split_by_user(rawlog_lists):
    #services = [
    #    'SMTP'       (6325 log/day),
    #    'VPN'        (6803 log/day),
    #    'Exchange'  (11557 log/day),
    #    'POP3'     (292894 log/day),
    #    'OWA      (1316837 log/day)'
    #]
    #services = ['SMTP', 'VPN', 'Exchange']
    services = ['SMTP']
    job_dict = {} # job_dict[user] = [rawlog_lists, rawlog_lists]
    
    print 'Selecting "'+(', ').join(services)+'" logs and Creating jobs...'
    for rawlog_list in rawlog_lists:
        #if rawlog_list[0] not in services: continue
        user = rawlog_list[3]
        if user not in job_dict:
            job_dict[user] = [rawlog_list]
        else:
            job_dict[user].append(rawlog_list)
    return job_dict

def start_process():
    print 'Starting', multiprocessing.current_process().name

def log2Features(filename, services):
    print '\nReading', filename, '...'
    inputFile = codecs.open(filename, 'r',encoding='ascii', errors='ignore') 
    rawlogs_csv = csv.reader(inputFile)
    rawlogs = list(rawlogs_csv)
    rawlog_lists = [] 
    
    for rawlog in rawlogs:
        if rawlog[0] not in services: continue
        rawlog_lists.append(rawlog)
    
    print 'Total number of logs:', len(rawlogs)
    print 'Sorting raw logs by time ...'
    rawlog_lists = sorted(rawlog_lists, key =itemgetter(2)) #sorted by time
    job_dict = split_by_user(rawlog_lists)
    job_list = []
    for user in sorted(job_dict):
        if user is not "":                       # CAUTION!!! Missing some logs
            job_list.append( job_dict[user] )
    
    start_time = time.time()
    
    #results = [ doJob(job) for job in job_list ]

    print 'Number of multiprocess Workers(cpu_count):',multiprocessing.cpu_count()
    pool_size = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes = pool_size)
    #results = [ pool.apply_async(doJob, args=(job, )) for job in job_list]
    results = pool.map_async(doJob, job_list, chunksize=1)
    pool.close()
    
    total_logs = total_jobs = 0
    for job in job_list:
        total_logs = total_logs + len(job)
    total_jobs = len(job_list)
    print 'Number of logs:', total_logs, ', Number of jobs (users):', total_jobs
    widgets=['Sending ', pb.SimpleProgress(),' jobs(', pb.Percentage(),') (', pb.Timer(), ')', pb.Bar('|','[',']'),'(', pb.ETA(),')']
    pbar = pb.ProgressBar(widgets=widgets, maxval=total_jobs).start()
    while (True):
        if(results.ready()): break
        remaining = results._number_left
        pbar.update(total_jobs-remaining)
        time.sleep(0.5)
    pbar.finish()

    log_pairs_of_users = results.get()
    log_pairs = []
    for log_pairs_of_user in log_pairs_of_users:
        log_pairs.extend(log_pairs_of_user)
    
    #pool.join()
    
    elapsed_time = time.time() - start_time
    print("--- Total time cost %s seconds ---" % (elapsed_time))
    print("--- Avg. Process Speed: %f logs/sec ---" % (total_logs/float(elapsed_time)))
    return log_pairs

def get_filename_list(TEST):
    filename_list = []
    if TEST:
        #filename = 'testInput.log'
        #filename = 'violation-201606.txt'
        filename = 'all-20160609-geo.log'
        filename_list.append(filename)
    else:
        start_date = date(2016,6,20)
        end_date = date(2016,6,20)
        #for filename in os.listdir(path):
        for d in dategenerator(start_date, end_date):
            filename = 'all-'+d.strftime('%Y%m%d')+'-geo.log'
            filename_list.append(filename)
    return filename_list 

def split_by_service(log_pairs):
    Exchange_log_pairs = []
    SMTP_log_pairs = []
    VPN_log_pairs = []
    for log_pair in log_pairs:
        log = log_pair[0]
        service = log[0]
        if service == 'Exchange':
            Exchange_log_pairs.append(log_pair)
        elif service == 'SMTP':
            SMTP_log_pairs.append(log_pair)
        elif service == 'VPN':
            VPN_log_pairs.append(log_pair)
    
    return SMTP_log_pairs, VPN_log_pairs, Exchange_log_pairs 

def removeItemFromList( l, item):
    if item in l:
        l.remove(item)
    #else:
    #    print item, 'is not in', l

def clear_ES_record(rawlog_list):
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
    
    es = Elasticsearch(hosts, maxsize=max_thread)
    res = es.get(index='ai2', doc_type='data', id=user, ignore=[400,404])
    if not res['found']:
        print 'Error: Cannot find data related to log', rawlog_list
    else:
        doc = res['_source']
        #print 'Before removing data in doc', user, ':', json.dumps(doc, indent=4)
        removeItemFromList(doc['device'], device) 
        removeItemFromList(doc['ip'], ip) 
        removeItemFromList(doc['city'], city) 
        removeItemFromList(doc['county'], county) 
        removeItemFromList(doc['nation'], nation) 
        #print 'After removing data in doc', user, ':', json.dumps(doc, indent=4)
        res = es.index(index='ai2', doc_type='data', id=user, body=doc, refresh=True)        

def getViolationList():
    user_list = []
    inputFile = codecs.open('rawlog/violation-201606.csv')
    rawlog_lists = csv.reader(inputFile)
    for rawlog_list in rawlog_lists:
        user = rawlog_list[3]
        if user not in user_list:
            user_list.append(user)
    return user_list

def delete_known_violation(sorted_list):
    print 'Deleting logs in violation list...'
    inputFile = codecs.open('rawlog/violation-201606.csv')
    violations = csv.reader(inputFile)

    new_sorted_list = []
    violation_data = []
    
    date = sorted_list[0]['log'][1] 

    violation_list = []
    for log in violations:
        if log[0] not in services: continue
        if log[1] != date: continue
        violation_list.append(log)
    
    for data in sorted_list:
        for violation in violation_list:
            if data['log'] == violation:
                print 'Deleting', data['log']
                violation_data.append(data) 
                clear_ES_record(data['log'])
                violation_data.append(data)
                break
        new_sorted_list.append(data)
    
    return new_sorted_list, violation_data


def supervision(sorted_list):
    #sorted_list = [{ 'score':score, 'log': [log], 'feature': [feature], 'encode_decode':[en_de]}, {}, ...]
    total_num = len(sorted_list) 
    normal_data = []
    abnormal_data = []
    data_dict = {} 
    for data in sorted_list:
        user = data['log'][3]
        if user not in data_dict:
            data_dict[user] = [data]
        else:
            data_dict[user].append(data)

    for data in sorted_list:
        print "\nSame user's log:"
        user = data['log'][3]
        for relevant_data in data_dict[user]:
            print 'score:', relevant_data['score'], ",".join(relevant_data['log'])
        
        print '\n          score:', data['score'] 
        print '            log:', data['log'] 
        print '        feature:', data['feature']
        print '  encode_decode:', data['encode_decode']
        print '\t\t\t\t\t\t\t\t\t\t', len(sorted_list),'/', total_num, 'logs left!\n'
        
        classify = raw_input("0: Anomaly, 1: Normal, O: All anomaly, A: All Normal, C:Continue, Please enter '0', '1', 'O', 'A', 'C' or 'exit':")
        while(classify not in ['0','1','O','A','C','exit']):
            print 'Undefined behavior, please try again!'
            classify = raw_input("0: Anomaly, 1: Normal, O: All anomaly, A: All Normal, C:Continue, Please enter '0', '1', 'O', 'A', 'C' or 'exit':")
        if classify == 'exit':
            print 'Terminating Program...'
            break
        elif classify == 'C': 
            if data in sorted_list: sorted_list.remove(data)
            continue
        elif classify == '0':
            abnormal_data.append(data) 
            clear_ES_record(data['log'])
        elif classify == '1':
            normal_data.append(data)
            if data in sorted_list: sorted_list.remove(data)
            if data in data_dict[user]: data_dict[user].remove(data)
        elif classify == 'A':
            for relevant_data in data_dict[user]:
                normal_data.append(relevant_data)
                if relevant_data in sorted_list: sorted_list.remove(relevant_data)
        elif classify == 'O':
            for relevant_data in data_dict[user]:
                abnormal_data.append(relevant_data)
                if relevant_data in sorted_list: sorted_list.remove(relevant_data)
        
        if data in sorted_list: sorted_list.remove(data)
        if data in data_dict[user]: data_dict[user].remove(data)
    return normal_data, abnormal_data  

def load_training_data(num, log_pairs):
    training_data = []
    
    es = Elasticsearch(hosts, maxsize=max_thread)
    res = es.get(index='ai2', doc_type='data', id='trainingData', ignore=[400,404])
    if res['found']:
        #normal_data = res['_source']['normalData']
        normal_data = json.loads(res['_source']['normalData'])
        training_data.extend( [data['feature'] for data in normal_data] )     
        print '\nFound number of normalData:', len(normal_data)
        print 'Number of trainingData needed:', num 
    
    #normal_data = json.load(open('data/normal_data.json', 'r')) 
    #for data in normal_data:
    #    training_data.append(data['feature'])
    
    if len(training_data) < num:
        new_data = [p[1] for p in log_pairs]
        if (num-len(training_data)) > len(new_data):
            num_new_data = len(new_data) 
        else:
            num_new_data = num-len(training_data) 

        training_data.extend( new_data[:num_new_data] )
    
    if len(training_data) > num:
        random.shuffle(training_data)
        training_data = training_data[:num]

    print 'Number of trainingData return:', len(training_data)
    return training_data

def save_training_data(normal_data, abnormal_data):
    es = Elasticsearch(hosts, maxsize=max_thread)
    res = es.get(index='ai2', doc_type='data', id='trainingData', ignore=[400,404])
    if res['found']:
        original_normal_data = json.loads(res['_source']['normalData']) 
        normal_data.extend(original_normal_data)  
        original_abnormal_data = json.loads(res['_source']['abnormalData']) 
        abnormal_data.extend(original_abnormal_data)  
    
    doc = {
        'normalData':json.dumps(normal_data),
        'abnormalData':json.dumps(abnormal_data)
    }
    print 'Saving number of normalData:', len(normal_data)
    print 'Saving number of abnormalData:', len(abnormal_data)
    res = es.index(index='ai2', doc_type='data', id='trainingData', body=doc, refresh=True)        


if __name__ == '__main__':
    # Define a defualt Elasticsearch client
    hosts = ['192.168.1.1:9200','192.168.1.2:9200','192.168.1.3:9200','192.168.1.4:9200','192.168.1.5:9200','192.168.1.6:9200','192.168.1.10:9200']
    max_thread = 100
    client = connections.create_connection(hosts=hosts, maxsize=max_thread)
    es = Elasticsearch(hosts, maxsize=max_thread)
    es.indices.create(index='ai2',ignore=[400])
    #Record.init()
    #services = ['SMTP', 'VPN', 'Exchange']
    services = ['SMTP']
    
    path = 'rawlog/'
    TEST = False 
    if TEST: 
        ADD_RECORD = ADD_DATA = ADD_FEATURES = False 
    else:
        ADD_RECORD = ADD_DATA = ADD_FEATURES = True 
        ''' 
        start_date = date(2016,6,1)
        end_date = date(2016,6,7)
        for d in dategenerator(start_date, end_date):
            filename = 'all-'+d.strftime('%Y%m%d')+'-geo.log'
            log2Features(path+filename, services)
        ''' 
    
    filename_list = get_filename_list(TEST)

    for filename in filename_list:
        log_pairs = log2Features(path+filename, services)
        SMTP_log_pairs, VPN_log_pairs, Exchange_log_pairs = split_by_service(log_pairs)
        
        from autoencoder import autoencoder
        testing_data = SMTP_log_pairs
        num_training_data = len(SMTP_log_pairs)*10
        training_data = load_training_data( num_training_data, testing_data )
        score_list = autoencoder(training_data, testing_data)
        
        score_list, violation_data  = delete_known_violation(score_list)
        normal_data, abnormal_data = supervision(score_list)
        abnormal_data.extend(violation_data)
        save_training_data(normal_data, abnormal_data)

        json.dump(score_list, open('output/'+filename+'.score', 'wb'), indent=4)
