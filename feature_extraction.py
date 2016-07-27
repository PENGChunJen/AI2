import os
import gc
import json
import sys
import csv
import collections
from operator import itemgetter

csv.field_size_limit(sys.maxsize)

if len(sys.argv) == 1:
    input_file_name = raw_input('Enter a filename: ') or 'inputfile.txt'
else:
    input_file_name = sys.argv[1]

recordfile = open(input_file_name+'.record','w')
#featurefile = open(input_file_name+'.feature', 'w') 
#outputfile = open(input_file_name+'.out', 'w') 

class Service:
    def __init__(self, service_name=None):
        self.service_name = service_name
        self.count = 0
        self.ip_dict = {}
        self.device_dict = {} 

    def add(self, ip, device):
        self.count += 1
        if self.ip_dict.has_key(ip):
            self.ip_dict[ip] += 1
        else:
            self.ip_dict[ip] = 1

        if self.device_dict.has_key(device):
            self.device_dict[device] += 1
        else:
            self.device_dict[device] = 1

    def get_json(self):
        return collections.OrderedDict([ (self.service_name,self.count), ('ip',self.ip_dict), ('device',self.device_dict) ]) 


class Record:
    def __init__(self, user=None, date=None, time=None, service=None, ip=None, LAN=None, device=None):
        self.user = user if user is not None else ''
        self.log_count = 1 if user is not None else 0
        #[user, id_type, #, #, #, {Exchange}, {OWA}, {POP3}, {SMTP}, {VPN}]    
        #{{'service':'Exchange'},{city:num},{device:num},past3_avg,past3_std,past7_avg,past7_std,pastDate_avg, pastDate_std}  
        self.service_dict = collections.OrderedDict([('Exchange',Service('Exchange')), ('OWA',Service('OWA')), ('POP3',Service('POP3')), ('SMTP',Service('SMTP')), ('VPN',Service('VPN'))])
        if self.service_dict.has_key(service):
            self.service_dict[service].add(ip, device)

    def add(self, user=None, date=None, time=None, service=None, ip=None, LAN=None, device=None):
        self.log_count += 1

        if self.service_dict.has_key(service):
            self.service_dict[service].add(ip,device)
    
    def get_json(self):
        d = [] 
        for s in self.service_dict.keys(): 
            d.append(self.service_dict[s].get_json())
        return collections.OrderedDict([('user', self.user), ('log_count', self.log_count), ('services', d)])  


    def __str__(self):
        str = '%20s: log:%4d ip:%3d dev:%3d service:%3d' % (self.user, self.log_count, len(self.ip_dict), len(self.device_dict), len(self.service_dict))
        return str

    def getFeatures(self):
        list = [ self.log_count, len(self.ip_dict.keys()), len(self.device_dict), len(self.service_dict) ]
        return list





with open(input_file_name,"r") as inputfile: 
    unsorted_lists = list(csv.reader(inputfile))
lists = sorted(unsorted_lists, key = itemgetter(3, 2)) # First sort by userid, then sort by time

record = {}
features = []
for list in lists:
    user_id = list[3]
    if record.has_key(user_id):
        record[user_id].add(list[3], list[1], list[2], list[0], list[5], list[4], list[6]) 
    else:
        record[user_id] = Record(list[3], list[1], list[2], list[0], list[5], list[4], list[6]) 
    
    #features.append(record[user_id].getFeatures()) 
    #print record[user_id]
#json.dump(features, featurefile)

# write recordfile
keylist = record.keys()
keylist.sort()
id_type = in_email_num = ex_email_num = 0 
for user in keylist:
    #i = 1
    print user
    j =  json.dumps(record[user].get_json(), ensure_ascii=False, indent=4)
    #os.system("curl -XPUT 'http://localhost:9200/record/record/"+i+"' -d '"+j+"'")
    print >> recordfile, j

del record, features, j
gc.collect()

'''    
record = {}
features = []
last_user = ''
record = Record()
for list in lists:
    if last_user == list[3]:
        record.add(list[3], list[1], list[2], list[0], list[5], list[4], list[6]) 
    else:
        j =  json.dumps(record.get_json(), indent=4)
        print >> recordfile, j
        del j, record
        gc.collect()
        record = Record(list[3], list[1], list[2], list[0], list[5], list[4], list[6]) 
        last_user = list[3]

print list
j =  json.dumps(record.get_json(), indent=4)
print >> recordfile, j
'''

