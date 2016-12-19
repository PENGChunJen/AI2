from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import cPickle
import json

# Create your views here.
def outliers(request):
    if 'from' not in request.GET:
        return HttpResponse("date parameter not provided", status=400)
    from_num = int(request.GET['from'])
    queryDSL = {
        "from": from_num,
        "size": 10,
        "query": {
            "bool": {
                "filter": {
                    "missing": {
                        "field": "label.analyst"
                    }
                }
            }
        },
        "sort": {
            "scores.autoencoder": {
                "order": "desc"
            }
        },
    }
    es = Elasticsearch(hosts=settings.ELASTICSEARCH_CONFIG['hosts'])
    res = es.search(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type="data", body=queryDSL)
    return JsonResponse(res['hits']['hits'], safe=False)

def labelData(request):
    payload = json.loads(request.body)
    users = {}
    for datum in payload['labeled_data']:
        user = datum['_source']['log']['user']
        if user not in users:
            users[user] = [ datum ]
        else:
            users[user].append(datum)

    for (userId, data) in users.items():
        es = Elasticsearch(hosts=settings.ELASTICSEARCH_CONFIG['hosts'])
        userBlob = es.get(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='userData', id=userId)
        userData = cPickle.loads(str(userBlob['_source']['blob']))
        attackTimestamp = None
        for datum in data:
            datumTimestamp = timestamp = datetime.strptime(datum['_source']['log']['timestamp'], '%Y-%m-%dT%H:%M:%S')
            logs = userData['timestamps'][datumTimestamp.date().isoformat()]
            for idx, log in enumerate(logs):
                if datumTimestamp == log['timestamp']:
                    if datum['label'] == 'normal':
                        log['label']['analyst'] = datum['label']
                    else:
                        del logs[idx]
                    break

            if datum['label'] != 'normal':
                if attackTimestamp == None:
                    attackTimestamp = datumTimestamp
                else:
                    attackTimestamp = datumTimestamp if datumTimestamp < attackTimestamp else attackTimestamp

            for cacheKey in [ 'services', 'IPs', 'devices', 'counties', 'nations' ]:
                should_deleted_keys = []
                for key in userData[cacheKey]:
                    if datumTimestamp in userData[cacheKey][key]:
                        userData[cacheKey][key].remove(datumTimestamp)
                    if len(userData[cacheKey][key]) == 0:
                        should_deleted_keys.append(key)
                for key in should_deleted_keys:
                    del userData[cacheKey][key]

            logIdStr = "%s_%s_%s" % (datumTimestamp.isoformat(), datum['_source']['log']['user'], datum['_source']['log']['service'])
            dataUpdatePartialDoc = {
                'doc': {
                    'label': {
                        'analyst': datum['label']
                    }
                }
            }
            logUpdatePartialDoc = {
                'doc': {
                    'label':{
                        'analyst': datum['label']
                    }
                }
            }
            es.update(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='data', id=datum['_id'], body=dataUpdatePartialDoc)
            es.update(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='log', id=logIdStr, body=logUpdatePartialDoc)
        
        userData['attackTimestamp'] = attackTimestamp
        es.index(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='userData', id=userId, body={ 'blob': cPickle.dumps(userData) })

    return JsonResponse({'status': 'OK'})
