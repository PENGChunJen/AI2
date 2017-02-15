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
        "size": 30,
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
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
        attackTimestamp = None
        for datum in data:
            datumTimestamp = datetime.strptime(datum['_source']['log']['timestamp'], '%Y-%m-%dT%H:%M:%S')

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
            logIdStr = "%s_%s_%s" % (datumTimestamp.isoformat(), datum['_source']['log']['user'], datum['_source']['log']['service'])
            es.update(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='data', id=datum['_id'], body=dataUpdatePartialDoc)
            es.update(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='log', id=logIdStr, body=logUpdatePartialDoc)

            if datum['label'] != 'normal':
                if attackTimestamp == None:
                    attackTimestamp = datumTimestamp
                else:
                    attackTimestamp = datumTimestamp if datumTimestamp < attackTimestamp else attackTimestamp
        if attackTimestamp != None:
            update_point = { 'timestamp': attackTimestamp }
            es.index(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='update_point', id=userId, body=update_point)

    return JsonResponse({'status': 'OK'})
