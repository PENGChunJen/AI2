from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# Create your views here.
def userDataDetail(request, userId):
    if 'startDate' not in request.GET or 'endDate' not in request.GET:
        return HttpResponse("date parameter not provided", status=400)
    startDate = datetime.strptime(request.GET['startDate'], '%Y-%m-%d') 
    endDate = datetime.strptime(request.GET['endDate'], '%Y-%m-%d') 
    es = Elasticsearch(hosts=settings.ELASTICSEARCH_CONFIG['hosts'])
    req_body = {
        "query": {
            "bool": {
                "must":[{
                    "match": { 
                        "user": userId, 
                    }
                }],
                "filter": [{
                    "range": {
                        "timestamp": {
                            "from": startDate,
                            "to"  : endDate, 
                        }
                    }
                }]
            }
        }
    }
    results = es.search(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type="log", body=req_body)
    return JsonResponse(results["hits"]["hits"], safe=False)
