from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import cPickle

# Create your views here.
def userDataDetail(request, userId):
    if 'startDate' not in request.GET or 'endDate' not in request.GET:
        return HttpResponse("date parameter not provided", status=400)
    startDate = datetime.strptime(request.GET['startDate'], '%Y-%m-%d') 
    endDate = datetime.strptime(request.GET['endDate'], '%Y-%m-%d') 
    es = Elasticsearch(hosts=settings.ELASTICSEARCH_CONFIG['hosts'])
    userBlob = es.get(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type='userData', id=userId)
    userData = cPickle.loads(str(userBlob['_source']['blob']))
    targetLogs = []
    for dayKey, logs in userData['timestamps'].items():
        day = datetime.strptime(dayKey, "%Y-%m-%d")
        targetLogs += logs if startDate <= day <= endDate else []
    return JsonResponse(targetLogs, safe=False)
