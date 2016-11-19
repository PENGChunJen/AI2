from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from elasticsearch import Elasticsearch, helpers

# Create your views here.
def outliers(request):
    queryDSL = {
        "query": {
            "bool": {
                "filter": {
                    "missing": {
                        "field": "label.analyst"
                    }
                }
            }
        }
    }
    es = Elasticsearch(hosts=settings.ELASTICSEARCH_CONFIG['hosts'])
    res = es.search(index=settings.ELASTICSEARCH_CONFIG['indexName'], doc_type="data", body=queryDSL)
    return JsonResponse(res['hits']['hits'], safe=False)
