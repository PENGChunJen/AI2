indexName = 'ai2_test'

log = {
    'user': user,
    'timestamp': timestamp,
    'service': service,
    'IP': IP,
    'device': device,
    'server': server,
    'city': city,
    'county': county,
    'nation': nation,
    'label':label
}
res = es.index(
        index = indexName, 
        doc_type = 'log', 
        id = timestamp+'_'+user+'_'+service,
        body = log, 
        refresh = True
      )

#Speed? if IP in userData['IPs']
# userData = { 'IPs':{ IP:[timestamp] } }
userData = {
    'services':defaultdict(list),
    'IPs':defaultdict(list),
    'devices':defaultdict(list),
    'cities':defaultdict(list),
    'counties':defaultdict(list),
    'nations':defaultdict(list),
    'timestamps':[
       {
          'timestamp':timestamp
          # What to save?
          'logs':[log]
       }
    ]
}
res = es.index(
        index = indexName, 
        doc_type = 'userData', 
        id = user,
        body = userData, 
        refresh = True
      )


feature = {
    'login':login,
    'featureVector':featureVector,
    'autoencoderScore':score,
    'label':label
}

res = es.index(
        index = indexName, 
        doc_type = 'feature', 
        id = timestamp+'_'+user+'_'+service,
        body = feature, 
        refresh = True
      )
