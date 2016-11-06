indexName = 'ai2_test'

login = {
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
        body = login, 
        refresh = True
      )

#Speed? if IP in userData['IPs']
# userData = { 'IPs':{ IP:[timestamp] } }
userData = {
    'services':[
        {
            'service':service,
            'timestamps':[timestamp] #better DS to save time?
        }
    ],
    'IPs':[
        {
            'IP':IP,
            'timestamps':[timestamp]
        }
    ],
    'devices':[
        {
            'device':device,
            'timestamps':[timestamp]
        }
    ],
    'cities':[
        {
            'city':city,
            'timestamps':[timestamp]
        }
    ],
    'counties':[
        {
            'county':county,
            'timestamps':[timestamp]
        }
    ],
    'nations':[
        {
            'nation':nation,
            'timestamps':[timestamp]
        }
    ],
    'dates':[
       {
          'date':date
          # What to save?
          'logins':[login]
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
