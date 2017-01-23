from datetime import date

# Global vairables

#index names must be LOWER CASE!!!
indexName = '2016dec' 

startDate = date(2016,11,24) 
endDate = date(2016,11,30) 
logPath = 'rawlog'

whiteList = ['140.112.*', '209,85,*']

# Define a defualt Elasticsearch client
hosts = ['localhost:9200']
'''
hosts = ['192.168.28.4:9200',
         '192.168.28.5:9200',
         '192.168.28.6:9200']
'''
maxThread = 5000
