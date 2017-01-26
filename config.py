from datetime import date

# Global vairables

#index names must be LOWER CASE!!!
indexName = 'dec_test' 

startDate = date(2016,12,1) 
endDate = date(2016,12,1) 
logPath = 'rawlog'

whiteList = ['140.112.*', '209,85,*']

#services = ['SMTP'] # 0.5%
services = ['SMTP', 'VPN', 'Exchange'] # 2%
#services = ['SMTP', 'VPN', 'Exchange', 'POP3'] # 20%
#services = ['SMTP', 'VPN', 'Exchange', 'POP3', 'OWA'] # 100% Don't touch this unless certain

# Define a defualt Elasticsearch client
hosts = ['localhost:9200']
'''
hosts = ['192.168.28.4:9200',
         '192.168.28.5:9200',
         '192.168.28.6:9200']
'''
maxThread = 5000
