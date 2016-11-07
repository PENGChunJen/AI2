import sys, csv, json

# Global vairables
indexName = 'ai2_test'
#indexName = 'ai2_v2.0'
startDate = date(2016,6,1) 
endDate = date(2016,6,1) 
whiteList = ['140.112.*', '209,85,*']



def run():
    fileNameList = generateFileNameList(startDate, endDate)

if __name__ == '__main__':
    run()
