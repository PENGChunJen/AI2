import json, random
import autoencoder
from elasticsearch import Elasticsearch
import cPickle
import numpy as np

import config

def renewTrainingData(fileName):
    es = Elasticsearch(hosts=config.hosts)
    res = es.get(index=config.indexName, doc_type='data', id='trainingData', ignore=[400,404])
    if res['found']:
        normalData = json.loads(res['_source']['normalData'])
        
        print 'Found number of normalData:', len(normalData)
        pList = []
        for data in normalData:
            idStr = '%sT%s_%s_%s'%(data['log'][1], data['log'][2], 
                                   data['log'][3], data['log'][0]) 
            data['feature'].extend([0.0 for x in range(16)])
            pList.append((idStr, data['feature']))

        print('Saving %d training data into %s...'%(len(pList),fileName))
        cPickle.dump(pList, open(fileName, 'wb')) 

def loadTrainingData(fileName, num):
    try:
        f = open(fileName, 'rb')
    except IOError:
        print("Cannot find training data file: %s, loading data from ES..."%fileName)
        renewTrainingData(fileName)
        f = open(fileName, 'rb')
    with f:
        trainingData = [featureVector for idStr, featureVector in cPickle.load(f)]

    if len(trainingData) > num:
        random.shuffle(trainingData)
        trainingData = trainingData[:num]

    return trainingData

def loadModel(modelName):
    es = Elasticsearch(hosts=config.hosts)
    res = es.get(index=config.indexName, doc_type='model', id=modelName, ignore=[400,404])
    if res['found']:
        model = cPickle.loads(res['_source']) 
    else:
        print('Cannot find model:%s in ES, Generating new model...'%modelName)
        trainingDataNum = 20000 
        trainingData = loadTrainingData('normalData.p',trainingDataNum)
        model = autoencoder.train(trainingData)
        print 'model:',model
        res = es.index(index=config.indexName, doc_type='model', id=modelName, body=cPickle.dumps(model))
    return model


def generateScore(data):
    #checkpointFile = loadModel('autoencoder') #should be global?
    #checkpointFile = 'data/autoencoder.ckpt' 
    #score, encode_decode = autoencoder.predict( checkpointFile, [data['featureVector']])
    score = 0.0
    data['scores']['autoencoder'] = score
    return data

def generateScoreList(dataList):
    score = 0.0
    #checkpointFile = loadModel('autoencoder') #should be global?
    featureVectors = [ data['featureVector'] for data in dataList ]
    checkpointFile = 'data/autoencoder.ckpt' 
    encode_decode, score = autoencoder.predict( checkpointFile, featureVectors)
    loss = np.sum(np.power(featureVectors - encode_decode, 2), axis=1)
    for i in xrange(len(featureVectors)):
        #print('         loss:', '{:.9f}'.format(loss[i]))
        #print('featureVector:',featureVectors[i])
        #print('encode_decode:',encode_decode[i])
        dataList[i]['scores']['autoencoder'] = loss[i]
    return dataList

if __name__ == '__main__':
    
    #renewTrainingData('data/normalData.p')
    featureVector = [0.0 for x in xrange(24)]
    testingData = {
        'log':None,
        'featureVector':featureVector,
        'scores':{},
        'label':{'analyst':None}
    }
    data = generateScore(testingData)
    print data['scores']
