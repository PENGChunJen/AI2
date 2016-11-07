from autoencoder import autoencoder

def loadModel():
    model = []
    return model

def predict( model, featureVector):
    score = 0.0
    return score

def generateScore(data):
    '''
    data = {
        'log':log,
        'featureVector':featureVector,
        'scores':defaultdict(float),
        'label':None
    }
    '''
    model = loadModel()
    score = predict(model, data['featureVector'])
    data['scores']['autoencoder'] = score
    return data
