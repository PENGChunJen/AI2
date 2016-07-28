import cPickle as pickle

features = pickle.load(open('output/r00643001_all-20160601-geo.log.feature', 'rb'))
for f in features:
    print(f)
print('len:',len(features))
