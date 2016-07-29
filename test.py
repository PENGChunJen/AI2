import cPickle as pickle

features = pickle.load(open('output/hhl/all-20160601-geo.log.feature', 'rb'))
for f in features:
    print(f[0], ','.join(f[1]))
print('len:',len(features))
