import cPickle as pickle

features = pickle.load(open('output/violation-201606.txt.feature', 'rb'))
for f in features:
    print(f[0], ','.join(f[1]))
print('len:',len(features))
