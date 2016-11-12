# 2016.09.03 08:58:18 CST
#Embedded file name: /home/peng/AI2/autoencoder.py
""" Auto Encoder Example.
Using an auto encoder on MNIST handwritten digits.
References:
    Y. LeCun, L. Bottou, Y. Bengio, and P. Haffner. "Gradient-based
    learning applied to document recognition." Proceedings of the IEEE,
    86(11):2278-2324, November 1998.
Links:
    [MNIST Dataset] http://yann.lecun.com/exdb/mnist/
"""
from __future__ import division, print_function, absolute_import
import tensorflow as tf
import numpy as np
import os, random, json, operator
from operator import itemgetter
import cPickle 
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.6f')

learning_rate = 0.01
n_input = 24 
n_hidden_1 = 12 
n_hidden_2 = 6
X = tf.placeholder('float', [None, n_input])
weights = {
    'encoder_h1': tf.Variable(tf.random_normal([n_input, n_hidden_1])),
    'encoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
    'decoder_h1': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_1])),
    'decoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_input]))
}
biases = {
    'encoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'encoder_b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'decoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'decoder_b2': tf.Variable(tf.random_normal([n_input]))
}

def encoder(x):
    layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['encoder_h1']), biases['encoder_b1']))
    layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['encoder_h2']), biases['encoder_b2']))
    return layer_2

def decoder(x):
    layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['decoder_h1']), biases['decoder_b1']))
    layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['decoder_h2']), biases['decoder_b2']))
    return layer_2

encoder_op = encoder(X)
decoder_op = decoder(encoder_op)
y_pred = decoder_op
y_true = X
cost = tf.reduce_mean(tf.pow(y_true - y_pred, 2))
optimizer = tf.train.RMSPropOptimizer(learning_rate).minimize(cost)


def optimization(sess, train, validation):
    training_epochs = 2000
    batch_size = 256 
    display_step = 100
    total_batch = int(len(train) / batch_size)
    for epoch in range(training_epochs):
        start = 0
        for i in xrange(1, total_batch):
            end = i * batch_size
            batch_xs = train[start:end]
            _, c = sess.run([optimizer, cost], feed_dict={X: batch_xs})
            start = end + 1

        if epoch % display_step == 0:
            print('Epoch:', '%04d' % (epoch + 1), 'cost =', '{:.9f}'.format(c/len(train)))

    print('Optimization Finished!')
    encode_decode, test_cost = sess.run([y_pred, cost], feed_dict={X: validation})
    print('Validation  cost =', '{:.9f}\n'.format(test_cost/len(validation)))
    return sess

def train(trainingData,checkpointFile):
    print('\nBuilding Autoencoder...')
    num_training = int(len(trainingData) * 0.9)
    validationData = trainingData[num_training:]
    trainingData = trainingData[:num_training]
    
    print('training  :', len(trainingData))
    print('validation:', len(validationData))
    with tf.Session() as session:
        init = tf.initialize_all_variables()
        session.run(init)
        session = optimization( session, trainingData, validationData )
        saver = tf.train.Saver()
        saver.save(session, checkpointFile)
    return checkpointFile 

def predict( checkpointFile, featureVectorList):
    score = 0.0
    with tf.Session() as session:
        saver = tf.train.Saver()
        saver.restore(session, checkpointFile)
        encode_decode, score = session.run([y_pred, cost], feed_dict={X: featureVectorList})
    return encode_decode, score


if __name__ == '__main__':
    checkpointFile = 'data/autoencoder.ckpt' 
    TRAIN = False
    if TRAIN:
        with open('data/normalData.p','rb') as f:
            trainingData = [featureVector for idStr, featureVector in cPickle.load(f)]
            random.shuffle(trainingData)
            checkpointFile = train(trainingData[:20000],checkpointFile)
    featureVector = [0.0 for x in range(24)]
    #score, encode_decode = predict( checkpointFile, [featureVector])
    featureVectors = [[y/100.0 for x in range(24)] for y in range(10)]
    encode_decode, score = predict( checkpointFile, featureVectors)
    loss = np.sum(np.power(featureVectors - encode_decode, 2), axis=1)
    for i in xrange(len(featureVectors)):
        print('            cost =', '{:.9f}'.format(loss[i]))
        print('featureVector:',featureVectors[i])
        print('encode_decode:',encode_decode[i])

