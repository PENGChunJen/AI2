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
import cPickle as pickle
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.6f')

def initialization(sess, learning_rate):
    n_input = 8
    n_hidden_1 = 4
    n_hidden_2 = 2
    X = tf.placeholder('float', [None, n_input])
    weights = {'encoder_h1': tf.Variable(tf.random_normal([n_input, n_hidden_1])),
     'encoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
     'decoder_h1': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_1])),
     'decoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_input]))}
    biases = {'encoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
     'encoder_b2': tf.Variable(tf.random_normal([n_hidden_2])),
     'decoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
     'decoder_b2': tf.Variable(tf.random_normal([n_input]))}

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
    init = tf.initialize_all_variables()
    sess.run(init)
    return (sess,
     X,
     y_pred,
     cost,
     optimizer)


def optimization(sess, training_features, testing_features, X, y_pred, cost, optimizer, training_epochs, batch_size, display_step):
    total_batch = int(len(training_features) / batch_size)
    for epoch in range(training_epochs):
        start = 0
        for i in xrange(1, total_batch):
            end = i * batch_size
            batch_xs = training_features[start:end]
            _, c = sess.run([optimizer, cost], feed_dict={X: batch_xs})
            start = end + 1

        if epoch % display_step == 0:
            print('Epoch:', '%04d' % (epoch + 1), 'cost=', '{:.9f}'.format(c))

    print('Optimization Finished!')
    encode_decode, test_cost = sess.run([y_pred, cost], feed_dict={X: testing_features})
    print('testing features avg cost =', '{:.9f}\n'.format(test_cost))
    return sess


def autoencoder( training_data, log_pairs):
    print('\nBuilding Autoencoder...')
    score_list = []
    #features = [ p[1] for p in log_pairs ]
    num_training = int(len(training_data) * 0.9)
    random.shuffle(training_data)
    training_features = training_data[:num_training]
    testing_features = training_data[num_training:]
    learning_rate = 0.01
    training_epochs = 1000
    batch_size = 256 if num_training >= 2560 else int(num_training / 10)
    display_step = 100
    print('total   :', len(training_data))
    print('training:', len(training_features))
    print('testing :', len(testing_features))
    sess = tf.Session()
    sess, X, y_pred, cost, optimizer = initialization(sess, learning_rate)
    sess = optimization(sess, training_features, testing_features, X, y_pred, cost, optimizer, training_epochs, batch_size, display_step)
    print('Generating scores for all features...')
    for log_pair in log_pairs:
        log = log_pair[0]
        feature = log_pair[1]
        encode_decode, score = sess.run([y_pred, cost], feed_dict={X: [feature]})
        data = {
            'log': log,
            'feature': feature,
            'encode_decode': [ float(e) for e in encode_decode[0] ],
            'score': float(score)
        }
        score_list.append(data)
    
    sorted_list = sorted(score_list, key=itemgetter('score'), reverse=True)
    return sorted_list


def load_training_data_from_file():
    log_pairs = []
    path = 'data/SMTP_without_first7Days/'
    for filename in os.listdir(path):
        print('Loading', filename, '...')
        log_pairs.extend(pickle.load(open(path + filename, 'rb')))

    return log_pairs


if __name__ == '__main__':
    log_pairs = pickle.load(open('data/SMTP_without_first7Days/all-20160608-geo.log.SMTP.feature', 'rb'))
    score_list = autoencoder(log_pairs)
