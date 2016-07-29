# -*- coding: utf-8 -*-

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
#import matplotlib.pyplot as plt

import os, random, json, operator
import cPickle as pickle

# Import MINST data
#from tensorflow.examples.tutorials.mnist import input_data
#mnist = input_data.read_data_sets("/tmp/data/", one_hot=True)



#features = pickle.load(open('output/testInput.log.feature', 'rb'))
#for f in features: print(f)

'''
user = 'b00606012'
path = 'data/output_only_features/'+user+'/'
features = []
for filename in os.listdir(path):
    features.extend( pickle.load(open(path+filename, 'rb')) )
print('user:', user)
'''

from log2elasticsearch import getViolationList
violationUsers = getViolationList()
log_pairs = []
for user in violationUsers:
    path = 'output/'+user+'/'
    for filename in os.listdir(path):
        log_pairs.extend( pickle.load(open(path+filename, 'rb')))
log_dict = {str(p[0]): p[1] for p in log_pairs}

features = [ p[0] for p in log_pairs ]
#features = []
#for p in log_pairs:
#    if p[0] is not None:
#        features.append(p[0]) 

num_examples = int(len(features)*0.9) #1281686
random.shuffle(features)
train_features = features[:num_examples]
test_features = features[num_examples:]
#for f in test_features: print(f)

violation_pairs = pickle.load(open('data/violation-201606.txt.feature'))
violation_dict = {str(p[0]): p[1] for p in violation_pairs}
#violation_features = [v[0] for v in violation_pairs]
violation_features = []
for v in violation_pairs:
    if v[0] is not None:
        violation_features.append(v[0])
#for f in violation_features: print(f)

print('    total features:', len(features))
print('      num_examples:', num_examples)
print('     test_features:', len(test_features))
print('violation_features:', len(violation_features))

# Parameters
learning_rate = 0.01
#training_epochs = 2000
training_epochs = 5000
#batch_size = 256
batch_size = int(num_examples/10)
display_step = 100
#examples_to_show = 1 
examples_to_show = len(test_features) if len(test_features) <= 5 else 5 




# Network Parameters
n_input = 8 # MNIST data input (img shape: 28*28)
n_hidden_1 = 4 # 1st layer num features
n_hidden_2 = 2 # 2nd layer num features

# tf Graph input (only pictures)
X = tf.placeholder("float", [None, n_input])

weights = {
    'encoder_h1': tf.Variable(tf.random_normal([n_input, n_hidden_1])),
    'encoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
    'decoder_h1': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_1])),
    'decoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_input])),
}
biases = {
    'encoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'encoder_b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'decoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'decoder_b2': tf.Variable(tf.random_normal([n_input])),
}


# Building the encoder
def encoder(x):
    # Encoder Hidden layer with sigmoid activation #1
    layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['encoder_h1']),
                                   biases['encoder_b1']))
    # Decoder Hidden layer with sigmoid activation #2
    layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['encoder_h2']),
                                   biases['encoder_b2']))
    return layer_2


# Building the decoder
def decoder(x):
    # Encoder Hidden layer with sigmoid activation #1
    layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['decoder_h1']),
                                   biases['decoder_b1']))
    # Decoder Hidden layer with sigmoid activation #2
    layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['decoder_h2']),
                                   biases['decoder_b2']))
    return layer_2

# Construct model
encoder_op = encoder(X)
decoder_op = decoder(encoder_op)

# Prediction
y_pred = decoder_op
# Targets (Labels) are the input data.
y_true = X

# Define loss and optimizer, minimize the squared error
cost = tf.reduce_mean(tf.pow(y_true - y_pred, 2))
optimizer = tf.train.RMSPropOptimizer(learning_rate).minimize(cost)

# Initializing the variables
init = tf.initialize_all_variables()

# Launch the graph
with tf.Session() as sess:
    sess.run(init)
    total_batch = int(num_examples/batch_size)
    # Training cycle
    for epoch in range(training_epochs):
        # Loop over all batches
        start = 0
        for i in xrange(1,total_batch):
            end = i*batch_size
            batch_xs = train_features[start:end]
            #print(batch_xs)
            # Run optimization op (backprop) and cost op (to get loss value)
            _, c = sess.run([optimizer, cost], feed_dict={X: batch_xs})
            start = end+1

        # Display logs per epoch step
        if epoch % display_step == 0:
            print("Epoch:", '%04d' % (epoch+1),
                  "cost=", "{:.9f}".format(c))

    print("Optimization Finished!")

    # Applying encode and decode over test set
    encode_decode, test_cost = sess.run(
        [y_pred, cost], feed_dict={X: test_features})
    print("\ntesting features avg cost =", "{:.9f}\n".format(test_cost))
    
    examples = test_features
    abnormal = normal = 0
    abnormal_avg_cost = normal_avg_cost = 0.0
    falsePositive = {}
    for i in range(len(examples)-1):
        encode_decode, c = sess.run(
            [y_pred, cost], feed_dict={X: [examples[i]]})
        if c > test_cost:
            abnormal += 1
            abnormal_avg_cost += c
            falsePositive[ repr( log_dict[str(examples[i])] ) ] = c
            #print('testing: ', ', '.join('{0:.3f}'.format(k) for k in examples[i]))
            #print('encoder: ', ', '.join('{0:.3f}'.format(k) for k in encode_decode[0]))
            #print("cost =", "{:.9f}\n".format(c))
        else:
            normal += 1
            normal_avg_cost += c
    
    print('for testing features:') 
    print('False Positive:', abnormal, ', cost = ', abnormal_avg_cost/abnormal)
    print('        Normal:', normal, ', cost = ', normal_avg_cost/normal)
    
    print('\nFalse Positive (Could be a threat!)')  
    with open('output/falsePositive', 'wb') as outFile: 
        for k, v in sorted(falsePositive.items(), key=operator.itemgetter(1), reverse=True):
            print('score:',v, k)
            outFile.write('score: '+str(v)+' '+str(k)+'\n')
        #json.dump(sorted(falsePositive), outFile, ensure_ascii=False, indent=4) 



    # Applying encode and decode over test set
    encode_decode, violation_cost= sess.run(
        [y_pred, cost], feed_dict={X: violation_features})
    print("\nviolation features avg cost =", "{:.9f}\n".format(violation_cost))

    examples = violation_features
    abnormal = normal = 0
    abnormal_avg_cost = normal_avg_cost = 0.0
    falseNegative = {}
    for i in range(len(examples)-1):
        encode_decode, c = sess.run(
            [y_pred, cost], feed_dict={X: [examples[i]]})
        if c > test_cost:
            abnormal += 1
            abnormal_avg_cost += c
            #print('testing: ', ', '.join('{0:.3f}'.format(k) for k in examples[i]))
            #print('encoder: ', ', '.join('{0:.3f}'.format(k) for k in encode_decode[0]))
            #print("cost =", "{:.9f}\n".format(c))
        else:
            falseNegative[ repr( violation_dict[str(examples[i])] ) ] = c
            normal += 1
            normal_avg_cost += c
    print('for violation features:') 
    print('abnormal:', abnormal, ', cost = ', abnormal_avg_cost/abnormal)
    print('normal :', normal, ', cost = ', normal_avg_cost/normal)
    
    print('\nFalse Negative (Should be normal logs!)')  
    with open('output/falseNegative', 'wb') as outFile: 
        for k, v in sorted(falseNegative.items(), key=operator.itemgetter(1)):
            print('score:',v, k)
            outFile.write('score: '+str(v)+' '+str(k)+'\n')


    SHOW_EXAMPLE = False 
    if SHOW_EXAMPLE:     
        print('\nEXAMPLES:\n')
        # Compare original images with their reconstructions
        examples = test_features[:examples_to_show]
        for i in range(examples_to_show):
            encode_decode, c = sess.run(
                [y_pred, cost], feed_dict={X: [examples[i]]})
            print('testing: ', ', '.join('{0:.3f}'.format(k) for k in examples[i]))
            print('encoder: ', ', '.join('{0:.3f}'.format(k) for k in encode_decode[0]))
            print("cost =", "{:.9f}\n".format(c))
    
        examples = violation_features[:examples_to_show]
        # Compare original images with their reconstructions
        for i in range(examples_to_show):
            encode_decode, c = sess.run(
                [y_pred, cost], feed_dict={X: [examples[i]]})
            print('abnormal: ', ', '.join('{0:.3f}'.format(k) for k in violation_features[i]))
            print('encoder: ', ', '.join('{0:.3f}'.format(k) for k in encode_decode[0]))
            print("cost =", "{:.9f}\n".format(c))
