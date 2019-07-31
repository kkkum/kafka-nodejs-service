/**
 * Copyright 2015-2018 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/**
 * Licensed Materials - Property of IBM
 * © Copyright IBM Corp. 2015-2018
 */

var consumer;
var consumerLoop;

var MongoClient = require('mongodb').MongoClient, assert = require('assert');
var url = "mongodb://mongouser:mongopassw0rd@mongodb.k8s-mvp.svc.cluster.local/sampledb";

var exports = module.exports = {};
exports.consumerLoop = consumerLoop;

/**
 * Constructs a KafkaConsumer and registers listeners on the most common events
 * 
 * @param {object} Kafka - an instance of the node-rdkafka module
 * @param {object} consumer_opts - consumer configuration
 * @param {string} topicName - name of the topic to consumer from
 * @param {function} shutdown - shutdown function
 * @return {KafkaConsumer} - the KafkaConsumer instance
 */
exports.buildConsumer = function(Kafka, consumer_opts, topicName, shutdown) {
    var topicOpts = {
        'auto.offset.reset': 'latest'
    };

    // function to insert documents into mongoDB
    var insertDocuments = function(db, docs, callback) {
        // Get the documents collection
        var collection = db.collection('messages');
        // Insert some documents
        collection.insertMany(docs, function(err, result) {
          assert.equal(err, null);
          // assert.equal(3, result.result.n);
          // assert.equal(3, result.ops.length);
          console.log("Inserted" + result.result.n + " documents into the collection");
          callback(result);
        });
    };
    var dbObject;
    // var clientObject;

    consumer = new Kafka.KafkaConsumer(consumer_opts, topicOpts);

    // Register listener for debug information; only invoked if debug option set in driver_options
    consumer.on('event.log', function(log) {
        console.log(log);
    });

    // Register error listener
    consumer.on('event.error', function(err) {
        console.error('Error from consumer:' + JSON.stringify(err));
    });

    var consumedMessages = []
    // Register callback to be invoked when consumer has connected
    consumer.on('ready', function() {
        console.log('The consumer has connected.');

        // Connect to the mongoDB instance if the MONGODB_URL is valid

        if (url) {
            MongoClient.connect(url, { useNewUrlParser: true }, (err, client) => {
                assert.equal(null, err);
                dbObject = client.db('sampledb');
                // clientObject = client;
                console.log('database connected!');
/*
                var docs = [
                    {key : 1, value: "This is message 1"},
                    {key : 2, value: "This is message 2"},
                    {key : 3, value: "This is message 3"}
                  ];

                insertDocuments(db, docs, function() {
                    client.close();
                  });
                  */
            });
        }

        // request metadata for one topic
        consumer.getMetadata({
            topic: topicName,
            timeout: 10000
        }, 
        function(err, metadata) {
            if (err) {
                console.error('Error getting metadata: ' + JSON.stringify(err));
                shutdown(-1);
            } else {
                console.log('Consumer obtained metadata: ' + JSON.stringify(metadata));
                if (metadata.topics[0].partitions.length === 0) {
                    console.error('ERROR - Topic ' + topicName + ' does not exist. Exiting');
                    shutdown(-1);
                }
            }
        });

        consumer.subscribe([topicName]);

        consumerLoop = setInterval(function () {
            if (consumer.isConnected()) { 
                // The consume(num, cb) method can take a callback to process messages.
                // In this sample code we use the ".on('data')" event listener instead,
                // for illustrative purposes.
                consumer.consume(10);
            }    

            if (consumedMessages.length === 0) {
                console.log('No messages consumed');
            } else {
                var docs = [];
                for (var i = 0; i < consumedMessages.length; i++) {
                    var m = consumedMessages[i];                 
                    console.log('Message consumed: topic=' + m.topic + ', partition=' + m.partition + ', offset=' + m.offset + ', key=' + m.key + ', value=' + m.value.toString());

                    // Push the message into the docs array for storage into mongoDB
                    var msg = {key: m.key, value: m.value.toString()};
                    docs.push(msg);
                }
                // Insert messages into mongoDB instance
                insertDocuments(dbObject, docs, function() {
                    console.log("Insert messages into mongoDB");
                  });


                consumedMessages = [];
            }
        }, 2000);
    });

    // Register a listener to process received messages
    consumer.on('data', function(m) {
        consumedMessages.push(m);
    });
    return consumer;
}
