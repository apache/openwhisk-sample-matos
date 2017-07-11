/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.matos;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

public class Load {

    private static Logger logger = Logger.getLogger(Load.class);
    private static boolean done = false;
    private static long lastOffset;
    private static Config config;


    // main method for Whisk action
    public static JsonObject main(JsonObject args) {

        try {
            Utils.initDirs();
            Utils.extractResourcesToFilesystem(true);

            config = new Config();
            config.overrideProperties(args);

            // invoke the "real" main method, shared with Java main
            doMain();

        } catch (Exception e) {
            e.printStackTrace();
        }

        JsonObject response = new JsonObject();
        response.addProperty("last", getLastOffset());
        return response;
    }

    public static void main(String args[]) throws InterruptedException, ExecutionException, IOException {

        Utils.initDirs();
        Utils.extractResourcesToFilesystem(true);

        if (args.length == 3) {
            config = new Config(args[0]);
            HashMap<String,String> amap = new HashMap<String,String>();
            amap.put(Config.KAFKA_API_KEY_PROP, args[1]);
            amap.put(Config.KAFKA_NUM_RECORDS_PROP, args[2]);
            config.overrideProperties(amap);
        } else {
            logger.log(Level.ERROR, "Usage:\n\n" +
                    "java -jar <name_of_jar>.jar <config-json-file-name> " +
                    "<kafka_api_key> <kafka_num_records>");
            return;
        }
        // invoke the "real" main method, shared with Whisk's main
        doMain();
    }

    public static void doMain() throws InterruptedException, ExecutionException {

        KafkaProducer<byte[], byte[]> kafkaProducer;
        String kafkaHost;
        String apiKey;
        String topic;
        int numRecords;
        int producedMessages;

        logger.log(Level.INFO, "Starting " + Load.class.getSimpleName() + "; CONFIG:");
        logger.log(Level.INFO, config);

        Utils.setJaasLocation();

        kafkaHost = config.get(Config.KAFKA_BROKER_PROP);
        apiKey = config.get(Config.KAFKA_API_KEY_PROP);
        topic = config.get(Config.KAFKA_TOPIC_PROP);
        numRecords = Integer.parseInt(config.get(Config.KAFKA_NUM_RECORDS_PROP));

        Utils.updateJaasConfiguration(apiKey.substring(0, 16), apiKey.substring(16));

        kafkaProducer = new KafkaProducer<byte[], byte[]>(
                Utils.getClientConfiguration(kafkaHost, true));

        done = false;
        producedMessages = 0;
        lastOffset = -1;
        Future<RecordMetadata> fm = null;

        while (!done) {
            String fieldName = "records";
            // Push a message into the list to be sent.
            MessageList list = new MessageList();
            long now = System.currentTimeMillis();
            list.push("This is a test message[" + producedMessages + "] ["
                    + new Date(now).toString()
                    + "|" + now + "]");

            try {
                // Create a producer record which will be sent
                // to the Message Hub service, providing the topic
                // name, field name and message. The field name and
                // message are converted to UTF-8.
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic,
                        fieldName.getBytes("UTF-8"), list.toString().getBytes("UTF-8"));

                // keep the metadata of the last produced message
                fm = kafkaProducer.send(record);
                producedMessages++;

                if(producedMessages >= numRecords) {
                    done = true;
                }
            } catch (final Exception e) {
                e.printStackTrace();
                done = true;
            }
        }
        // wait until last message has been sent, retrieve its offset
        RecordMetadata m = fm.get();
        logger.log(Level.INFO, "[" + producedMessages + " messages sent, last offset: " + m.offset() + "]");
        lastOffset = m.offset()+1;  // 'next after last' offset

        logger.log(Level.INFO, Load.class.toString() + " is shutting down.");
        kafkaProducer.close();

        System.out.println("Last offset: " + getLastOffset());
    }

    public static long getLastOffset() {
        return lastOffset;
    }

}
