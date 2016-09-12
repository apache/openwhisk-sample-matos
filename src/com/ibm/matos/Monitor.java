/**
 * Copyright 2015 IBM
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
 * (c) Copyright IBM Corp. 2015
 */
package com.ibm.matos;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

public class Monitor {

	private static Logger logger = Logger.getLogger(Monitor.class);
	private static long lastOffset;
	private static long committedOffset;
	private static Config config;
	private final static String KAFKA_CONSUMER_ID_KEY = "group.id";
	
	// main method for Whisk action
	public static JsonObject main(JsonObject args) {

		try {
			Utils.initDirs();
			Utils.extractResourcesToFilesystem(false);

			config = new Config();
			config.overrideProperties(args);
			
			// invoke the "real" main method, shared with Java main
			doMain(true);

		} catch (Exception e) {
			e.printStackTrace();
		}

		JsonObject response = new JsonObject();
		response.addProperty("last", getLastOffset());
		response.addProperty("committed", getCommittedOffset());
		return response;
	}

	public static void main(String args[]) throws InterruptedException, ExecutionException, IOException {

		Utils.initDirs();
		Utils.extractResourcesToFilesystem(false);

		if (args.length == 2 || args.length == 3) {
			config = new Config(args[0]);
			HashMap<String,String> amap = new HashMap<String,String>();
			amap.put(Config.KAFKA_API_KEY_PROP, args[1]);
			if (args.length == 3) {
				amap.put(Config.KAFKA_PARTITION_PROP, args[2]);
			}
			config.overrideProperties(amap);
		} else {
			logger.log(Level.ERROR, "Usage:\n\n" +
					"java -jar <name_of_jar>.jar <config-json-file-name> " +
					"<kafka_api_key> [<kafka_partition>]");
			return;
		}
		// invoke the "real" main method, shared with Whisk's main
		doMain(false);
	}
		
	private static void doMain(boolean once) throws InterruptedException {
		
		String kafkaHost;
		String apiKey;
		String topic;
		int partition;
		String consumerId;
		boolean done = false;

		Utils.setJaasLocation();

		logger.log(Level.INFO, "Starting " + Monitor.class.getSimpleName() + "; CONFIG:");
		logger.log(Level.INFO, config);

		kafkaHost = config.get(Config.KAFKA_BROKER_PROP);
		apiKey = config.get(Config.KAFKA_API_KEY_PROP);
		topic = config.get(Config.KAFKA_TOPIC_PROP);
		partition = Integer.parseInt(config.get(Config.KAFKA_PARTITION_PROP));
		consumerId = config.get(Config.KAFKA_CONSUMER_ID_PROP);
		
		Utils.updateJaasConfiguration(apiKey.substring(0, 16), apiKey.substring(16));

		TopicPartition tp = new TopicPartition(topic, partition);

		// configure Kafka consumer that will be used to retrieve the last offset
		Properties propsLast = Utils.getClientConfiguration(kafkaHost, false);
		KafkaConsumer<byte[], byte[]> kafkaConsumerLast = new KafkaConsumer<byte[], 
				byte[]>(propsLast, new ByteArrayDeserializer(),	new ByteArrayDeserializer());

		// configure Kafka consumer that will be used to retrieve the committed offset
		Properties propsCommitted = Utils.getClientConfiguration(kafkaHost, false);
		propsCommitted.put(KAFKA_CONSUMER_ID_KEY, consumerId);
		KafkaConsumer<byte[], byte[]> kafkaConsumerCommitted = new KafkaConsumer<byte[], 
				byte[]>(propsCommitted, new ByteArrayDeserializer(),	new ByteArrayDeserializer());

		lastOffset = committedOffset = -1;

		while (!done) {
			// retrieve last and committed offset in the topic-partition
			updateLastOffset(kafkaConsumerLast, tp);
			updateCommittedOffset(kafkaConsumerCommitted, tp);
			System.out.print("[" + getCommittedOffset() + "," + getLastOffset() + "]");
			
			if (once) {
				done = true;
			} else {
				Thread.sleep(5000);
			}
		}

		kafkaConsumerLast.close();
		kafkaConsumerCommitted.close();

		logger.log(Level.INFO, "Shutting down " + Monitor.class.getSimpleName());
		
	}
	
	private static void updateLastOffset(KafkaConsumer<?, ?> kafkaConsumer, TopicPartition tp) {
		kafkaConsumer.assign(Collections.singletonList(tp));
		kafkaConsumer.seekToEnd(Collections.singletonList(tp));
		lastOffset = kafkaConsumer.position(tp);
		logger.log(Level.INFO, "Retrieved last offset: " + lastOffset);
	}
	
	private static void updateCommittedOffset(KafkaConsumer<?, ?> kafkaConsumer, TopicPartition tp) {
		// does not require assignment
		OffsetAndMetadata committed = kafkaConsumer.committed(tp);
		logger.log(Level.INFO, "Position of " + tp + ": " + committed);
		committedOffset = (committed != null ? committed.offset() : -1);
	}
	
	private static long getLastOffset() {
		return lastOffset;
	}
	
	private static long getCommittedOffset() {
		return committedOffset;
	}
}
