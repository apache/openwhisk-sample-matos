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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

public class Batch {

	private static Logger logger = Logger.getLogger(Batch.class);
	private static KafkaConsumer<byte[], byte[]> kafkaConsumer;
	// private Collection<TopicPartition> myPartitions = null;
	private static TopicPartition tp;
	private static boolean done;
	private static final String KAFKA_CONSUMER_ID_KEY = "group.id";
	private static final long FETCH_TIMEOUT_SEC = 60;
	private static long startOffset, endOffset;
	private static BatchAppender processor;
	private static Config config;

	// main method for Whisk action
	public static JsonObject main(JsonObject args) {

		try {
			Utils.initDirs();
			Utils.extractResourcesToFilesystem(false);

			config = new Config();
			config.overrideProperties(args);

			// invoke the "real" main method, shared with Java main
			doMain();
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		JsonObject response = new JsonObject();
		response.addProperty("offsets", getOffsets());
		return response;
	}

	// Java main class, reading arguments from command line and invoking doMain() 
	public static void main(String args[]) throws InterruptedException, ExecutionException, IOException {

		Utils.initDirs();
		Utils.extractResourcesToFilesystem(false);

		if (args.length >= 5 && args.length <=8) {
			config = new Config(args[0]);
			HashMap<String,String> amap = new HashMap<String,String>();
			amap.put(Config.KAFKA_API_KEY_PROP, args[1]);
			amap.put(Config.SWIFT_TENANT_ID_PROP, args[2]);
			amap.put(Config.SWIFT_USER_ID_PROP, args[3]);
			amap.put(Config.SWIFT_PASSWORD_PROP, args[4]);
			if (args.length >= 6)
				amap.put(Config.KAFKA_PARTITION_PROP, args[5]);
			if (args.length >= 7)
				amap.put(Config.KAFKA_START_OFFSET_PROP, args[6]);
			if (args.length >= 8)
				amap.put(Config.KAFKA_END_OFFSET_PROP, args[7]);
			config.overrideProperties(amap);
		} else {
			logger.log(Level.ERROR, "Usage:\n\n" +
					"java -jar <name_of_jar>.jar <config-json-file-name> " +
					"<kafka_api_key> <swift_tenant_id> <swift_user_id> <swift_password> " +
					"[<kafka_partition> [<kafka_start_offset> [<kafka_end_offset>]]]");
			return;
		}
		// invoke the "real" main method, shared with Whisk's main
		doMain();
	}
	
	private static void doMain() throws IOException, InterruptedException {
		String consumerGroup;
		logger.log(Level.INFO, "Starting " + Batch.class.getSimpleName() + "; CONFIG:");
		logger.log(Level.INFO, config);

		Utils.setJaasLocation();

		String apiKey = config.get(Config.KAFKA_API_KEY_PROP);
		Utils.updateJaasConfiguration(apiKey.substring(0, 16), apiKey.substring(16));

		consumerGroup = config.get(Config.KAFKA_CONSUMER_ID_PROP);
		startOffset = Integer.parseInt(config.get(Config.KAFKA_START_OFFSET_PROP));
		endOffset = Integer.parseInt(config.get(Config.KAFKA_END_OFFSET_PROP));
		
		String broker = config.get(Config.KAFKA_BROKER_PROP);
		String topic = config.get(Config.KAFKA_TOPIC_PROP);
		int partition = Integer.parseInt(config.get(Config.KAFKA_PARTITION_PROP));

		Properties props = Utils.getClientConfiguration(broker, false);
		props.put(KAFKA_CONSUMER_ID_KEY, consumerGroup);

		// initialize Kafka consumer
		kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props, new ByteArrayDeserializer(),
				new ByteArrayDeserializer());

		tp = new TopicPartition(topic, partition);
		
		logger.log(Level.INFO, "Assigning topic-partition: " + tp);
		kafkaConsumer.assign(Collections.singletonList(tp));
		
		if (startOffset >= 0) {
			logger.log(Level.INFO, "Rewinding " + tp + " to " + startOffset);
			kafkaConsumer.seek(tp, startOffset);
		} else {
			logger.log(Level.INFO, "Starting with current offset");
		}
		
		processor = new BatchAppender();
		done = false;
		int received = 0;
		long start_time_sec = System.currentTimeMillis() / 1000;
		
		kafkaConsumer.poll(0);	// TODO: not needed?
		
		startOffset = kafkaConsumer.position(tp);
		if (endOffset < 0) {	// get last offset
			// rewind to end, get offset, then rewind back
			kafkaConsumer.seekToEnd(Collections.singletonList(tp));
			endOffset = kafkaConsumer.position(tp);	// returns the 'next after last' offset
			kafkaConsumer.seek(tp, startOffset);
		}
		logger.log(Level.INFO, "Offsets to read: [" + startOffset + "," + endOffset +"]");
		while (!done) {
			// Poll on the Kafka consumer every second.
			ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(100);
			logger.log(Level.INFO, "Retrieved " + records.count() + " records");
			if (records.count() > 0) {
				// we might need less than we have in the buffer
				// considering also records received in previous iterations 
				int last = Math.min(records.count(), 
						(int) (endOffset - startOffset - received));
				processor.processRecords(records.records(tp).subList(0, last), tp);
			}
			received += records.count();
			if (startOffset + received >= endOffset) {
				logger.log(Level.INFO, "Setting offset of " + tp + "(group=" + consumerGroup + ") to " + endOffset);
				kafkaConsumer.seek(tp, endOffset);
				kafkaConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(endOffset)));
				done = true;
			} else {
				if (System.currentTimeMillis() / 1000 - start_time_sec > FETCH_TIMEOUT_SEC) {
					String err = "TIMEOUT fetching from " + tp + ": expected " + (endOffset - startOffset)
							+ " messages with offsets [" + startOffset + ".." + endOffset + "], "
							+ " and received only " + received + " messages";
					logger.log(Level.ERROR, err);
					shutdown();
				}
			}
		}
		
		kafkaConsumer.close();

		// Store the retrieved messages into Object Storage
		if (processor.getLast() > processor.getFirst()) {
			BatchObStor obstor = new BatchObStor(config);
			final String obstor_path = 
				"matos/" + System.currentTimeMillis() + "_" 
					+ processor.getFirst() + "-" + processor.getLast() + ".txt";
			obstor.uploadFile(obstor_path, processor.getBytes());
		}
		
		System.out.println("Offsets: " + getOffsets());
	}
	
	private static String getOffsets() {
		return "[" + processor.getFirst() + ".." + processor.getLast() + "]";
	}

	private static void shutdown() {
		done = true;
	}
}
