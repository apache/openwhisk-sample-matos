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

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BatchAppender implements RecordsProcessor {
	private static final Logger logger = Logger.getLogger(RecordsProcessor.class);
	private long first, last;
	private StringBuilder buffer;

	public BatchAppender() {
		first = -1;
		last = -1;
	}
	
	@Override
	public void processRecords(List<ConsumerRecord<byte[], byte[]>> records, TopicPartition tp) {
		if (buffer == null) {
			buffer = new StringBuilder();
		}
		if (first == -1) { // update 'first' only the first time
			first = records.get(0).offset();	
		}
		last = records.get(0).offset() + records.size();
		
		logger.log(Level.INFO, "Processing " + records.size() + " records from " + tp + " starting from offset "
				+ records.get(0).offset() + ":");
		for (ConsumerRecord<byte[], byte[]> record : records) {
			// processing goes here
			String msg = "[" + record.offset() + "] " + new String(record.value());
			buffer.append(msg + System.getProperty("line.separator"));
//			logger.log(Level.DEBUG, "   " + msg);
		}
	}
	
	public byte[] getBytes() {
		return buffer.toString().getBytes();		
	}
	
	public long getFirst() {
		return first;
	}

	public long getLast() {
		return last;
	}

}
