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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Config {

    private static Logger logger = Logger.getLogger(Config.class);

    // static Kafka properties
    final static String KAFKA_BROKER_PROP = "kafkaBroker";
    final static String KAFKA_REST_PROP = "kafkaRest";
    final static String KAFKA_API_KEY_PROP = "kafkaApiKey";
    final static String KAFKA_TOPIC_PROP = "kafkaTopic";
    final static String KAFKA_PARTITION_PROP = "kafkaPartition";
    final static String KAFKA_CONSUMER_ID_PROP = "kafkaConsumerId";
    // variable Kafka properties
    final static String KAFKA_START_OFFSET_PROP = "kafkaStartOffset";
    final static String KAFKA_END_OFFSET_PROP = "kafkaEndOffset";
    final static String KAFKA_NUM_RECORDS_PROP = "kafkaNumRecords";
    // static Swift properties
    final static String SWIFT_AUTH_URL_PROP = "swiftAuthUrl";
    final static String SWIFT_REGION_PROP = "swiftRegion";
    final static String SWIFT_TENANT_ID_PROP = "swiftTenantId";
    final static String SWIFT_USER_ID_PROP = "swiftUserId";
    final static String SWIFT_PASSWORD_PROP = "swiftPassword";
    final static String SWIFT_CONTAINER_PROP = "swiftContainer";
    final static String[] ALL_PROPS = {
            KAFKA_BROKER_PROP,
            KAFKA_REST_PROP,
            KAFKA_API_KEY_PROP,
            KAFKA_TOPIC_PROP,
            KAFKA_PARTITION_PROP,
            KAFKA_CONSUMER_ID_PROP,
            KAFKA_START_OFFSET_PROP,
            KAFKA_END_OFFSET_PROP,
            KAFKA_NUM_RECORDS_PROP,
            SWIFT_AUTH_URL_PROP,
            SWIFT_REGION_PROP,
            SWIFT_TENANT_ID_PROP,
            SWIFT_USER_ID_PROP,
            SWIFT_PASSWORD_PROP,
            SWIFT_CONTAINER_PROP
    };

    private final static String PROP_FILE_NAME = "matos.json";
    private JsonObject matos;

    public Config() throws IOException {
        this(PROP_FILE_NAME);
    }

    public Config(String filename) throws IOException {
        matos = loadProperties(filename);
    }

    private JsonObject loadProperties(String filename) throws IOException {
        JsonObject matos = null;
        InputStream is = new FileInputStream(Utils.resourceDir + File.separator + filename);
        matos = new JsonParser().parse(new InputStreamReader(is)).getAsJsonObject();
        logger.log(Level.INFO, "Matos Properties:" + matos);
        return matos;
    }


    public void overrideProperties(JsonObject props) {
        // override config properties with those passed in params
        for (String prop : Config.ALL_PROPS) {
            if (props.has(prop))
                matos.addProperty(prop, props.getAsJsonPrimitive(prop).getAsString());
        }
    }

    public void overrideProperties(Map<String,String> props) {
        // override config properties with those passed in params
        for (String prop : Config.ALL_PROPS) {
            if (props.containsKey(prop))
                matos.addProperty(prop, props.get(prop));
        }
    }

    public String get(String key) {
        return matos.getAsJsonPrimitive(key).getAsString();
    }

    public String toString() {
        return matos.toString();
    }

}
