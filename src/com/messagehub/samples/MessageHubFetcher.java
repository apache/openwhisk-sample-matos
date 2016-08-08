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
package com.messagehub.samples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.messagehub.samples.env.CreateTopicParameters;

public class MessageHubFetcher {

    private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
    private static Logger logger = Logger.getLogger(MessageHubFetcher.class);
    private static String userDir, resourceDir, resourcePathInJar;

    public static long lastOffset = -1;

    public static void main(String args[]) throws InterruptedException,
            ExecutionException, IOException {

        String topic = "mytopic";
        String kafkaHost = null;
        String restHost = null;
        String apiKey = null;
        Thread consumerThread, producerThread;
        RESTRequest restApi = null;

        userDir = System.getenv("user.dir");
        
        //logger.log(Level.INFO, "Running in local mode.");
        resourceDir = userDir + File.separator + "resources";
        resourcePathInJar = "resources";

        extractResourcesToFilesystem();

        // Set JAAS configuration property.
        if(System.getProperty(JAAS_CONFIG_PROPERTY) == null) {
            System.setProperty(JAAS_CONFIG_PROPERTY, resourceDir + File.separator + "jaas.conf");
        }

        logger.log(Level.INFO, "Starting Message Hub Fetcher");

        if(args.length == 3) {
            // Arguments parsed from the command line.
            kafkaHost = args[0];
            restHost = args[1];
            apiKey = args[2];
            updateJaasConfiguration(apiKey.substring(0, 16), apiKey.substring(16));
        } else {
                logger.log(Level.ERROR, "Usage:\n\n" +
                        "java -jar <name_of_jar>.jar <kafka_endpoint> <rest_endpoint> <api_key>");
                return;
        }

        logger.log(Level.INFO, "Resource directory: " + resourceDir);
        logger.log(Level.INFO, "Kafka Endpoint: " + kafkaHost);
        logger.log(Level.INFO, "Rest API Endpoint: " + restHost);

        restApi = new RESTRequest(restHost, apiKey);

        // Create a topic, ignore a 422 response - this means that the
        // topic name already exists.
        restApi.post("/admin/topics",
                new CreateTopicParameters(topic, 1).toString(),
                new int[] { 422 });

        String topics = restApi.get("/admin/topics", false);

        logger.log(Level.INFO, "Topics: " + topics);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(kafkaHost, apiKey, topic);
        producerThread = new Thread(consumerRunnable);

        ProducerRunnable producerRunnable = new ProducerRunnable(kafkaHost, apiKey, topic);
        consumerThread = new Thread(producerRunnable);

        consumerThread.start();
        producerThread.start();
        
        logger.log(Level.INFO, "Waiting for 10 seconds");
        Thread.sleep(10000);
        logger.log(Level.INFO, "Done waiting, interrupting consumer/producer");
        	
        if (producerThread != null) {
            producerRunnable.shutdown();
        }
        if (consumerThread != null) {
            consumerRunnable.shutdown();
        }
        
        System.out.println("Last offset: " + lastOffset);
    }

    private static void extractResourcesToFilesystem() throws IOException {
        Files.createDirectories(Paths.get(resourceDir));
        for (String path: new String[] {
        		"jaas.conf.template", 
        		"consumer.properties", 
        		"producer.properties"}) {
            InputStream is = MessageHubFetcher.class.getClassLoader().getResourceAsStream(resourcePathInJar + File.separator + path);
            Path tPath = Paths.get(resourceDir + File.separator + path);
            logger.log(Level.INFO, "[Copying " + resourcePathInJar + File.separator + path + " from JAR to " + tPath + "]");
            Files.copy(is, tPath, StandardCopyOption.REPLACE_EXISTING);
        }
	}

	/**
     * Create a message consumer, returning the thread object it will run on.
     *
     * @param broker
     *            {String} The host and port of the broker to interact with.
     * @param apiKey
     *            {String} The API key used to connect to the Message Hub
     *            service.
     * @param topic
     *            {String} The topic to consume on.
     * @return {Thread} Thread object that the consumer will run on.
     */
    public static Thread createMessageConsumer(String broker, String apiKey,
            String topic) {
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(broker,
                apiKey, topic);
        return new Thread(consumerRunnable);
    }

    /**
     * Create a message producer, returning the thread object it will run on.
     *
     * @param broker
     *            {String} The host and port of the broker to interact with.
     * @param apiKey
     *            {String} The API key used to connect to the Message Hub
     *            service.
     * @param topic
     *            {String} The topic to consume on.
     * @param message
     *            {String} String representation of a JSON array of messages to
     *            send.
     * @return {Thread} Thread object that the producer will run on.
     */
    public static Thread createMessageProducer(String broker, String apiKey,
            String topic) {
        ProducerRunnable producerRunnable = new ProducerRunnable(broker,
                apiKey, topic);
        return new Thread(producerRunnable);
    }

    /**
     * Retrieve client configuration information, using a properties file, for
     * connecting to secure Kafka.
     *
     * @param broker
     *            {String} A string representing a list of brokers the producer
     *            can contact.
     * @param apiKey
     *            {String} The API key of the Bluemix Message Hub service.
     * @param isProducer
     *            {Boolean} Flag used to determine whether or not the
     *            configuration is for a producer.
     * @return {Properties} A properties object which stores the client
     *         configuration info.
     */
    public static final Properties getClientConfiguration(String broker,
            String apiKey, boolean isProducer) {
        Properties props = new Properties();
        InputStream propsStream;
        String fileName;

        if (isProducer) {
            fileName = "producer.properties";
        } else {
            fileName = "consumer.properties";
        }

        try {
            propsStream = new FileInputStream(resourceDir + File.separator + fileName);
            props.load(propsStream);
            propsStream.close();
        } catch (IOException e) {
            logger.log(Level.ERROR, "Could not load properties from file");
            return props;
        }

        props.put("bootstrap.servers", broker);

        // update truststore location property to the Java certificates folder 
        props.put("ssl.truststore.location", 
            System.getProperty("java.home") + File.separator + "lib" 
            + File.separator + "security" + File.separator + "cacerts");

        return props;
    }

    private static void updateJaasConfiguration(String username, String password) {
        String templatePath = resourceDir + File.separator + "jaas.conf.template";
        String path = resourceDir + File.separator + "jaas.conf";
        OutputStream jaasStream = null;

        logger.log(Level.INFO, "Updating JAAS configuration");

        try {
            Path tPath = Paths.get(templatePath);
            String templateContents = new String(Files.readAllBytes(tPath));
            jaasStream = new FileOutputStream(path, false);

            // Replace username and password in template and write
            // to jaas.conf in resources directory.
            String fileContents = templateContents
                .replace("$USERNAME", username)
                .replace("$PASSWORD", password);

            jaasStream.write(fileContents.getBytes(Charset.forName("UTF-8")));
        } catch (final FileNotFoundException e) {
            logger.log(Level.ERROR, "Could not load JAAS config file at: " + path);
        } catch (final IOException e) {
            logger.log(Level.ERROR, "Writing to JAAS config file:");
            e.printStackTrace();
        } finally {
            if(jaasStream != null) {
                try {
                    jaasStream.close();
                } catch(final Exception e) {
                    logger.log(Level.ERROR, "Closing JAAS config file:");
                    e.printStackTrace();
                }
            }
        }
    }
}
