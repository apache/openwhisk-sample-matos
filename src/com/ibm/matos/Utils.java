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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// Auxiliary class
public class Utils {

	static Logger logger = Logger.getLogger(Utils.class);
	private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";

	private static String userDir = null;
	static String resourceDir = null;
	private static String resourcePathInJar = null;

	static void extractResourcesToFilesystem(boolean isProducer) throws IOException {
		String propfile = (isProducer ? "producer.properties" : "consumer.properties");

		Files.createDirectories(Paths.get(resourceDir));

		for (String path : new String[] { "jaas.conf.template", propfile, "matos.json" }) {
			String fullPath = resourcePathInJar + File.separator + path;
			InputStream is = Utils.class.getClassLoader()
					.getResourceAsStream(fullPath);
			if (is == null) {
				is = new FileInputStream(fullPath);
			}
			Path tPath = Paths.get(resourceDir + File.separator + path);
			logger.log(Level.INFO,
					"[Copying " + resourcePathInJar + File.separator + path + " from JAR to " + tPath + "]");
			Files.copy(is, tPath, StandardCopyOption.REPLACE_EXISTING);
		}
	}

	/**
	 * Retrieve client configuration information, using a properties file, for
	 * connecting to secure Kafka.
	 *
	 * @param broker
	 *            {String} A string representing a list of brokers the producer
	 *            can contact.
	 * @param isProducer
	 *            {Boolean} Flag used to determine whether or not the
	 *            configuration is for a producer.
	 * @return {Properties} A properties object which stores the client
	 *         configuration info.
	 */
	public static final Properties getClientConfiguration(String broker, boolean isProducer) {
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
		props.put("ssl.truststore.location", System.getProperty("java.home") + File.separator + "lib" + File.separator
				+ "security" + File.separator + "cacerts");

		return props;
	}

	static void updateJaasConfiguration(String username, String password) {
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
			String fileContents = templateContents.replace("$USERNAME", username).replace("$PASSWORD", password);

			jaasStream.write(fileContents.getBytes(Charset.forName("UTF-8")));
		} catch (final FileNotFoundException e) {
			logger.log(Level.ERROR, "Could not load JAAS config file at: " + path);
		} catch (final IOException e) {
			logger.log(Level.ERROR, "Writing to JAAS config file:");
			e.printStackTrace();
		} finally {
			if (jaasStream != null) {
				try {
					jaasStream.close();
				} catch (final Exception e) {
					logger.log(Level.ERROR, "Closing JAAS config file:");
					e.printStackTrace();
				}
			}
		}
	}

	public static void initDirs() {
		userDir = System.getProperty("user.dir");
		resourceDir = userDir + File.separator + "resources";
		resourcePathInJar = "resources";

		logger.log(Level.INFO, "Resource directory: " + resourceDir);
	}

	public static void setJaasLocation() {
		// Set JAAS configuration property.
		if (System.getProperty(JAAS_CONFIG_PROPERTY) == null) {
			System.setProperty(JAAS_CONFIG_PROPERTY, resourceDir + File.separator + "jaas.conf");
		}
	}
	

	
}
