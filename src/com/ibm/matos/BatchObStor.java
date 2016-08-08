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

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.auth.PasswordScopeAccessProvider;

public class BatchObStor {
	private Container container;

	public BatchObStor (Config config) {
		String auth_url = config.get(Config.SWIFT_AUTH_URL_PROP);
		String userid = config.get(Config.SWIFT_USER_ID_PROP);
		String passwd = config.get(Config.SWIFT_PASSWORD_PROP);
		String tenantid = config.get(Config.SWIFT_TENANT_ID_PROP);
		String region = config.get(Config.SWIFT_REGION_PROP);
		String folder = config.get(Config.SWIFT_CONTAINER_PROP);
	
		AccountConfig ac = new AccountConfig();
		ac.setAuthUrl(auth_url);
		ac.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);
		PasswordScopeAccessProvider psap = new PasswordScopeAccessProvider(userid, passwd, tenantid, auth_url, region);
		ac.setAccessProvider(psap);
		
		JossAccount acct = new JossAccount(ac, region, false);
		acct.createAccount();
		acct.authenticate();
	
	    container = acct.getAccount().getContainer(folder);
	    if (!container.exists()) {
	        container.create();
	      }
	}
	
	public void uploadFile(String filename, byte[] bytes) {
	    StoredObject object = container.getObject(filename);
	    object.uploadObject(bytes);
		
	}

}
