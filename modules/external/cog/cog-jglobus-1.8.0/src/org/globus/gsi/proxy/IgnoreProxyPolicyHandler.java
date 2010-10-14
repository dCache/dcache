/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi.proxy;

import java.security.cert.X509Certificate;

import org.globus.gsi.proxy.ext.ProxyCertInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple restricted proxy policy handler that logs the
 * proxy policy language oid. It can be used for debugging purposes.
 */
public class IgnoreProxyPolicyHandler implements ProxyPolicyHandler {

    private static Log logger = 
	LogFactory.getLog(IgnoreProxyPolicyHandler.class.getName());

    public void validate(ProxyCertInfo proxyCertInfo,
			 X509Certificate[] certPath,
			 int index)
	throws ProxyPathValidatorException {
	logger.info("ProxyPolicy ignored: " + proxyCertInfo.getProxyPolicy().getPolicyLanguage().getId());
    }
    
}
