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
package org.globus.gram.internal;

/**
 * This protocol is documented at:
 * http://www.globus.org/internal/gram-1.1-protocol.html
 */

import org.globus.util.Util;
import org.globus.util.http.HTTPProtocol;

public class GRAMProtocol extends HTTPProtocol {

    public static final int GRAM_PROTOCOL_VERSION = 2;

    public static final String APPLICATION = "application/x-globus-gram";
    
    public static final String PROTOCOL_VERSION = "protocol-version: ";
    public static final String JOB_STATE_MASK   = "job-state-mask: ";
    public static final String CALLBACK_URL     = "callback-url: ";
    public static final String RSL              = "rsl: ";

    public static final String STATUS     = "status";
    public static final String CANCEL     = "cancel";
    public static final String REGISTER   = "register";
    public static final String UNREGISTER = "unregister";
    public static final String SIGNAL     = "signal";
    
    public static final String PROTOCOL_VERSION_LINE = PROTOCOL_VERSION + GRAM_PROTOCOL_VERSION + CRLF;
    
    public static String REQUEST(String servicename,
				 String hostname,
				 int state_mask,
				 String callback_url,
				 String rsl_string) {
	
	StringBuffer msg = new StringBuffer();
	msg.append(PROTOCOL_VERSION_LINE);
	msg.append(JOB_STATE_MASK).append(String.valueOf(state_mask)).append(CRLF);
	msg.append(CALLBACK_URL).append(callback_url).append(CRLF);
	msg.append(RSL).append(Util.quote(rsl_string)).append(CRLF).append(CRLF);
	
	return createHTTPHeader(servicename,
				hostname,
				APPLICATION,
				msg);	
    }

    public static String PING(String servicename,
			      String hostname) {
	return createHTTPHeader("ping" + servicename,
                                hostname,
                                APPLICATION,
				new StringBuffer(0));
    }
    
    public static String STATUS_POLL(String jobmanager_url,
				     String hostname) {
	return JMREQUEST(jobmanager_url,
			 hostname,
			 STATUS);
    }
    
    public static String SIGNAL(String jobmanager_url,
				String hostname,
				int signal,
				String arg) {
	return JMREQUEST(jobmanager_url,
			 hostname,
			 SIGNAL + " " + signal + " " + arg);
    }

    public static String REGISTER_CALLBACK(String jobmanager_url,
					   String hostname,
					   int state_mask,
					   String callback_url) {
	return JMREQUEST(jobmanager_url,
			 hostname,
			 REGISTER + " " + state_mask + " " + callback_url);
    }
    
    public static String UNREGISTER_CALLBACK(String jobmanager_url,
					     String hostname,
					     String callback_url) {
	return JMREQUEST(jobmanager_url,
			 hostname,
			 UNREGISTER + " " + callback_url);
    }
    
    public static String CANCEL_JOB(String jobmanager_url,
				    String hostname) {
	return JMREQUEST(jobmanager_url,
			 hostname,
			 CANCEL);
    }    
    
    private static final String JMREQUEST(String jobmanager_url,
					  String hostname, 
					  String request) {
        StringBuffer msg = new StringBuffer();
	msg.append(PROTOCOL_VERSION_LINE);
	msg.append(Util.quote(request));
	msg.append(CRLF).append(CRLF);
	
        return createHTTPHeader(jobmanager_url,
                                hostname,
				APPLICATION,
                                msg);
    }

    public static String OKReply() {
	return getOKReply(APPLICATION);
    }
    
}
