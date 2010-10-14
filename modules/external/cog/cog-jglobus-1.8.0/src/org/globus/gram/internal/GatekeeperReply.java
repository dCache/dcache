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

import org.globus.util.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GatekeeperReply extends HttpResponse {

    private static Log logger =
	LogFactory.getLog(GatekeeperReply.class.getName());

    public int protocolVersion   = -1;
    public int status            = -1;
    public String jobManagerUrl  = null;
    public int failureCode       = -1;
    public int jobFailureCode    = -1;

    public GatekeeperReply(InputStream in) throws IOException {
	super(in);    
	charsRead = 1;
	if (contentLength > 0) myparse();
    }

    protected void myparse() throws IOException {
	
	String line, tmp;
	
        while(charsRead < contentLength) {
            line = readLine(input);
	    if (line.length() == 0) break;

	    if (logger.isTraceEnabled()) {
		logger.trace(line);
	    }

            tmp = getRest(line.trim());   
	    
	    if (line.startsWith("protocol-version:")) {
	      protocolVersion = Integer.parseInt(tmp);
	    } else if (line.startsWith("status:")) {
		status = Integer.parseInt(tmp);
	    } else if (line.startsWith("job-manager-url:")) {
		jobManagerUrl = tmp;
	    } else if (line.startsWith("failure-code:")) {
		failureCode = Integer.parseInt(tmp);
	    } else if (line.startsWith("job-failure-code:")) {
		jobFailureCode = Integer.parseInt(tmp);
	    }
	}
    }

    public String toString() {
	StringBuffer buf = new StringBuffer();

	buf.append(super.toString());
	
	buf.append("Protocol-version : " + protocolVersion + "\n");
	buf.append("Status           : " + status);
	if (jobManagerUrl != null) {
	    buf.append("\nJob-manager-url  : " + jobManagerUrl);
	}
	if (failureCode >= 0) {
	    buf.append("\nFailure-code     : " + failureCode);
	}
	if (jobFailureCode >= 0) {
            buf.append("\nJob failure code     : " + jobFailureCode);
        }
	
	return buf.toString();
    }
}
	



