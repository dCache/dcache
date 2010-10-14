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
package org.globus.util.http;

import java.io.InputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HttpResponse {

    private static Log logger =
	LogFactory.getLog(HttpResponse.class.getName());

    protected InputStream input;
    protected long charsRead  = 0;

    public String httpType    = null;
    public String httpMsg     = null;
    public int httpCode       = -1;
    public long contentLength  = -1;
    public String connection  = null;
    public String contentType = null;
    public String server      = null;
    public boolean chunked    = false;
    public String location    = null;

    public HttpResponse(InputStream in) throws IOException {
	input = in;
	parse();
    }

    /**
     * Read a line of text from the given Stream and return it
     * as a String.  Assumes lines end in CRLF.
     */
    protected String readLine(InputStream in) throws IOException {
	StringBuffer buf = new StringBuffer();
	int c, length = 0;
	
	while(true) {
	    c = in.read();
	    
	    if (c == -1 || c == '\n' || length > 512) {
		charsRead++;
		break;
	    } else if (c == '\r') { 
		in.read(); 
		charsRead+=2;
		break;
	    } else {	    
		buf.append((char)c);
		length++;	
	    }
	}
	
	charsRead += length;
	return buf.toString();
    } 
    
    public static String getRest(String line) {
	int pos = line.indexOf(":");
	if (pos == -1) {
	    return null;
	} else
	    return line.substring(pos+1).trim();
    }
    
    public void parseHttp(String line) {
	
	int p1 = line.indexOf(" ");
	if (p1 == -1) {
	    return;
	}
	httpType = line.substring(0,p1);
	
	int p2 = line.indexOf(" ",p1+1);
	
	String tmp;
	
	if (p2 == -1) {
	    tmp = line.substring(p2);
	} else {
	    tmp = line.substring(p1,p2);
	    httpMsg = line.substring(p2).trim();
	}
	httpCode = Integer.parseInt(tmp.trim());
    }

    private void parse() throws IOException {
	String line, tmp;
	
	line = readLine(input);
	if (logger.isTraceEnabled()) {
	    logger.trace(line);
	}
	parseHttp(line);
	
	while ( (line=readLine(input)).length() != 0 ) {
	    if (logger.isTraceEnabled()) {
		logger.trace(line);
	    }
	    tmp = getRest(line);
	    
	    if (line.startsWith(HTTPProtocol.CONNECTION)) {
		connection = tmp;
	    } else if (line.startsWith(HTTPProtocol.SERVER)) {
		server = tmp;
	    } else if (line.startsWith(HTTPProtocol.CONTENT_TYPE)) {
		contentType = tmp;
	    } else if (line.startsWith(HTTPProtocol.CONTENT_LENGTH)) {
		contentLength = Long.parseLong(tmp.trim());
	    } else if (line.startsWith(HTTPProtocol.CHUNKED)) {
		chunked = true;
	    } else if (line.startsWith(HTTPProtocol.LOCATION)) {
		location = tmp;
	    }
	}
    }

    /** Generates a string representation of the http header
     * 
     * @return <code>String</code> a string representation of the http header
     */
    public String toString() {
	StringBuffer buf = new StringBuffer();
	
	buf.append("Http    : " + httpType + "\n");
	buf.append("Message : " + httpMsg + "\n");
	buf.append("Code    : " + httpCode + "\n");

	if (server != null) {
	    buf.append("Server  : " + server + "\n");
	}

	buf.append("Length  : " + contentLength + "\n");
	buf.append("Chunked : " + chunked + "\n");
	buf.append("Type    : " + contentType + "\n");
	
	if (connection != null) {
	    buf.append("Connection   : " + connection + "\n");
	}

	if (location != null) {
	    buf.append("Location     : " + location + "\n");
	}
	
	return buf.toString();
    }
    
}
