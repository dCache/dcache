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

public class HTTPProtocol {
    
    public static final String CRLF             = "\r\n";
    public static final String HTTP_VERSION     = "HTTP/1.1";
    public static final String METHOD           = "POST ";
    public static final String HOST             = "Host: ";
    public static final String CONTENT_LENGTH   = "Content-Length: ";
    public static final String CONTENT_TYPE     = "Content-Type: ";
    public static final String USER_AGENT       = "User-Agent: ";
    public static final String SERVER           = "Server: ";
    public static final String CONNECTION       = "Connection: ";
    public static final String LOCATION         = "Location: ";
    public static final String CHUNKED          = "Transfer-Encoding: chunked";
    
    public static final String CONNECTION_CLOSE = "Connection: close\r\n";
    public static final String CHUNKING         = CHUNKED + CRLF;


    /* Used for GRAM and GARA messages */
    protected static String createHTTPHeader(String service,
					     String hostname,
					     String application,
					     StringBuffer msg) {
	
	StringBuffer head = new StringBuffer();
	head.append("POST ").append(service).append(" ").append(HTTP_VERSION).append(CRLF);
	head.append(HOST).append(hostname).append(CRLF);
	head.append(CONTENT_TYPE).append(application).append(CRLF);
	head.append(CONTENT_LENGTH).append(String.valueOf(msg.length())).append(CRLF);
	head.append(CRLF);
	head.append(msg);
	
	return head.toString();
    }

    /* Used for GASS GET */
    public static String createGETHeader(String path,
					 String host,
					 String user_agent) {
	
	StringBuffer head = new StringBuffer();
	head.append("GET " + path + " " + HTTP_VERSION + CRLF);
	head.append(HOST + host + CRLF);
	head.append(CONNECTION_CLOSE);
	head.append(USER_AGENT + user_agent + CRLF);
	head.append(CRLF);
	
	return head.toString();
    }

    /* Used for GASS PUT */
    public static String createPUTHeader(String path, 
					 String host, 
					 String user_agent,
					 String type, 
					 long length, 
					 boolean append) {
	
	StringBuffer head = new StringBuffer();
	
	if (append) {
	    head.append("POST ");
	} else {
	    head.append("PUT ");
	}

	head.append(path + " " + HTTP_VERSION + CRLF);

	head.append(HOST + host + CRLF);
	head.append(CONNECTION_CLOSE);
	head.append(USER_AGENT + user_agent + CRLF);
	head.append(CONTENT_TYPE + type + CRLF);

	if (length == -1) {
	    head.append(CHUNKING);
	} else {
	    head.append(CONTENT_LENGTH + length + CRLF); 
	}
	
	head.append(CRLF);
	
	return head.toString();
    }


    // ------ new HTTP stuff ---------------------------

    public static String ErrorReply(int error, String msg) {
	return getErrorReply(error, msg);
    }

    public static String getErrorReply(int error, String message) {
	StringBuffer head = new StringBuffer();
	
	head.append(HTTP_VERSION)
	    .append(" ")
	    .append(String.valueOf(error))
	    .append(" ")
	    .append(message)
	    .append(CRLF)
	    .append(CONNECTION_CLOSE)
	    .append(CRLF);
	
	return head.toString();
    }
    
    public static String getBadRequestErrorReply() {
	return getErrorReply(400, "BAD REQUEST");
    }

    public static String getFileNotFoundErrorReply() {
	return getErrorReply(404, "FILE NOT FOUND");
    }

    public static String getServerErrorReply() {
	return getErrorReply(500, "INTERAL SERVER ERROR");
    }

    public static String getForbiddenErrorReply() {
	return getErrorReply(403, "FORBIDDEN");
    }
    
    public static String getOKReply(String application) {
	return getOKReply(application, null);
    }

    public static String getOKReply(String application, String msg) {
	StringBuffer head = new StringBuffer();
	
	head.append(HTTP_VERSION)
	    .append(" 200 OK")
	    .append(CRLF)
	    .append(CONTENT_TYPE)
	    .append(application)
	    .append(CRLF)
	    .append(CONTENT_LENGTH);

	if (msg == null) {
	    head.append("0");
	} else {
	    head.append(msg.length());
	}
	
	head.append(CRLF).append(CRLF);
	
	if (msg != null) {
	    head.append(msg);
	}
	
	return head.toString();
    }
}
