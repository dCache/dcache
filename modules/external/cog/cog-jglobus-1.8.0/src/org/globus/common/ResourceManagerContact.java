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
package org.globus.common;

import java.util.StringTokenizer;

/**
 * The purpose of this class is to parse resource manager contact strings.
 * It can handle literal IPv6 addresses enclosed in square brackets 
 * ('[' and ']'). 
 */
public class ResourceManagerContact {
  
    public final static String DEFAULT_SERVICE = "/jobmanager";
    public final static int DEFAULT_PORT       = 2119;
    
    protected String hostName    = null;
    protected int portNumber     = -1;
    protected String serviceName = null;
    protected String globusDN   = null;

    /* just for the super classes */
    protected ResourceManagerContact() {
    }

    public ResourceManagerContact(String contact) {
	parse(contact);
    }

    protected void parse(String contact) {

	char c;
	int i;
	
	i = getHostToken(contact);
    
	if (i == -1) {
	    hostName = contact;
	    return;
	}

	hostName = contact.substring(0, i);
	c        = contact.charAt(i);
	
	if (c == '/') {
	    parseService(contact, i);
	} else {
	    
	    int j = getToken(contact, i+1);
	    if (j == -1) {
		portNumber = parsePort(contact.substring(i+1));
		return;
	    }

	    portNumber = parsePort(contact.substring(i+1, j));
	    c          = contact.charAt(j);

	    if (c == ':') {
		globusDN = contact.substring(j+1);
		return;
	    } else if (c == '/') {
		parseService(contact, j);
	    }
	}
    }

    private int parsePort(String port) {
	if (port.length() == 0) {
	    return DEFAULT_PORT;
	} else {
	    return Integer.parseInt(port);
	}
    }

    private void parseService(String contact, int from) {
	int pos = contact.indexOf(":", from);
	if (pos == -1) {
	    serviceName = contact.substring(from);
	} else {
	    serviceName = contact.substring(from, pos);
	    globusDN   = contact.substring(pos+1);
	}
    }

    private int getToken(String contact, int from) {
	int len = contact.length();
	char c;
	int i;
	
	for (i=from;i<len;i++) {
	    c = contact.charAt(i);
	    
	    if (c == '/' || c == ':') {
		return i;
	    }
	}
	
	return -1;
    }

    private int getHostToken(String contact) {
	int len = contact.length();

	if (contact.charAt(0) == '[') {
	    int from = contact.indexOf(']');
	    if (from == -1) {
		throw new IllegalArgumentException(
                          "Missing ']' in IPv6 address: "+ contact
			  );
	    }
	    return (from+1 == len) ? -1 : from+1;
	}

	return getToken(contact, 0);
    }

    public static String convertDN(String globusDN) {
	if (globusDN == null) return null;
	
	StringTokenizer tokens = new StringTokenizer(globusDN, "/");
	StringBuffer buf = new StringBuffer();
	while( tokens.hasMoreTokens() ) {
	    buf.insert(0, tokens.nextToken());
	    if (tokens.hasMoreTokens())
		buf.insert(0, ", ");
	}
	
	return buf.toString();
    }

    public String getHostName() {
	return hostName;
    }
    
    public int getPortNumber() {
	if (portNumber == -1) {
	    return DEFAULT_PORT;
	} else {
	    return portNumber;
	}
    }
    
    public String getServiceName() {
	if (serviceName == null) {
	    return DEFAULT_SERVICE;
	} else {
	    return serviceName;
	}
    }
    
    public String getSubject() {
	return globusDN;
    }
    
    public String getGlobusDN() {
	return globusDN;
    }
    
    public String getDN() {
	return globusDN;
    }
    
    public String toString() {
	StringBuffer buf = new StringBuffer();
	
	buf.append("Host name   : " + hostName + "\n");
	buf.append("Port number : " + getPortNumber() + "\n");
	buf.append("Service     : " + getServiceName() + "\n");
	buf.append("DN          : " + getGlobusDN() + "\n");
	
	return buf.toString();
    }

}
