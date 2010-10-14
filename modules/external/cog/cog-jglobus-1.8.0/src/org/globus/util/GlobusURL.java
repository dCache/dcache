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
package org.globus.util;

import java.net.MalformedURLException;
import java.net.URL;

/** 
 * This class represents the URLs needed by various Globus services,
 * including:
 * <ul>
 * <li> GASS </li>
 * <li> GRAM </li>
 * <li> FTP </li>
 * <li> GSIFTP </li>
 * </ul>
 *
 * This class is not extended from URL since it is not intended to do stream
 * handling.  Instead it is primarily for the parsing of Globus URLs and is
 * able to handle the extraction of user names and passwords from the URL
 * as well. It also can parse literal IPv6 addresses (as per RFC 2732).
 */
public class GlobusURL {
    
    protected String protocol = null;
    protected String host = null;    
    protected String urlPath = null;
    protected String user = null;
    protected String pwd  = null;
    protected String url  = null;
    protected int port = -1;

    /**
     * Parses the url and extracts the url parts.
     *
     * @param url the url to parse.
     * @throws MalformedURLException if the url
     *         is malformed.
     */
    public GlobusURL(String url) throws MalformedURLException {
	
	int p1, p2;

        url = url.trim();
	p1 = url.indexOf("://");
	if (p1 == -1) {
	    throw new MalformedURLException("Missing '[protocol]://'");
	}
    
	protocol = url.substring(0, p1).toLowerCase();
	p1 += 3;

	String base = null;

	p2 = url.indexOf('/', p1);

	if (p2 == -1) {
	    /*throw new MalformedURLException("Missing '/' at [host]:[port]/");*/
      
	    base = url.substring(p1);
	    urlPath = null;
	    
	} else {
	    
	    base = url.substring(p1, p2);
	    
	    // this is after /
	    p2++;
	    if (p2 != url.length()) {
		urlPath = url.substring(p2);
	    } else {
		urlPath = null;
	    }
	}
	
	// this is [user]:[pwd]@[host]:[port]
	p1 = base.indexOf('@');
    
	if (p1 == -1) {
	    parseHostPort(base);
	} else {
	    parseUserPwd( base.substring(0, p1) );
	    parseHostPort( base.substring(p1+1) );
	}
	
	if (port == -1) {
	    port = getPort(protocol);
	}
    
	if (protocol.equals("ftp") && user == null && pwd == null) {
	    user = "anonymous";
	    pwd  = "anon@anon.com";
	}
	
	this.url = url;
    }

    /**
     * Creates a GlobusURL instance from URL instance.
     * <BR><B>Note: </B><I>Not all the url parts are 
     * copied.</I>
     */
    public GlobusURL(URL url) {
	// TODO: does not handle the password:user spec
	protocol = url.getProtocol();
	host = url.getHost();
	port = url.getPort();
	urlPath = url.getFile();
    }

    public static int getPort(String protocol) {
	if (protocol.equals("ftp")) {
	    return 21;
	} else if (protocol.equals("gsiftp") ||
		   protocol.equals("gridftp")) {
	    return 2811;
	} else if (protocol.equals("http")) {
	    return 80;
	} else if (protocol.equals("https")) {
	    return 443;
	} else if (protocol.equals("ldap")) {
	    return 389;
	} else if (protocol.equals("ldaps")) {
	    return 636;
	} else {
	    return -1;
	}
    }

    private void parseHostPort(String str) throws MalformedURLException {
	int start = 0;

	if (str.length() > 0 && str.charAt(0) == '[') {
	    start = str.indexOf(']');
	    if (start == -1) {
		throw new MalformedURLException(
                    "Missing ']' in IPv6 address: " + str
                );
	    }
	}

	int p1 = str.indexOf(':', start);
	
	if (p1 == -1) {
	    host = str;
	} else {
	    host = str.substring(0, p1);
	    String pp = str.substring(p1+1);
	    try {
		port = Integer.parseInt(pp);
	    } catch(NumberFormatException e) {
		throw new MalformedURLException("Invalid port number: " + pp);
	    }
	}
    }
    
    private void parseUserPwd(String str) {
	int p1;
	
	p1 = str.indexOf(':');
	
	if (p1 == -1) {
	    user = Util.decode(str);
	} else {
	    user = Util.decode(str.substring(0, p1));
	    pwd  = Util.decode(str.substring(p1+1));
	}
    }
    
    /**
     * Returns the string representation of 
     * an url.
     *
     * @return the url as string.
     */
    public String getURL() {
	return url;
    }
    
    /**
     * Returns the protocol of an url.
     *
     * @return the protocol part of the url.
     */
    public String getProtocol() {
	return protocol;
    }
    
    /**
     * Returns the host name of an url.
     *
     * @return the host name part of the url.
     */
    public String getHost() {
	return host;
    }
    
    /**
     * Returns the port number of an url.
     *
     * @return the port name of the url. -1 if
     *         the port was not specified.
     */
    public int getPort() {
	return port;
    }

    /**
     * Returns the url path part of an url.
     *
     * @return the url path part of the url.
     *         Returns null if the url path is 
     *         not specified.
     */
    public String getPath() {
	return urlPath;
    }
    
    /**
     * Returns the url path part of an url.
     *
     * @return the url path part of the url.
     *         Returns null if the url path is
     *         not specified.
     * @deprecated
     */
    public String getFile() {
	return getUrlPath();
    }
    
    /**
     * Returns the url path part of an url.
     *
     * @return the url path part of the url.
     *         Returns null if the url path is 
     *         not specified.
     * @deprecated Though this class does not extend java.net.URL,
     *		   it should be similar where possible to be intuitive
     *		   to the user used to URL.
     */
    public String getUrlPath() {
	return urlPath;
    }
    
    /**
     * Returns the user name of an url.
     *
     * @return the user name if present in the url,
     *         otherwise returns null.
     */
    public String getUser() {
	return user;
    }
  
    /**
     * Returns the password of an url.
     *
     * @return the password if present in the url,
     *         otherwise returns null.
     */
    public String getPwd() {
	return pwd;
    }

    /**
     * Compares two urls. 
     * 
     * @param obj could be a string representation of an url
     *        or an instance of this class. 
     * @return true if the urls are the same, false otherwise.
     */
    public boolean equals(Object obj) {
	GlobusURL cUrl = null;
	
	if (obj instanceof String) {
	    try {
		cUrl = new GlobusURL((String)obj);
	    } catch(MalformedURLException e) {
		return false;
	    }
	} else if (obj instanceof GlobusURL) {
	    cUrl = (GlobusURL)obj;
	} else {
	    return false;
	}
	
	// do the comparison

	// compare ports
	if (getPort() != cUrl.getPort()) return false;
	if (!compare(getProtocol(), cUrl.getProtocol(), false)) return false;
	if (!compare(getHost(), cUrl.getHost(), false)) return false;
	if (!compare(getUrlPath(), cUrl.getUrlPath(), false)) return false;
	if (!compare(getUser(), cUrl.getUser(), false)) return false;
	if (!compare(getPwd(), cUrl.getPwd(), false)) return false;
	
	return true;
    }

    private boolean compare(String s1, String s2, boolean ignoreCase) {
	if (s1 == null) {
	    return (s2 == null);
	} else if (s2 == null) {
	    return false;
	} else {
	    return (ignoreCase) ? s1.equalsIgnoreCase(s2) : s1.equals(s2);
	}
    }

    public int hashCode() {
        int value = this.port;
        if (this.protocol != null) {
            value += this.protocol.hashCode();
        }
        if (this.host != null) {
            value += this.host.hashCode();
        }
        if (this.urlPath != null) {
            value += this.urlPath.hashCode();
        }
        if (this.user != null) {
            value += this.user.hashCode();
        }
        if (this.pwd != null) {
            value += this.pwd.hashCode();
        }
        return value;
    }

    public String toString() {
	StringBuffer info = new StringBuffer();
	info.append("Protocol    : " + protocol + "\n");
	info.append("Host name   : " + host + "\n");
	info.append("Port number : " + port + "\n");
	info.append("Url path    : " + urlPath + "\n");
	info.append("User        : " + user + "\n");
	info.append("Pwd         : " + pwd + "\n");
	return info.toString();
    }
    
}
