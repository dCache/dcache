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
package org.globus.ftp;

/**
 * Utility class for parsing and converting host-port information from EPSV 
 * and EPRT ftp commands.
 */
public class HostPort6 extends HostPort {

    public static final String IPv4 = "1";
    public static final String IPv6 = "2";
    
    private String host;
    private int port;
    private String version;
    
    public HostPort6(String version, String host, int port) {
        this.version = version;
        this.host = host;
        this.port = port;
    }

    /**
     * Parses host-port from passive mode reply message.
     * Note that the argument is not the whole message, but
     * only the content of the brackets:
     * <d><net-prt><d><net-addr><d><tcp-port><d>
     *
     * @param passiveReplyMessage reply message for the EPSV command
     */
    public HostPort6(String passiveReplyMessage) {
        Parser tokens = new Parser(passiveReplyMessage);
        String token = null;

        token = tokens.nextToken().trim();
        if (token.length() == 0) {
            // do nothing assume the same as control channel
        } else if (token.equals(IPv4)) {
            this.version = IPv4;
        } else if (token.equals(IPv6)) {
            this.version = IPv6;
        } else {
            throw new IllegalArgumentException("Invalid network protocol: " + 
                                               token);
        }

        token = tokens.nextToken().trim();
        if (token.length() == 0) {
            // do nothing assume the same as control channel
        } else {
            this.host = token;
        }
        
        token = tokens.nextToken().trim();
        if (token.length() == 0) {
            throw new IllegalArgumentException("Port number is required");
        }
        this.port = Integer.parseInt(token);
    }
 
    private static class Parser {

        String line;
        int offset = 0;

        public Parser(String line) {
            this.line = line;
        }

        public String nextToken() {
            int start = line.indexOf('|', this.offset);
            if (start == -1) {
                throw new IllegalArgumentException("Formatting error");
            }
            int end = line.indexOf('|', start+1);
            if (end == -1) {
                throw new IllegalArgumentException("Formatting error");
            }
            this.offset = end;
            return line.substring(start+1, end);
        }
    }

    /**
     * Returns the port number
     *
     * @return port number
     */
    public int getPort() {
	return this.port;
    }
  
    /**
     * Sets the host address
     *
     * @param host the host address 
     */
    public void setHost(String host) {
	this.host = host;
    }

    /**
     * Returns the host address
     *
     * @return host address 
     */
    public String getHost() {
	return this.host;
    }

    /**
     * Returns the address version
     *
     * @return address version
     */
    public String getVersion() {
	return this.version;
    }

    /**
     * Sets the address version
     *
     * @param version the address version
     */
    public void setVersion(String version) {
	this.version = version;
    }
  
    /**
     * Returns the host-port information in the
     * format used by EPRT command. 
     * <d><net-prt><d><net-addr><d><tcp-port><d>
     *
     * @return host-port information in EPRT command
     *         representation.
     */
    public String toFtpCmdArgument() {
	StringBuffer msg = new StringBuffer();
        msg.append("|");
        if (this.version != null) {
            msg.append(this.version);
        }
        msg.append("|");
        if (this.host != null) {
            msg.append(this.host);
        }
        msg.append("|");
        msg.append(String.valueOf(this.port));
        msg.append("|");
	return msg.toString();
    }

    public static String getIPAddressVersion(String address) {
        return (address.indexOf(':') == -1) ? IPv4 : IPv6;
    }
    
}
