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
package org.globus.mds.gsi;

import netscape.ldap.LDAPConnection;
import netscape.ldap.LDAPv2;
import netscape.ldap.LDAPEntry;
import netscape.ldap.LDAPSearchResults;
import netscape.ldap.LDAPAttribute;
import netscape.ldap.LDAPAttributeSet;

import java.util.Hashtable;
import java.util.Enumeration;

// this is not required
import org.globus.mds.gsi.common.GSIMechanism;

/**
 * This is an example to demonstrate how to use Netscape Directory SDK
 * to connect to a secure LDAP server (MDS-2 server) using the GSI SASL
 * mechanism. This is a pretty standard example for Netscape Directory
 * SDK that could work with an arbitraty SASL mechanism.
 * For more information about Netscape Directory SDK and SASL 
 * authentication see the 
 * <A HREF="http://docs.iplanet.com/docs/manuals/dirsdk/jsdk40/sasl.htm#1915315">
 * Netscape Programming Guide</A>
 */
public class NetscapeTest {

    public static void main(String [] args) {

	String host    = null;
	String binddn  = null;
	String baseDN  = "mds-vo-name=local, o=grid";
	String filter  = "(objectclass=*)";
	String qop     = "auth-conf, auth";
	boolean debug  = false;
	int port       = 389;
	int version    = 3;

	for (int i = 0; i < args.length; i++) {
	    if (args[i].equals("-h")) {
		host = args[++i];
	    } else if (args[i].equals("-p")) {
		port = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-ver")) {
                version = Integer.parseInt(args[++i]);
	    } else if (args[i].equals("-d")) {
		debug = true;
	    } else if (args[i].equals("-D")) {
		binddn = args[++i];
	    } else if (args[i].equals("-b")) {
		baseDN = args[++i];
	    } else if (args[i].equals("-qop")) {
		qop = args[++i];
	    } else if (args[i].equalsIgnoreCase("-usage") ||
		       args[i].equalsIgnoreCase("-help")) {
		System.err.println("Usage: NetscapeTest -h [host] -p [port] -D [binddn] [-d] -b [baseDN]");
		System.err.println("\tExample: NetscapeTest -h mds.globus.org -p 389 -r o=globus,c=us");
		System.exit(1);
	    } else {
		System.err.println("Invalid argument: " + args[i]);
		System.exit(1);
	    }
	}
	
	if (host == null) {
	    System.err.println("Error: hostname not specified!");
	    System.exit(1);
	}
	
	LDAPConnection ld = null;
	ld = new LDAPConnection();
	
	Hashtable props = new Hashtable();

	/* This property specifies where the implementation of
         * the GSI SASL mechanism for Netscape Directory SDK
	 * can be found. 
         */
	props.put ( "javax.security.sasl.client.pkgs", 
		    "org.globus.mds.gsi.netscape" );
	
        /* This property specifies the quality of protection
         * value. It can be a comma separated list of protection
         * values in preference order. There are three possible
         * qop values: 
         *  "auth"      - authentication only,
         *  "auth-int"  - authentication with integrity protection
         *                (GSI without encryption)
         *  "auth-conf" - authentication with integrity and privacy
         *                protections. (GSI with encryption)
         * If not specified, defaults to "auth"
         */
	props.put("javax.security.sasl.qop", qop);

	/* This property can be used to pass a specific
	 * set of credentials for the GSI SASL mechanism
	 * to use. It must be a GSSCredential object.
	 * If not set, the defaut credential will be 
	 * used.
	 */
	//env.put(GSIMechanism.SECURITY_CREDENTIALS, cred);
    
	try {
	    if (debug) {
		// to enable debugging
		ld.setProperty( "debug", "true");
		ld.setProperty( LDAPConnection.TRACE_PROPERTY, System.out);
	    }
      
	    ld.setOption( LDAPv2.PROTOCOL_VERSION, new Integer(version) );

	    ld.connect( host, port );
      
	    /* Authenticate to the server over SASL.
	     * Use GSIMechanism.NAME for the GSI SASL mechanism.
	     */
	    ld.authenticate( binddn, new String [] {GSIMechanism.NAME}, props, null );

	    LDAPSearchResults myResults = null;
	    myResults = ld.search( baseDN, LDAPv2.SCOPE_ONE, filter, null, false );
 
	    while ( myResults.hasMoreElements() ) {
		LDAPEntry myEntry = myResults.next();
		String nextDN = myEntry.getDN();
		System.out.println( nextDN + ":");
		LDAPAttributeSet entryAttrs = myEntry.getAttributeSet();
		System.out.println(entryAttrs);
		System.out.println();
	    }

	} catch(Exception e) {
	    System.err.println("NetscapeTest failed: " + e.getMessage());
	    e.printStackTrace();
	} finally {
	    try {
		ld.disconnect();
	    } catch(Exception ee) {}
	}
	
    }
}
