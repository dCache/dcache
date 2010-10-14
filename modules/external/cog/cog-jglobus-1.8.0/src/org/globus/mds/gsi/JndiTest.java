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

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;

import org.globus.mds.gsi.common.GSIMechanism;

/**
 * This is an example to demonstrate how to use JNDI to connect to 
 * a secure LDAP server (MDS-2 server) using the GSI SASL mechanism.
 * This is a pretty standard example for JNDI that could work
 * with an arbitraty SASL mechanism.
 * For more information about JNDI and SASL authentication see the
 * <A HREF="http://java.sun.com/products/jndi/tutorial/ldap/security/sasl.html">
 * JNDI Tutorial</A>
 */
public class JndiTest {
  
    public static void main(String[] args) {

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
		System.err.println("Usage: JndiTest -h [host] -p [port] -D [binddn] [-d] -b [baseDN]");
		System.err.println("\tExample: JndiTest -h mds.globus.org -p 389 -r o=globus,c=us");
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

	   
	Hashtable env = new Hashtable();
    
	env.put(Context.INITIAL_CONTEXT_FACTORY, 
		"com.sun.jndi.ldap.LdapCtxFactory");

	env.put("java.naming.ldap.version", String.valueOf(version));

	/* Specify host and port to use for directory service */
	env.put(Context.PROVIDER_URL, "ldap://"+ host + ":" + port);
    
	/* Specify the particular SASL mechanism to use
	 * Use GSIMechanism.NAME for the GSI SASL mechanism.
	 */
	env.put(Context.SECURITY_AUTHENTICATION, GSIMechanism.NAME);

	/* This property specifies where the implementation of
	 * the GSI SASL mechanism for JNDI can be found. 
	 */
	env.put("javax.security.sasl.client.pkgs", 
		"org.globus.mds.gsi.jndi");
	
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
	env.put("javax.security.sasl.qop", qop);
	
	/* This property can be used to pass a specific
	 * set of credentials for the GSI SASL mechanism
	 * to use. It must be a GSSCredential object.
	 * If not set, the defaut credential will be 
	 * used.
	 */
	//env.put(GSIMechanism.SECURITY_CREDENTIALS, cred);
	
	if (binddn != null) {
	    // perform authentication as a user.
	    env.put(Context.SECURITY_PRINCIPAL, binddn);
	}

	// enables ldap packet debugging
	if (debug) {
	    env.put("com.sun.jndi.ldap.trace.ber", System.err);
	}

	
	DirContext ctx = null;
	
	try {
	    ctx = new InitialDirContext(env);

	    NamingEnumeration results = ctx.search(baseDN, filter, null);

	    SearchResult si;
	    Attributes attrs;
	    
	    while (results.hasMoreElements()) {
		si = (SearchResult)results.next(); 
		attrs = si.getAttributes();
		System.out.println(si.getName() + ":");
		System.out.println(attrs);
		System.out.println();
	    }

	} catch(Exception e) {
	    System.err.println("JndiTest failed: " + e.getMessage());
	    e.printStackTrace();
	} finally {
	    if (ctx != null) {
		try { ctx.close(); } catch(Exception e) {}
	    }
	}
    }
    
}
