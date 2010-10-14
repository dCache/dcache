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
package org.globus.gsi.gssapi.example;

import org.globus.net.SocketFactory;
import org.globus.gsi.gssapi.SSLUtil;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GlobusGSSManagerImpl;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSCredential;

import org.gridforum.jgss.ExtendedGSSContext;

import java.io.OutputStream;
import java.io.InputStream;
import java.net.Socket;

public class GssClient {

    private static final String helpMsg = 
	"Where options are:\n" +
	" -gss-mode mode\t\t\tmode is: 'ssl' or 'gsi' (default)\n" +
	" -deleg-type type\t\ttype is: 'none', 'limited' (default), or 'full'\n" +
	" -lifetime time\t\t\tLifetime of context. time is in seconds.\n" +
	" -rejectLimitedProxy\t\tEnables checking for limited proxies (off by default)\n" +
	" -anonymous\t\t\tDo not send certificates to the server\n " +
	" -enable-conf\t\t\tEnables confidentiality (do encryption) (enabled by default)\n" +
	" -disable-conf\t\t\tDisables confidentiality (no encryption)\n" +
	" -auth auth\t\t\tIf auth is 'host' host authorization will be performed.\n" +
	"           \t\t\tIf auth is 'self' self authorization will be performed.\n" +
	"           \t\t\tOtherwise, identity authorization is performed.\n" +
	"           \t\t\tAuthorization is not performed by default.";

    public static void main(String [] args) {

	String usage = "Usage: java GssClient [options] host port";

	GetOpts opts = new GetOpts(usage, helpMsg);

	int pos = opts.parse(args);
	
	if (pos + 2 > args.length) {
	    System.err.println(usage);
	    return;
	}

	String host = args[pos];
	int port = Integer.parseInt(args[pos+1]);
	
	// to make sure we use right impl
	GSSManager manager = new GlobusGSSManagerImpl();

	ExtendedGSSContext context = null;
	Socket s = null;

	try {
	    s = SocketFactory.getDefault().createSocket(host, port);

	    OutputStream out = s.getOutputStream();
	    InputStream in = s.getInputStream();
	    
	    byte [] inToken = new byte[0];
	    byte [] outToken = null;

	    GSSName targetName = null;
	    if (opts.auth != null) {
		if (opts.auth.equals("host")) {
		    targetName = manager.createName("host@" + host, GSSName.NT_HOSTBASED_SERVICE);
		} else if (opts.auth.equals("self")) {
		    targetName = manager.createCredential(GSSCredential.INITIATE_ONLY).getName();
		} else {
		    targetName = manager.createName(opts.auth, null);
		}
	    }

	    context = (ExtendedGSSContext)manager.createContext(targetName,
								GSSConstants.MECH_OID,
								null, 
								opts.lifetime);
	    
	    context.requestCredDeleg(opts.deleg);
	    context.requestConf(opts.conf);
	    context.requestAnonymity(opts.anonymity);

	    context.setOption(GSSConstants.GSS_MODE,
			      (opts.gsiMode) ? 
			      GSIConstants.MODE_GSI : 
			      GSIConstants.MODE_SSL);

	    if (opts.deleg) {
		context.setOption(GSSConstants.DELEGATION_TYPE,
				  (opts.limitedDeleg) ? 
				  GSIConstants.DELEGATION_TYPE_LIMITED :
				  GSIConstants.DELEGATION_TYPE_FULL);
	    }

	    context.setOption(GSSConstants.REJECT_LIMITED_PROXY,
			      new Boolean(opts.rejectLimitedProxy));

	    // Loop while there still is a token to be processed
	    while (!context.isEstablished()) {
		outToken 
		    = context.initSecContext(inToken, 0, inToken.length);

		if (outToken != null) {
		    out.write(outToken);
		    out.flush();
		}
		
		if (!context.isEstablished()) {
		    inToken = SSLUtil.readSslMessage(in);
		}
	    }

	    System.out.println("Context established.");
	    System.out.println("Initiator : " + context.getSrcName());
	    System.out.println("Acceptor  : " + context.getTargName());
	    System.out.println("Lifetime  : " + context.getLifetime());
	    System.out.println("Privacy   : " + context.getConfState());
	    System.out.println("Anonymity : " + context.getAnonymityState());

	    String msg = 
		"POST ping/jobmanager HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Content-Type: application/x-globus-gram\r\n" +
		"Content-Length: 0\r\n\r\n";

	    byte [] tmp = msg.getBytes();

	    outToken = context.wrap(tmp, 0, tmp.length, null);

	    out.write(outToken);
	    out.flush();
	    
	    inToken = SSLUtil.readSslMessage(in);

	    outToken = context.unwrap(inToken, 0, inToken.length, null);

	    System.out.println(new String(outToken));
    
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    if (s != null) {
		try { s.close(); } catch(Exception e) {}
	    }
	    if (context != null) {
		try {
		    System.out.println("closing...");
		    context.dispose();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}
    }
}
