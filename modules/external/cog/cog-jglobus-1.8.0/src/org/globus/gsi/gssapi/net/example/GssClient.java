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
package org.globus.gsi.gssapi.net.example;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.auth.IdentityAuthorization;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSCredential;

import org.gridforum.jgss.ExtendedGSSContext;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

public class GssClient {

    private static final String helpMsg = 
	"Where options are:\n" +
	" -gss-mode mode\t\t\tmode is: 'ssl' or 'gsi' (default)\n" +
	" -deleg-type type\t\ttype is: 'none', 'limited' (default), or 'full'\n" +
	" -lifetime time\t\t\tLifetime of context. time is in seconds.\n" +
	" -rejectLimitedProxy\t\tEnables checking for limited proxies (off by default)\n" +
	" -anonymous\t\t\tDo not send certificates to the server\n" +
	" -enable-conf\t\t\tEnables confidentiality (do encryption) (enabled by default)\n" +
	" -disable-conf\t\t\tDisables confidentiality (no encryption)\n" +
	" -auth auth\t\t\tIf auth is 'host' host authorization will be performed.\n" +
	"           \t\t\tIf auth is 'self' self authorization will be performed.\n" +
	"           \t\t\tOtherwise, identity authorization is performed.\n" +
	"           \t\t\tAuthorization is not performed by default.\n" +
	" -wrap-mode mode\t\tmode is: 'ssl' (default) or 'gsi'";
    

    private Authorization auth;
    private GSSName targetName;
    private static GSSCredential cred;

    private static GSSCredential getCredential(GSSManager manager) 
        throws Exception {
        // return null if needed to automatically reload the default creds
        if (cred == null) {
            cred = manager.createCredential(GSSCredential.INITIATE_AND_ACCEPT);
        }
        return cred;
    }

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
	
        GssClient client = new GssClient();

	Authorization auth = SelfAuthorization.getInstance();

	    if (opts.auth != null) {
		if (opts.auth.equals("host")) {
                client.auth = HostAuthorization.getInstance();
		} else if (opts.auth.equals("self")) {
                client.auth = SelfAuthorization.getInstance();
		} else {
                client.auth = new IdentityAuthorization(opts.auth);
		}
	    }

            // XXX: When doing delegation targetName cannot be null.
            // additional authorization will be performed after the handshake 
            // in the socket code.
            if (opts.deleg) {
                if (auth instanceof GSSAuthorization) {
                    GSSAuthorization gssAuth = (GSSAuthorization)auth;
                try {
                    client.targetName = 
                        gssAuth.getExpectedName(null, host);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
                }
        
        client.connect(host, port, opts);
            }

    public void connect(String host, int port, GetOpts opts) {
        // to make sure we use right impl
	GSSManager manager = new GlobusGSSManagerImpl();
        
	ExtendedGSSContext context = null;
	Socket s = null;
        
        try {
	    context = (ExtendedGSSContext)manager.createContext(
                                             this.targetName,
								GSSConstants.MECH_OID,
                                             getCredential(manager),
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

	    s = GssSocketFactory.getDefault().createSocket(host, port, context);
	    ((GssSocket)s).setWrapMode(opts.wrapMode);
	    ((GssSocket)s).setAuthorization(this.auth);

	    OutputStream out = s.getOutputStream();
	    InputStream in = s.getInputStream();

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

	    out.write(tmp);
	    out.flush();

	    String line = null;
	    BufferedReader r = new BufferedReader(new InputStreamReader(in));
	    while ( (line = r.readLine()) != null ) {
		System.out.println(line);
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    if (s != null) {
		try { s.close(); } catch(Exception e) {}
	    }
	}
    }
}
