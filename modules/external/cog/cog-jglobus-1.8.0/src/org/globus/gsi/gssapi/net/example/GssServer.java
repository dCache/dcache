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

import org.globus.net.ServerSocketFactory;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.GssSocket;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSCredential;

import org.gridforum.jgss.ExtendedGSSContext;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.ServerSocket;

public class GssServer {

    private static final String helpMsg = 
	"Where options are:\n" +
	" -gss-mode mode\t\t\tmode is: 'ssl' or 'gsi' (default)\n" +
	" -deleg-type type\t\ttype is: 'none', 'limited' (default), or 'full'\n" +
	" -lifetime time\t\t\tLifetime of context. time is in seconds.\n" +
	" -rejectLimitedProxy\t\tEnables checking for limited proxies. By default off\n" +
	" -anonymous\t\t\tDo not require client authentication\n" +
	" -enable-conf\t\t\tEnables confidentiality (do encryption) (enabled by default)\n" +
	" -disable-conf\t\t\tDisables confidentiality (no encryption)\n" +
	" -wrap-mode mode\t\tmode is: 'ssl' (default) or 'gsi'";

    public static void main(String [] args) {

	String usage = "Usage: java GssServer [options] [port]";

	GetOpts opts = new GetOpts(usage, helpMsg);

	int pos = opts.parse(args);
	int port = 0;

	if (pos < args.length) {
	    port = Integer.parseInt(args[pos]);
	}

        ServerSocketFactory factory = ServerSocketFactory.getDefault();
	try {
	    ServerSocket server = factory.createServerSocket(port);
	    System.out.println("Server running at: " + server.getLocalPort());
	    while(true) {
		Client c = new Client(server.accept(), opts);
		c.start();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	} 
    }

}

class Client extends Thread {

    GetOpts opts;
    Socket s;

    private static GSSCredential cred;
    
    public Client(Socket s, GetOpts opts) {
	this.s = s;
	this.opts = opts;
    }

    private static GSSCredential getCredential(GSSManager manager) 
        throws Exception {
        // return null if needed to automatically reload the default creds
        if (cred == null) {
            cred = manager.createCredential(GSSCredential.INITIATE_AND_ACCEPT);
        }
        return cred;
    }

    public void run() {

	System.out.println("client connected");

	// to make sure we use right impl
	GSSManager manager = new GlobusGSSManagerImpl();
	ExtendedGSSContext context = null;
	
	try {
            GSSCredential credd = getCredential(manager);
	    context = (ExtendedGSSContext)manager.createContext(credd);

	    context.requestConf(opts.conf);
	    
	    context.setOption(GSSConstants.GSS_MODE,
			      (opts.gsiMode) ? 
			      GSIConstants.MODE_GSI : 
			      GSIConstants.MODE_SSL);
	    
	    context.setOption(GSSConstants.REJECT_LIMITED_PROXY,
			      new Boolean(opts.rejectLimitedProxy));
	    
	    context.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
			      new Boolean(!opts.anonymity));
	    
	    s = GssSocketFactory.getDefault().createSocket(s, null, 0, context);
	    
	    // server socket
	    ((GssSocket)s).setUseClientMode(false);
	    ((GssSocket)s).setWrapMode(opts.wrapMode);
	    
	    OutputStream out = s.getOutputStream();
	    InputStream in = s.getInputStream();
	    
	    System.out.println("Context established.");
	    System.out.println("Initiator : " + context.getSrcName());
	    System.out.println("Acceptor  : " + context.getTargName());	    
	    System.out.println("Lifetime  : " + context.getLifetime());
	    System.out.println("Privacy   : " + context.getConfState());
	    
	    GlobusGSSCredentialImpl cred = 
		(GlobusGSSCredentialImpl)context.getDelegCred();
	    System.out.println("Delegated credential :");
	    if (cred != null) {
		System.out.println(cred.getGlobusCredential());
	    } else {
		System.out.println("None");
	    }
	    
	    String line = null;
	    BufferedReader r = new BufferedReader(new InputStreamReader(in));
	    while ( (line = r.readLine()) != null ) {
		if (line.length() == 0) {
		    break;
		}
		System.out.println(line);
	    }
	    
	    byte[] msg = 
		"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n".getBytes();
	    
	    out.write(msg);
	    out.flush();
	    
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    try { s.close(); } catch(Exception e) {}
	    System.out.println("client disconnected");
	}
    }
}
