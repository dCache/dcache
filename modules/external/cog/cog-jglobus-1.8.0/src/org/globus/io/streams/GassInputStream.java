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
package org.globus.io.streams;

import java.io.IOException;
import java.net.Socket;

import org.globus.common.ChainedIOException;
import org.globus.io.gass.client.GassException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;

public class GassInputStream extends HTTPInputStream {

    private GSSCredential cred;
    private Authorization auth;

    /**
     * Opens Gass input stream in secure mode with default
     * user credentials.
     *
     * @param host host name of the gass server
     * @param port port number of the gass server
     * @param file file to retrieve from the server
     */
    public GassInputStream(String host,
			   int port,
			   String file) 
	throws GassException, GSSException, IOException {
	this(null, SelfAuthorization.getInstance(),
	     host, port, file);
    }
    
    /**
     * Opens Gass input stream in secure mode with specified
     * user credentials.
     *
     * @param cred user credentials to use
     * @param host host name of the gass server
     * @param port port number of the gass server
     * @param file file to retrieve from the server
     */
    public GassInputStream(GSSCredential cred,
			   Authorization auth,
			   String host,
			   int port,
			   String file) 
	throws GassException, GSSException, IOException {
	super();
	this.cred = cred;
	this.auth = auth;
	get(host, port, file);
    }

    protected Socket openSocket(String host, int port) 
	throws IOException {
	
	GSSManager manager = ExtendedGSSManager.getInstance();

	ExtendedGSSContext context = null;
	try { 
	    context = 
		(ExtendedGSSContext)manager.createContext(
                                       null,
				       GSSConstants.MECH_OID,
				       this.cred,
				       GSSContext.DEFAULT_LIFETIME
            );
	
	    context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_SSL);
	} catch (GSSException e) {
	    throw new ChainedIOException("Security error", e);
	}
	
	GssSocketFactory factory = GssSocketFactory.getDefault();
	
	socket = factory.createSocket(host, port, context);

	((GssSocket)socket).setAuthorization(this.auth);
	
	return socket;
    }
    
}
