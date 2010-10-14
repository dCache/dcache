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

public class GassOutputStream extends HTTPOutputStream {

    /**
     * Opens Gass output stream in secure mode with default
     * user credentials.
     *
     * @param host host name of the gass server.
     * @param port port number of the gass server.
     * @param file name of the file on the remote side.
     * @param length total size of the data to be transfered.
     *               Use -1 if unknown. The data then will be 
     *               transfered in chunks.
     * @param append if true, append data to existing file. 
     *               Otherwise, the file will be overwritten.
     */
    public GassOutputStream(String host,
			    int port,
			    String file,
			    long length,
			    boolean append) 
	throws GassException, GSSException, IOException {
	this(null, SelfAuthorization.getInstance(),
	     host, port, file, length, append);
    }

    /**
     * Opens Gass output stream in secure mode with specified
     * user credentials.
     *
     * @param cred user credentials to use. If null,
     *             default user credentials will be used.
     * @param host host name of the gass server.
     * @param port port number of the gass server.
     * @param file name of the file on the remote side.
     * @param length total size of the data to be transfered. 
     *               Use -1 if unknown. The data then will be
     *               transfered in chunks. 
     * @param append if true, append data to existing file. 
     *               Otherwise, the file will be overwritten.
     */
    public GassOutputStream(GSSCredential cred,
                            String host,
                            int port,
                            String file,
                            long length,
                            boolean append)
        throws GassException, GSSException, IOException {
	this(cred, SelfAuthorization.getInstance(),
             host, port, file, length, append);
    }

    /**
     * Opens Gass output stream in secure mode with specified
     * user credentials.
     *
     * @param cred user credentials to use. If null,
     *             default user credentials will be used.
     * @param host host name of the gass server.
     * @param port port number of the gass server.
     * @param file name of the file on the remote side.
     * @param length total size of the data to be transfered.
     *               Use -1 if unknown. The data then will be 
     *               transfered in chunks.
     * @param append if true, append data to existing file.
     *               Otherwise, the file will be overwritten.
     */
    public GassOutputStream(GSSCredential cred,
			    Authorization auth,
			    String host,
			    int port,
			    String file,
			    long length,
			    boolean append) 
	throws GassException, GSSException, IOException {
	super();

	this.size = length;
	this.append = append;
	
	GSSManager manager = ExtendedGSSManager.getInstance();
	
	ExtendedGSSContext context = 
	    (ExtendedGSSContext)manager.createContext(null, 
						      GSSConstants.MECH_OID,
						      cred,
						      GSSContext.DEFAULT_LIFETIME);
	
	context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_SSL);
	
	GssSocketFactory factory = GssSocketFactory.getDefault();
	
	socket = factory.createSocket(host, port, context);
	
	((GssSocket)socket).setAuthorization(auth);
	
	put(host, file, length, -1);
    }
    
}
