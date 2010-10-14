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

import org.globus.ftp.GridFTPClient;
import org.globus.ftp.Session;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.exception.FTPException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;

import org.ietf.jgss.GSSCredential;

public class GridFTPOutputStream extends FTPOutputStream {
  
    public GridFTPOutputStream(GSSCredential cred, 
			       String host,
			       int port,
			       String file,
			       boolean append)
	throws IOException, FTPException {
	this(cred, HostAuthorization.getInstance(),
	     host, port, file, append,
	     true, Session.TYPE_IMAGE, true);
    }

    public GridFTPOutputStream(GSSCredential cred, 
			       Authorization auth,
			       String host,
			       int port,
			       String file,
			       boolean append,
			       boolean reqDCAU)
	throws IOException, FTPException {
	this(cred, auth, 
	     host, port, file, append,
	     true, Session.TYPE_IMAGE, reqDCAU);
    }
    
    public GridFTPOutputStream(GSSCredential cred, 
                   Authorization auth,
                   String host,
                   int port,
                   String file,
                   boolean append,
                   boolean reqDCAU, 
                   long size)
    throws IOException, FTPException {
    this(cred, auth, 
         host, port, file, append,
         true, Session.TYPE_IMAGE, reqDCAU, size);
    }
    
    public GridFTPOutputStream(GSSCredential cred, 
                   Authorization auth,
                   String host,
                   int port,
                   String file,
                   boolean append,
                   boolean passive,
                   int type,
                   boolean reqDCAU) 
    throws IOException, FTPException {
        this(cred, auth, 
             host, port, file, append, 
             passive, type, reqDCAU, -1);
    }

    public GridFTPOutputStream(GSSCredential cred, 
			       Authorization auth,
			       String host,
			       int port,
			       String file,
			       boolean append,
			       boolean passive,
			       int type,
			       boolean reqDCAU,
			       long size)
	throws IOException, FTPException {
	GridFTPClient gridFtp = new GridFTPClient(host, port);
	gridFtp.setAuthorization(auth);
	gridFtp.authenticate(cred);
	
	if (gridFtp.isFeatureSupported("DCAU")) {
	    if (!reqDCAU) {
		gridFtp.setDataChannelAuthentication(DataChannelAuthentication.NONE);
	    }
	} else {
            gridFtp.setLocalNoDataChannelAuthentication();
	}
	
	ftp = gridFtp;
	
	put(passive, type, file, append);
    }
}
