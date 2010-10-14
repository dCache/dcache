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
package org.globus.gsi.gssapi.net;

import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.globus.common.ChainedIOException;
import org.globus.net.WrappedSocket;
import org.globus.net.SocketFactory;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class GssSocket extends WrappedSocket {

    private static Log logger = 
	LogFactory.getLog(GssSocket.class.getName());

    protected GSSContext context;
    protected boolean clientMode;

    protected InputStream in;
    protected OutputStream out;

    protected Authorization authorization = 
	SelfAuthorization.getInstance();

    public static final int SSL_MODE = 1;
    public static final int GSI_MODE = 2;
    
    protected int mode = -1;
    
    public GssSocket(String host, int port, GSSContext context)
	throws IOException {
	this(SocketFactory.getDefault().createSocket(host, port), context);
    }
    
    public GssSocket(Socket socket, GSSContext context) {
	super(socket);
	this.context = context;
	this.clientMode = true;
    }

    public void setAuthorization(Authorization auth) {
	this.authorization = auth;
    }

    public Authorization getAuthorization() {
	return this.authorization;
    }

    public void setUseClientMode(boolean clientMode) {
	this.clientMode = clientMode;
    }

    public boolean getClientMode() {
	return this.clientMode;
    }

    public void setWrapMode(int mode) {
	this.mode = mode;
    }
    
    public int getWrapMode() {
	return this.mode;
    }

    public GSSContext getContext() {
	return this.context;
    }
    
    abstract protected void writeToken(byte [] token)
	throws IOException;

    abstract protected byte[] readToken()
	throws IOException;
    
    protected synchronized void authenticateClient() 
	throws IOException, GSSException {
	
	byte [] outToken = null;
	byte [] inToken = new byte[0];
	
	while (!this.context.isEstablished()) {

	    outToken = 
		this.context.initSecContext(inToken, 0, inToken.length);
	    
	    if (outToken != null) {
		writeToken(outToken);
	    }
	    
	    if (!this.context.isEstablished()) {
		inToken = readToken();
	    }
	}
    }

    protected synchronized void authenticateServer() 
	throws IOException, GSSException {
	
	byte [] outToken = null;
	byte [] inToken = null;

	while (!this.context.isEstablished()) {
	    inToken = readToken();
	    
	    outToken = 
		this.context.acceptSecContext(inToken, 0, inToken.length);
	    
	    if (outToken != null) {
		writeToken(outToken);
	    }
	}
    }
    
    public synchronized void startHandshake()
	throws IOException {
	if (this.context.isEstablished()) return;
	logger.debug("Handshake start");
	
	try {
	    if (this.clientMode) {
		authenticateClient();
	    } else {
		authenticateServer();
	    }
	} catch (GSSException e) {
	    throw new ChainedIOException("Authentication failed", e);
	}

	logger.debug("Handshake end");
	if (this.authorization != null) {
	    logger.debug("Performing authorization.");
	    this.authorization.authorize(this.context, 
					 getInetAddress().getHostAddress());
	} else {
	    logger.debug("Authorization not set");
	}
    }

    public synchronized OutputStream getOutputStream() 
	throws IOException {
        try {
	startHandshake();
	return this.out;
        } catch (IOException e) {
            try { close(); } catch (IOException ioe) {}
            throw e;
        }
    }
    
    public synchronized InputStream getInputStream() 
	throws IOException {
        try {
	startHandshake();
	return this.in;
        } catch (IOException e) {
            try { close(); } catch (IOException ioe) {}
            throw e;
        }
    }

    /**
     * Disposes of the context and closes the connection
     */
    public void close() 
	throws IOException {
	try {
	    this.context.dispose();
	} catch (GSSException e) {
	    throw new ChainedIOException("dispose failed.", e);
	} finally {
	    this.socket.close();
	}
    }

}
