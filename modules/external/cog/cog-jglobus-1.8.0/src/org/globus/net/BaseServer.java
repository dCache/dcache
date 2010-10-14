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
package org.globus.net;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.MalformedURLException;
import java.net.InetAddress;
import java.io.IOException;

import org.globus.util.deactivator.DeactivationHandler;
import org.globus.util.deactivator.Deactivator;
import org.globus.util.Util;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides the basics for writing various servers.
 * <b>Note:</b> Sockets created by this server have a 5 minute default timeout.
 * The timeout can be changed using the {@link #setTimeout(int) setTimeout()}
 * function.
 */
public abstract class BaseServer implements Runnable {

    private static Log logger =
	LogFactory.getLog(BaseServer.class.getName());
    
    /** Socket timeout in milliseconds. */ 
    public static final int SO_TIMEOUT = 5*60*1000;
    
    protected boolean accept;
    protected ServerSocket _server = null;
    private boolean secure = true;
    protected String url = null;
    private Thread serverThread = null;
    
    protected GSSCredential credentials = null;
    protected Authorization authorization = null;
    protected Integer gssMode = GSIConstants.MODE_SSL;

    protected int timeout = SO_TIMEOUT;

    public BaseServer() 
	throws IOException {
	this(null, 0);
    }
  
    public BaseServer(int port) 
	throws IOException {
	this(null, port);
    }
  
    public BaseServer(GSSCredential cred, int port) 
	throws IOException {
	this.credentials = cred;
	this._server = ServerSocketFactory.getDefault().createServerSocket(port);
	this.secure = true;
	initialize();
    }
    
    public BaseServer(boolean secure, int port) 
	throws IOException {
	this.credentials = null;
	this._server = ServerSocketFactory.getDefault().createServerSocket(port);
	this.secure = secure;
	initialize();
    }

    /**
     * This method should be called by all subclasses.
     *
     */
    protected void initialize() {
	setAuthorization(SelfAuthorization.getInstance());
	start();
    }
    
    /**
     * Starts the server.
     */
    protected void start() {
	if (serverThread == null) {
	    accept       = true;
	    serverThread = new Thread(this);
	    serverThread.start();
	}
    }
    
    /**
     * Sets timeout for the created sockets.
     * By default if not set, 5 minute timeout is used.
     */
    public void setTimeout(int timeout) {
	this.timeout = timeout;
    }
    
    public int getTimeout() {
	return this.timeout;
    }

    /**
     * Stops the server but does
     * not stop all the client threads
     */
    public void shutdown() {
	accept = false;
	try {
	    _server.close();
	} catch(Exception e) {}
	// this is a hack to ensue the server socket is 
	// unblocked from accpet()
	// but this is not guaranteed to work still
        SocketFactory factory = SocketFactory.getDefault();
	Socket s = null;
	try {
	    s = factory.createSocket(InetAddress.getLocalHost(), getPort());
	    s.getInputStream();
	} catch (Exception e) {
	    // can be ignored
	} finally {
	    if (s != null) {
		try { s.close(); } catch (Exception e) {}
	    }
	}

	// reset everything
        serverThread = null;
        _server = null;
    }
    
    public GSSCredential getCredentials() {
	return this.credentials;
    }
    
    public String getProtocol() {
	return (secure) ? "https" : "http";
    }

    /**
     * Returns url of this server
     *
     * @return url of this server
     */
    public String getURL() {
	if (url == null) {
	    StringBuffer buf = new StringBuffer();
	    buf.append(getProtocol()).
		append("://").
		append(getHost()).
		append(":").
		append(String.valueOf(getPort()));
	    url = buf.toString();
	}
	return url;
    }
  
    /**
     * Returns port of this server
     *
     * @return port number
     */
    public int getPort() {
	return _server.getLocalPort();
    }
    
    /**
     * Returns hostname of this server
     *
     * @return hostname
     */
    public String getHostname() {
	return Util.getLocalHostAddress();
    }

    /**
     * Returns hostname of this server. The format of the host conforms 
     * to RFC 2732, i.e. for a literal IPv6 address, this method will 
     * return the IPv6 address enclosed in square brackets ('[' and ']'). 
     *
     * @return hostname
     */
    public String getHost() {
	String host = Util.getLocalHostAddress();
	try {
	    URL u = new URL("http", host, 80, "/");
	    return u.getHost();
	} catch (MalformedURLException e) {
	    return host;
	}
    }
    
    public void run() {
	Socket socket = null ;

	while(accept) {
	    
	    try {
		socket = _server.accept();
		if (!accept) {
		    break;
		}
		socket.setSoTimeout(getTimeout());
	    } catch(IOException e) {
		if (accept) { // display error message
		    logger.error("Server died: " + e.getMessage(), e);
		} 
		break;
	    }

	    if (this.secure) {
		try {
		    socket = wrapSocket(socket);
		} catch (GSSException e) {
		    logger.error("Failed to secure the socket", e);
		    break;
		}
	    }
	    
	    handleConnection(socket);
	}
	
	logger.debug("server thread stopped");
    }

    protected Socket wrapSocket(Socket socket) 
	throws GSSException {
	
	GSSManager manager = ExtendedGSSManager.getInstance();

	ExtendedGSSContext context = 
	    (ExtendedGSSContext)manager.createContext(credentials);

	context.setOption(GSSConstants.GSS_MODE, gssMode);

	GssSocketFactory factory = GssSocketFactory.getDefault();

	GssSocket gsiSocket = (GssSocket)factory.createSocket(socket, null, 0, context);
	    
	// server socket
	gsiSocket.setUseClientMode(false);
	gsiSocket.setAuthorization(this.authorization);
	
	return gsiSocket;
    }

    public void setGssMode(Integer mode) {
	this.gssMode = mode;
    }

    public void setAuthorization(Authorization auth) {
        authorization = auth;
    }

    /**
     * This method needs to be implemented by subclasses.
     * Optimmaly, it should be a non-blocking call starting
     * a separate thread to handle the client.  Note that to
     * start an SSL handshake, you need to call socket.getInput(Output)
     * stream().
     */
    protected abstract void handleConnection(Socket socket) ;

    /**
     * Registers a default deactivation handler. It is used
     * to shutdown the server without having a reference to
     * the server. Call Deactivate.deactivateAll() to shutdown
     * all registered servers.
     */
    public void registerDefaultDeactivator() {
	if (deactivator == null) {
	    deactivator = new AbstractServerDeactivator(this);
	}
	Deactivator.registerDeactivation(deactivator);
    }
    
    /**
     * Unregisters a default deactivation handler. 
     */
    public void unregisterDefaultDeactivator() {
	if (deactivator == null) return;
	Deactivator.unregisterDeactivation(deactivator);
    }
    
    /**
     * A handler for the deactivation framework.
     */
    protected AbstractServerDeactivator deactivator = null;
    
}

class AbstractServerDeactivator implements DeactivationHandler {
    
    private BaseServer server = null;
    
    public AbstractServerDeactivator(BaseServer server) {
        this.server = server;
    }
    
    public void deactivate() {
        if (server != null) server.shutdown();
    }
    
}
