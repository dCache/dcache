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
package org.globus.gatekeeper;

import java.util.Vector;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;

import org.globus.security.gridmap.GridMap;
import org.globus.net.BaseServer;
import org.globus.util.QuotedStringTokenizer;
import org.globus.gsi.GSIConstants;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.FileAppender;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

/**
 * Globus GateKeeper.
 */
public class GateKeeperServer extends BaseServer {

    protected static final int PORT = 2119;

    private static final String LOG_PATTERN = "%-5p: %m%n";

    private Vector listeners;
    private Map _services;
    private GridMap _gridMap;

    private Logger _logger;

    /**
     * Initializes the GateKeeperServer with default credentials
     * and starts to listen to client connections
     * @exception IOException is thrown when the port cannot be opened
     */
    public GateKeeperServer() 
	throws IOException {
        super();
	init();
    }

    /**
     * initializes the GateKeeperServer with default credentials
     * and starts to listen to client connections on the port passed as a
     * parameter
     * @param port the port number used by this server
     * @exception IOException is thrown when the port cannot be opened
     */
    public GateKeeperServer(int port) 
	throws IOException {
        super(port);
	init();
    }

    /**
     * initializes the GateKeeperServer with the provided credentials
     * and starts to listen to client connections on the port passed as a
     * parameter
     * @param cred the credentials used by this server, if null then default
     * credentials are used.
     * @param port the port number used by this server
     */
    public GateKeeperServer(GSSCredential cred, int port)
        throws IOException {
        super(cred, port);
	init();
    }

    private void init() {
	_services = new HashMap();
	_logger = Logger.getLogger( getClass().getName() + "." + hashCode() );
	setAuthorization(null);
	setGssMode(GSIConstants.MODE_GSI);
    }

    public void setLogFile(String file) {
	FileAppender ap = new FileAppender();
        ap.setFile(file);
	
        ap.setName("Gatekeeper Log");
        ap.setLayout(new PatternLayout(LOG_PATTERN));
        ap.activateOptions();
	
	_logger.addAppender(ap);
    }
    
    public void setGridMap(GridMap gridMap) {
	_gridMap = gridMap;
    }
    
    public GridMap getGridMap() {
	return _gridMap;
    }
    
    /**
     * Handles individual client connections by starting a different thread.
     * @param socket is connected to a client ready to send request to the
     * gatekeeper.
     */
    protected void handleConnection(Socket socket) {
	if (_logger.isInfoEnabled()) {
	    _logger.info("Client connected: " + socket.getInetAddress() + 
			 ":" + socket.getPort());
	}

	// FIXME: might need to keep track of all clients.

	informListener(socket);
	GateKeeperClient c = new GateKeeperClient(this, socket);
	c.setLogger(_logger);
	c.start();
    }

    /**
     * Add a new listener for this Gatekeeper.
     * 
     * @param   listener        
     */
    public void addListener(GateKeeperListener listener) {
        if (listeners == null) listeners = new Vector();
        listeners.addElement(listener);
    }
    
    /**
     * Remove a listener from this Gatekeeper.
     * 
     * @param   listener        
     */
    public void removeListener(GateKeeperListener listener) {
        if (listeners == null) return;
        listeners.removeElement(listener);
    }

    /**
     * Inform all registered listeners about new connections.
     * 
     * @param   aSocket 
     */
    private void informListener(Socket aSocket) {
        if (listeners == null) return;
        int size = listeners.size();
        for (int i=0; i<size; i++) {
            GateKeeperListener listener = (GateKeeperListener)listeners.elementAt(i);
            listener.connected(aSocket);
        }
    }

    public String getContact() 
	throws UnknownHostException {
	String gid = null;
	try {
	    gid = getCredentials().getName().toString();
	} catch (GSSException e) {
	    return null;
	}
	
	StringBuffer url = new StringBuffer();
	url.append(getHost())
	    .append(":")
	    .append(String.valueOf(getPort()))
	    .append(":")
	    .append(gid);
	
	return url.toString();
    }

    class ServiceInfo {
	Class _clazz;
	String [] _args;

	public ServiceInfo(Class clazz, String [] args) {
	    _clazz = clazz;
	    _args = args;
	}
	
	public Class getServiceClass() {
	    return _clazz;
	}

	public String[] getArguments() {
	    return _args;
	}
    }

    public void registerServices(Properties servicesInfo) 
	throws Exception {
	Enumeration e = servicesInfo.keys();
	String key;
	String value;
	while(e.hasMoreElements()) {
	    key = (String)e.nextElement();
	    value = servicesInfo.getProperty(key);
	    
	    _services.put(key, parseServiceDesc(value));
	}
    }

    private ServiceInfo parseServiceDesc(String description) 
	throws Exception {
	
	Class clazz = null;
	String [] args = null;

	QuotedStringTokenizer tokens = new QuotedStringTokenizer(description);
	
	if (tokens.hasMoreTokens()) {
	    String className = null;
	    className = tokens.nextToken().trim();
	    try {
		clazz = Class.forName(className);
	    } catch (Exception e) {
		throw new Exception("Failed to load service class: " + className + 
				    ", " + e.getMessage()) ;
	    }
	    if (!Service.class.isAssignableFrom(clazz)) {
		throw new Exception("Service class must be of Service type");
	    }
	} else {
	    throw new Exception("No service classname specified");
	}
	
	if (tokens.hasMoreTokens()) {
	    int i=0;
	    args = new String [ tokens.countTokens() ];
	    while(tokens.hasMoreTokens()) {
		args[i++] = tokens.nextToken().trim();
	    }
	}
	
	return new ServiceInfo(clazz, args);
    }
    
    public Service getService(String serviceName)
        throws GateKeeperException {
	
	ServiceInfo info = (ServiceInfo)_services.get(serviceName);
	
	if (info == null) {
            throw new ServiceNotFoundException(serviceName);
        }

        Service ser = null;
        try {
            ser = (Service) info.getServiceClass().newInstance();
        } catch(InstantiationException e) {
            throw new ServiceNotFoundException(e.getMessage());
        } catch(IllegalAccessException e) {
            throw new ServiceNotFoundException(e.getMessage());
        }
	
	try {
	    ser.setArguments(info.getArguments());
	} catch(ServiceException e) {
	    throw new GateKeeperException(e.getMessage());
	}
	
	return ser;
    }

    public void registerService(String serviceName, 
				String className, 
				String [] args) 
	throws ClassNotFoundException {
	registerService(serviceName, Class.forName(className), args);
    }
    
    public void registerService(String serviceName, 
				Class clazz, 
				String [] args) {
	if (!Service.class.isAssignableFrom(clazz)) {
	    throw new IllegalArgumentException("Service class must be of Service type : " + clazz);
	}
	_services.put(serviceName, new ServiceInfo(clazz, args));
    }
    
    public boolean unregisterService(String serviceName) {
	return (_services.remove(serviceName) != null);
    }

}
