package org.dcache.xrootd.core.connection;

import java.io.IOException;
import java.net.Socket;

import org.dcache.xrootd.core.ProtocolHandler;
import org.dcache.xrootd.core.request.RequestEngine;
import org.dcache.xrootd.core.response.AbstractResponseEngine;
import org.dcache.xrootd.core.stream.LogicalStreamManager;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.network.NetworkConnection;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;

public class PhysicalXrootdConnection {
	
	private NetworkConnection networkConnection;
	private RequestEngine request;
	private AbstractResponseEngine response;
	private ProtocolHandler protocolHandler;
	private ConnectionStatus status;
	private LogicalStreamManager streamManager;
	private PhysicalConnectionListener connectionListener = new GenericConnectionListener();
	private int serverType;

			
	public PhysicalXrootdConnection(Socket socket, int serverType) throws IOException {
		this(new NetworkConnection(socket), serverType);
	}
	
	public PhysicalXrootdConnection(NetworkConnection networkConnection, int serverType) {
		this.networkConnection = networkConnection;
		this.protocolHandler = new ProtocolHandler(this);
		this.request = new RequestEngine(this);
//		this.response = new NewResponseEngine(this);
		this.status = new ConnectionStatus();
		this.streamManager = new LogicalStreamManager(this);
		this.serverType = serverType;
		
//		getStatus().setConnected(true);
//		this.response.startEngine();
//		this.request.startEngine();						
	}
	
	public RequestEngine getRequestEngine() {
		return request;
	}
	
	public AbstractResponseEngine getResponseEngine() {
		return response;
	}
	
	public LogicalStreamManager getStreamManager() {
		return streamManager;
	}
	
	public ProtocolHandler getProtocolHandler() {
		return protocolHandler;
	}
	
	public NetworkConnection getNetworkConnection() {
		return networkConnection;
	}

	public synchronized void closeConnection() {
						
		getStreamManager().destroyAllStreams();
		
		getResponseEngine().stopEngine();
		getRequestEngine().stopEngine();
		
		connectionListener.closeConnection();
		
		if (getStatus().isConnected()) {
			
			getNetworkConnection().close();
			getStatus().setConnected(false);
		}
	}

	public ConnectionStatus getStatus() {
		return status;
	}

	public void setStatus(ConnectionStatus status) {
		this.status = status;
	}

	public PhysicalConnectionListener getConnectionListener() {
		return connectionListener;
	}

	public void setConnectionListener(PhysicalConnectionListener listener) {
		this.connectionListener = listener;
	}
		
	/**
	 * set timeout of network connection
	 * @param timeout timeout in ms, 0 (default) means no timeout
	 */
	
	public void setTimeout(int timeout) {
		getNetworkConnection().setConnectionTimout(timeout);
	}
	
	public int getTimeout() {
		return getNetworkConnection().getConnectionTimout();
	}
	
	public void handleHandshakeRequest() {
		getConnectionListener().handshakeRequest();
	}
	
	public boolean handleLoginRequest(LoginRequest login) {
		
		boolean result = false;
		
		if (getConnectionListener().loginRequest(login)) {
			getStatus().setLoggedIn(true);
			result = true;
		} else {
			getStatus().setLoggedIn(false);
		}
		
		return result;
	}
	
	public boolean  handleAuthRequest(AuthentiticationRequest auth) {
		
		boolean result = false;
		
		if (getConnectionListener().authRequest(auth)) {
			getStatus().setAuthenticated(true);
			result = true;
		} else {
			getStatus().setAuthenticated(false);
		}
		
		return result;
	}

	public void setRequestEngine(RequestEngine request) {
		this.request = request;
	}

	public void setResponseEngine(AbstractResponseEngine response) {
		this.response = response;
	}

	public void setNetworkConnection(NetworkConnection networkConnection) {
		this.networkConnection = networkConnection;
	}

	public void setProtocolHandler(ProtocolHandler protocolHandler) {
		this.protocolHandler = protocolHandler;
	}

	public void setStreamManager(LogicalStreamManager streamManager) {
		this.streamManager = streamManager;
	}
	
	public void setMaxStreams(int number) {
		getStreamManager().setMaxStreams(number);
	}
	
	public int getMaxStreams() {
		return getStreamManager().getMaxStreams();
	}

	public StreamListener handleNewStream(int streamID) {
		return connectionListener.newStreamForked(streamID);
	}

	public int getServerType() {
		return serverType;
	}
}