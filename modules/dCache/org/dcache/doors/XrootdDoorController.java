package org.dcache.doors;

import org.dcache.xrootd.core.connection.PhysicalConnectionListener;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.response.ThreadedResponseEngine;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.OKResponse;


public class XrootdDoorController implements PhysicalConnectionListener {

	private XrootdDoor door;
	private PhysicalXrootdConnection physicalXrootdConnection;

	public XrootdDoorController(XrootdDoor door, PhysicalXrootdConnection physicalXrootdConnection) {
		this.door = door;
		this.physicalXrootdConnection = physicalXrootdConnection;
		
		physicalXrootdConnection.setMaxStreams(door.getMaxFileOpens());
		
		physicalXrootdConnection.setResponseEngine(new ThreadedResponseEngine(physicalXrootdConnection));
		physicalXrootdConnection.getResponseEngine().startEngine();
		physicalXrootdConnection.getRequestEngine().startEngine();
				
		physicalXrootdConnection.getStatus().setConnected(true);
	}
	
	public void handshakeRequest() {
		door.say("handshake attempt coming from "+physicalXrootdConnection.getNetworkConnection().getSocket().getRemoteSocketAddress().toString());
	}

	public AbstractResponseMessage loginRequest(LoginRequest login) {
		
//		plug login module method here
		door.say("login attempt, access granted");
				
		return new OKResponse(login.getStreamID());
	}

	public AbstractResponseMessage authRequest(AuthentiticationRequest auth) {

//		plug authentitication module here

		door.say("authentitication passed");
		
		return new OKResponse(auth.getStreamID());
	}

	public StreamListener newStreamForked(int streamID) {
		return new XrootdDoorListener(this, streamID);
	}
	public void closeConnection() {
		
		door.clearOpenFiles();

//		set flag to prevent endless shutdown loop (important for door.cleanUp() )
		if (!door.isCloseInProgress()) {

//			end door mini thread
			synchronized (door) {
				door.notify();
			}
			
//			end cell, caused door.cleanUp() to be called
			door.getNucleus().kill();

		}		
		
	}
	
	public XrootdDoor getDoor() {
		return door;
	}
	
	public PhysicalXrootdConnection getXrootdConnection() {
		return physicalXrootdConnection;
	}

	public void shutdownXrootd() {
		physicalXrootdConnection.getNetworkConnection().close();
		physicalXrootdConnection.closeConnection();
	}

}
