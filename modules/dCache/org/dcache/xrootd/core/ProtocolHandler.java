package org.dcache.xrootd.core;

import java.io.IOException;
import java.util.Arrays;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.stream.TooMuchLogicalStreamsException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.AuthentiticationResponse;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.LoginResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

public class ProtocolHandler {

	private PhysicalXrootdConnection physicalConnection;

	public ProtocolHandler(PhysicalXrootdConnection physicalConnection) {
		this.physicalConnection = physicalConnection;
	}
	
	public void handleRequest(int[] rawRequest) throws IOException {
		
		AbstractRequestMessage requestMsg = unmarshal(rawRequest);
		
//		message id unknown or unsupported ?
		if (requestMsg == null) {
			physicalConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(requestMsg.getStreamID(), XrootdProtocol.kXR_Unsupported, "invalid or unsupported request"));
			return;
		}
		 
		
//		login and authentitication are handled synchronously because they affect the physical networkconnection (socket)
		
//		request is login attempt? 
		if (requestMsg instanceof LoginRequest) {
			
//			handle login request synchronously
			if (physicalConnection.handleLoginRequest((LoginRequest) requestMsg)) {
//				send back positive response immediately
				physicalConnection.getResponseEngine().sendResponseMessage(new LoginResponse(requestMsg.getStreamID(), null, null));
			} else {
//				send back negative response in case of login failure
				physicalConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(requestMsg.getStreamID(), XrootdProtocol.kXR_NotAuthorized, "login failed"));
			}
			
			return;
		} 
		
//		request is authentitication attempt?
		if (requestMsg instanceof AuthentiticationRequest) {
			
//			handle auth request synchronously
			if (physicalConnection.handleAuthRequest((AuthentiticationRequest) requestMsg)) {
//				send back positive response immediately
				physicalConnection.getResponseEngine().sendResponseMessage(new AuthentiticationResponse(requestMsg.getStreamID(), 0, 0));
			} else {
//				send back negative response in case of auth failure
				physicalConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(requestMsg.getStreamID(), XrootdProtocol.kXR_NotAuthorized, "authentitication failed"));
			}
						
			return;
		}

//		at this point we assume the request is a 'normal' xrootd message
//		request needs Login (see protocol matrix) ?
//		request needs auth (see protocol matrix) ?
		
		try {
			
//			requests regarding file operations are handled asynchronously by indepedent (multithreaded) logical streams
			physicalConnection.getStreamManager().dispatchMessage(requestMsg);
			
		} catch (TooMuchLogicalStreamsException e) {
			System.err.println("discarding request: too much logical streams");
			physicalConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(requestMsg.getStreamID(), XrootdProtocol.kXR_noserver, "cannot process request: too much logical streams in use"));
		}		
		
		
		
		
		
		
			
//			find the session (logical stream) adressed by the message's stream id
//			AsyncSession session = null;
//			try {
//				
//				session = (AsyncSession) sm.getSession(sID);
//				
//			} catch (TooMuchSessionsException e) {
//				System.err.println(e.getMessage());
//				
//				putResponse(new ErrorResponse(sID, XrootdProtocol.kXR_noserver, "maximum number of logical streams reached."));
//				return;
//			}
//			
//			System.out.println(Thread.currentThread().getName()+": dispatching request "+request.getClass().getName()+" (SID="+request.getStreamID()+")");
//			
////			dispatch message to corresponding session
//			session.putRequest(request);
		
//		} else 	{
						
			
			
//		}
	}
	
	protected byte[] marshal(AbstractResponseMessage res) {
		
		int hl = res.getHeader().length;
		int dl = res.getData().length;
		byte[] result = new byte[ hl + dl ];
		
		System.arraycopy(res.getHeader(), 0, result, 0, hl);
		System.arraycopy(res.getData(), 0, result, hl, dl);
		
		return result;
	}
	
	protected AbstractRequestMessage unmarshal(int[] header) throws IOException {
	
		int requestID = header[2]  << 8 | header[3];
		int dlength = header[20] << 24 | header[21] << 16 | header[22] << 8 | header[23];
		
//		int[] data = null;
		byte[] data = null;
		
		if (dlength > 0) {
//			data = physicalConnection.getRequestEngine().receive(dlength);
			data = physicalConnection.getRequestEngine().receiveData(dlength);
		
			if (data == null) {
				System.err.println("data part of received request incomplete or corrupt");
				return null;
			}
			
		}
		
		AbstractRequestMessage result = null;
		
		switch (requestID) {
		case XrootdProtocol.kXR_login:
			result = new LoginRequest(header, data);
			break;
		case XrootdProtocol.kXR_open:
			result = new OpenRequest(header, data);
			break;
		case XrootdProtocol.kXR_stat:
			result = new StatRequest(header, data);
			break;
		case XrootdProtocol.kXR_read:
			result = new ReadRequest(header, data);
			break;
		case XrootdProtocol.kXR_write:
			result = new WriteRequest(header, data);
			break;
		case XrootdProtocol.kXR_sync:
			result = new SyncRequest(header, data);
			break;
		case XrootdProtocol.kXR_close:
			result = new CloseRequest(header, data);
			break;
		}
		
		return result;
	}
	
	public int getMessageLength() {
		return XrootdProtocol.CLIENT_REQUEST_LEN;
	}
	
	public int getHandshakeLength() {
		return XrootdProtocol.CLIENT_HANDSHAKE_LEN;
	}

	public byte[] handshake(byte[] request) {
		
		byte [] result = null; 
		
		if (Arrays.equals(request, XrootdProtocol.HANDSHAKE_REQUEST)) {
			
			switch (physicalConnection.getServerType()) {
				case XrootdProtocol.LOAD_BALANCER:
					result = XrootdProtocol.HANDSHAKE_RESPONSE_LOADBALANCER;
					break;

				case XrootdProtocol.DATA_SERVER:
					result = XrootdProtocol.HANDSHAKE_RESPONSE_DATASERVER;
					break;
			} 
		}
		
		return result;
	}
	
}
