package org.dcache.services.info.conduits;

import java.net.*;
import java.io.*;

import org.dcache.services.info.*;
import org.dcache.services.info.serialisation.XmlSerialiser;

/**
 * Information Exporter class.<br>
 * This class is instantiated by the <code>InfoCollector</code> to send
 * over a plain tcp socket a <code>Schema</code> object that carries out
 * dynamic information from dCache.<br><br>
 * Also this class is independent from the particular implementation of 
 * Schema used. As matter of fact, this class serializes a generic Schema   
 * object over a socket. It's a job of the client to know what particular
 * implementation of Schema was sent.<br><br>
 * Note that client needs only to know the specializing class of the Schema.
 */
public class XmlConduit extends AbstractThreadedConduit {
	
	private static final int DEFAULT_PORT = 22112;
	
	/** TCP port that the server listen on by default */
	public  int _port = DEFAULT_PORT;
	
	/** Server Socket reference */
	private ServerSocket _svr_skt=null;
	
	/** Our serialiser for the current dCache state */
	private XmlSerialiser _xmlSerialiser = new XmlSerialiser();
	
	public void enable() {
		try {
			_svr_skt = new ServerSocket(_port);
		} catch( IOException e) {
			Thread.currentThread().interrupt();
			return;
		} catch(SecurityException e) {
			InfoProvider.getInstance().say("security issue creating port "+_port);
			InfoProvider.getInstance().esay(e);
			return;
		}
		super.enable(); // start the thread.
	}
	

	void triggerBlockingActivityToReturn() {
		if( _svr_skt == null) 
			return;
		
		try {
			_svr_skt.close();
		} catch( IOException e) {
			Thread.currentThread().interrupt();
		} finally {
			_svr_skt = null;
		}
	}

	
	/**
	 * Wait for an incoming connection to the listening socket.  When
	 * one is received, send it the XML serialisation of our current state.
	 */
	void blockingActivity() {
		Socket skt=null;
		
		try {
			skt = _svr_skt.accept();
		} catch( SocketException e) {
			/* This may simply be a user disabling this Conduit */
			// TODO: this is ugly!
			if( !e.toString().equals("Socket closed")) {
				InfoProvider.getInstance().say("accept() failed: >" + e.toString() +"< ");
				InfoProvider.getInstance().esay(e);
			}
		} catch( IOException e) {
			Thread.currentThread().interrupt();
			return;
		} catch( SecurityException e) {
			InfoProvider.getInstance().say("accept() failed for security reasons");
			InfoProvider.getInstance().esay(e);
			return;
		} catch( Exception e) {
			InfoProvider.getInstance().say("accept() induced an unknown exception");
			InfoProvider.getInstance().esay(e);
			return;
		}

		if( skt != null) {
			try {
				_callCount++;
				String xmlData = _xmlSerialiser.serialise();
				skt.getOutputStream().write( xmlData.getBytes());
			} catch( IOException e) {
				InfoProvider.getInstance().say("failed to write XML data.");
				InfoProvider.getInstance().esay(e);
			} catch( Exception e) {
				InfoProvider.getInstance().esay(e);
			} finally {
				try {
					skt.close();			
				} catch( IOException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}
