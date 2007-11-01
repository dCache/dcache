package org.dcache.xrootd.core.stream;

import java.util.HashMap;
import java.util.Iterator;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;

public class LogicalStreamManager {
	
	HashMap streams = new HashMap();
	private int maxLogicalStreams = 100;
	private PhysicalXrootdConnection physicalConnection;
	
	public LogicalStreamManager(PhysicalXrootdConnection physicalConnection) {
		this.physicalConnection = physicalConnection;
	}
	
	public LogicalStream getStream(int streamID) throws TooMuchLogicalStreamsException {
		Integer key = new Integer(streamID);
		
		if (streams.containsKey(key))
			return (LogicalStream) streams.get(key);
		
		if (streams.keySet().size() == maxLogicalStreams)
			throw new TooMuchLogicalStreamsException("number of multiple sessions is limited to "+maxLogicalStreams);
			
		LogicalStream newStream = new LogicalStream(physicalConnection, streamID);
		streams.put(key, newStream);
				
		StreamListener streamListener = physicalConnection.handleNewStream(streamID);
		newStream.setListener(streamListener);
		
		return newStream;
	}
	
	public int getNumberOfStreams() {
		return streams.size();
	}
	
	public void destroyStream(int streamID) {
		
		Integer id = new Integer(streamID);
		
		LogicalStream stream = (LogicalStream) streams.get(id);
		
		if (stream != null) {
			stream.close();
			streams.remove(id);
		}
			
//		if (streams.isEmpty() && physicalConnection.getStatus().isConnected()) {
//			
//			if (physicalConnection.getTimeout() == 0) {
//				physicalConnection.closeConnection();
//			}
			
//			if (physicalConnection.hasTimeout()) {
//				if (physicalConnection.getTimeout() == 0) {
//					physicalConnection.closeConnection();
//				}
//			}
//		}
	}	
	
	public void destroyAllStreams() {
		
		if (streams.isEmpty()) {
			System.out.println("no open streams to close");
		} else {
			for (Iterator it = streams.values().iterator(); it.hasNext(); ) {
				((LogicalStream)it.next()).close();
			}
			
			System.out.println(streams.size()+" streams closed");
			streams.clear();
		}
	}
	
	/**
	 * Hand over request message to the appopriate logical stream thread according to the StreamID
	 * @param requestMessage
	 * @throws TooMuchLogicalStreamsException in case a new logical stream (e.g. a new file open) is requested, but the limit of concurrent streams is reached
	 */
	public void dispatchMessage(AbstractRequestMessage request) throws TooMuchLogicalStreamsException {
		getStream(request.getStreamID()).putRequest(request);
	}

	public void setMaxStreams(int number) {
		maxLogicalStreams = number;
	}

	public int getMaxStreams() {
		return maxLogicalStreams;
	}
	
}
