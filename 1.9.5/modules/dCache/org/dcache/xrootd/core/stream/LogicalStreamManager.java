package org.dcache.xrootd.core.stream;

import java.util.HashMap;
import java.util.Iterator;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;

import org.apache.log4j.Logger;

public class LogicalStreamManager {

    private final static Logger _log =
        Logger.getLogger(LogicalStreamManager.class);

    protected HashMap streams = new HashMap();
    protected int maxLogicalStreams = 100;
    protected PhysicalXrootdConnection physicalConnection;

    public LogicalStreamManager(PhysicalXrootdConnection physicalConnection) {
        this.physicalConnection = physicalConnection;
    }

    public LogicalStream getStream(AbstractRequestMessage request) throws TooMuchLogicalStreamsException {
        Integer key = new Integer(request.getStreamID());

        if (streams.containsKey(key))
            return (LogicalStream) streams.get(key);

        if (streams.keySet().size() == maxLogicalStreams)
            throw new TooMuchLogicalStreamsException("number of multiple sessions is limited to "+maxLogicalStreams);

        LogicalStream newStream = new LogicalStream(physicalConnection, request.getStreamID());
        streams.put(key, newStream);

        StreamListener streamListener = physicalConnection.handleNewStream(request.getStreamID());
        newStream.setListener(streamListener);

        return newStream;
    }

    public LogicalStream getStreamByID(int streamID) {
        return (LogicalStream) streams.get(streamID);
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
            _log.debug("no open streams to close");
        } else {
            for (Iterator it = streams.values().iterator(); it.hasNext(); ) {
                ((LogicalStream)it.next()).close();
            }

            _log.debug(streams.size()+" streams closed");
            streams.clear();
        }
    }

    /**
     * Hand over request message to the appopriate logical stream thread according to the StreamID
     * @param requestMessage
     * @throws TooMuchLogicalStreamsException in case a new logical stream (e.g. a new file open) is requested, but the limit of concurrent streams is reached
     */
    public void dispatchMessage(AbstractRequestMessage request) throws TooMuchLogicalStreamsException {
        getStream(request).putRequest(request);
    }

    public void setMaxStreams(int number) {
        maxLogicalStreams = number;
    }

    public int getMaxStreams() {
        return maxLogicalStreams;
    }

}
