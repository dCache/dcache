package org.dcache.xrootd.core.stream;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd.protocol.messages.OpenRequest;


/*
 * This class is just a quick hack to support current xrootd clients sending messages
 * asyncronously. It will just put possibly async xrootd messages into sequence by using
 * the queue of a single LogicalStream instance. Therefore this channel can only be used for
 * a single file session (open, io, close), which is the case in the mover. Attempting a
 * 2nd open on this channel will raise an TooMuchStreamsException.
 *
 */
public class LogicalStreamManager2 extends LogicalStreamManager {

    // the dummy logical stream number (since we have only one per physical connection)
    private final Integer _dummyKey = new Integer(0);

    public LogicalStreamManager2(PhysicalXrootdConnection physicalConnection) {
        super(physicalConnection);
    }

    @Override
    public LogicalStream getStream(AbstractRequestMessage request)
        throws TooMuchLogicalStreamsException {


        if ( !( request instanceof OpenRequest) ){
            return (LogicalStream) streams.get(_dummyKey);
        }

        if (streams.keySet().size() >= maxLogicalStreams) {
            throw new TooMuchLogicalStreamsException("number of file opens is limited to "+maxLogicalStreams +"on this physical connection.");
        }

        LogicalStream newStream = new LogicalStream(physicalConnection, _dummyKey);
        streams.put(_dummyKey, newStream);

        StreamListener streamListener = physicalConnection.handleNewStream(_dummyKey);
        newStream.setListener(streamListener);

        return newStream;
    }
}
