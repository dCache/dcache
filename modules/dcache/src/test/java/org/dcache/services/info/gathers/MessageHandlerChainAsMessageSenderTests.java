package org.dcache.services.info.gathers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.Args;

/**
 * Some tests that check the MessageHandlerChain's implementation of the
 * MessageSender Interface
 */
public class MessageHandlerChainAsMessageSenderTests {

    /**
     * A small class that implements StateUpdateManager and fails if anything
     * is called.
     */
    static class CactusStateUpdateManager implements StateUpdateManager {
        @Override
        public int countPendingUpdates() {
            fail( "call to countPentingUpdates in SUM");
            return 0;
        }

        @Override
        public void enqueueUpdate( StateUpdate pendingUpdate) {
            fail( "call to enqueueUpdate in SUM");
        }

        @Override
        public void shutdown()
        {
            fail( "call to shutdown in SUM");
        }
    }

    /**
     * A simple test CellMessageAnswerable that fails if any of the methods
     * are called.
     */
    public static class CactusCellMessageAnswerable implements
            CellMessageAnswerable {

        @Override
        public void answerArrived( CellMessage request, CellMessage answer) {
            fail( "call to answerArrived");
        }

        @Override
        public void answerTimedOut( CellMessage request) {
            fail( "call to answerTimedOut");
        }

        @Override
        public void exceptionArrived( CellMessage request, Exception exception) {
            fail( "call to exceptionArrived");
        }
    }

    /**
     * A simple test-class that allows us to enquire what outgoing messages
     * are being sent. Calls to methods other than sendMessage() will fail
     * the test.
     */
    static class MessageCollectingCellEndpoint implements CellEndpoint {
        List<CellMessage> _sendMessages = new ArrayList<>();

        @Override
        public Args getArgs() {
            fail( "call to getArgs");
            return null;
        }

        @Override
        public CellInfo getCellInfo() {
            fail( "call to getCellInfo");
            return null;
        }

        @Override
        public Map<String, Object> getDomainContext() {
            fail( "call to getDomainContext");
            return null;
        }

        @Override
        public CellMessage sendAndWait( CellMessage envelope, long timeout)
                throws SerializationException, NoRouteToCellException,
                InterruptedException {
            fail( "call to sendAndWait");
            return null;
        }

        @Override
        public CellMessage sendAndWaitToPermanent( CellMessage envelope,
                                                   long timeout)
                throws SerializationException, InterruptedException {
            fail( "call to sendAndWaitToPermanent");
            return null;
        }

        @Override
        public void sendMessage( CellMessage envelope)
                throws SerializationException, NoRouteToCellException {
            _sendMessages.add( envelope);
        }

        @Override
        public void sendMessage( CellMessage envelope,
                                 CellMessageAnswerable callback, long timeout)
                throws SerializationException {
            _sendMessages.add( envelope);
        }

        public List<CellMessage> getSentMessages() {
            return _sendMessages;
        }
    }

    MessageSender _sender;
    MessageCollectingCellEndpoint _endpoint;

    @Before
    public void setUp()
    {
        _endpoint = new MessageCollectingCellEndpoint();
        _sender =
                new MessageHandlerChain( new CactusStateUpdateManager(), _endpoint);
    }

    @Test
    public void testSendMessageEnvelope() {
        CellPath dest = new CellPath( "test-cell", "test-domain");
        Serializable obj = new Serializable() {};
        CellMessage msg = new CellMessage( dest, obj);

        _sender.sendMessage( 10, new CactusCellMessageAnswerable(), msg);

        List<CellMessage> sentMsgs = _endpoint.getSentMessages();

        assertEquals( "number of sent messages", 1, sentMsgs.size());

        CellMessage cm = sentMsgs.get( 0);

        assertEquals( "destination", dest, cm.getDestinationPath());
        assertEquals( "message", obj, cm.getMessageObject());
    }

    @Test
    public void testSendMessage() {
        CellPath dest = new CellPath( "test-cell", "test-domain");
        Message vehicle = new Message();

        _sender.sendMessage( 10, dest, vehicle);

        List<CellMessage> sentMsgs = _endpoint.getSentMessages();

        assertEquals( "number of sent messages", 1, sentMsgs.size());

        CellMessage cm = sentMsgs.get( 0);

        assertEquals( "destination", dest, cm.getDestinationPath());
        assertEquals( "vehicle", vehicle, cm.getMessageObject());
    }

    @Test
    public void testSendStringMessage() {
        CellPath dest = new CellPath( "test-cell", "test-domain");
        String request = "get all data";

        _sender.sendMessage( 10, new CactusCellMessageAnswerable(), dest, request);

        List<CellMessage> sentMsgs = _endpoint.getSentMessages();

        assertEquals( "number of sent messages", 1, sentMsgs.size());

        CellMessage cm = sentMsgs.get( 0);

        assertEquals( "destination", dest, cm.getDestinationPath());

        Object msgObject = cm.getMessageObject();
        assertTrue( "msg object is not a String", msgObject instanceof String);
        assertEquals( "request", request, msgObject);
    }
}
