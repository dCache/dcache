package org.dcache.services.info.gathers;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;

import org.dcache.util.Args;

import static org.junit.Assert.*;

/**
 * Some tests that check the MessageHandlerChain's implementation of the
 * MessageSender Interface
 */
public class MessageHandlerChainAsMessageSenderTests {

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
        public void sendMessage( CellMessage envelope)
                throws SerializationException, NoRouteToCellException {
            _sendMessages.add( envelope);
        }

        @Override
        public void sendMessage(CellMessage envelope, CellMessageAnswerable callback,
                                Executor executor, long timeout)
                throws SerializationException
        {
            _sendMessages.add( envelope);
        }

        @Override
        public void sendMessageWithRetryOnNoRouteToCell(CellMessage envelope, CellMessageAnswerable callback,
                                                        Executor executor, long timeout)
                throws SerializationException
        {
            _sendMessages.add(envelope);
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
        MessageHandlerChain mhc = new MessageHandlerChain();
        mhc.setCellEndpoint(_endpoint);
        _sender = mhc;
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
