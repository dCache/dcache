package org.dcache.tests.cells;

import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import diskCacheV111.vehicles.Message;

public class GenericMockCellHelper extends CellAdapterHelper {



    private static class MessageEnvelope {

        private final Message _message;
        private final boolean _isPersistent;

        MessageEnvelope(Message message, boolean isPersistent) {
            _message = message;
            _isPersistent = isPersistent;
        }

        Message getMessage() {
            return _message;
        }

        boolean isPesistent() {
            return _isPersistent;
        }
    }


    private static final Map<CellPath, Map<String, List<MessageEnvelope>>> _messageQueue = new HashMap<CellPath, Map<String, List<MessageEnvelope>>>();
    private final CellNucleus _nucleus;

    public GenericMockCellHelper(String name, String args) {
        super(name, args);
        _nucleus = new NucleusHelper(this, name+"-fake");
    }

    @Override
    public CellMessage sendAndWait(CellMessage msg, long millisecs) throws NotSerializableException, NoRouteToCellException, InterruptedException {

        Object messageObject = msg.getMessageObject();

        if( messageObject instanceof Message ) {


            CellPath destinationPath = msg.getDestinationAddress();

            Map<String, List<MessageEnvelope>> messagesByType = _messageQueue.get(destinationPath);
            if( messagesByType == null ) {
                return null;
            }

            String messageType = messageObject.getClass().getName();
            List<MessageEnvelope> messages = messagesByType.get(messageType);

            if( messages == null || messages.isEmpty() ) {
                return null;
            }

            MessageEnvelope messageEnvelope = messages.get(0);
            if( !messageEnvelope.isPesistent() ) {
                messages.remove(0);
            }

            Message message = messageEnvelope.getMessage();
            msg.setMessageObject(message);

            return msg;
        }

        return null;
    }

    @Override
    public void sendMessage(CellMessage msg) throws NotSerializableException, NoRouteToCellException {
        // OK :)
    }

    /**
     *
     * same as <i>prepareMessage( cellPath, message, false);</i>
     *
     * @param cellPath
     * @param message
     */
    public static void prepareMessage(CellPath cellPath, Message message) {
        prepareMessage( cellPath, message, false);
    }


    /**
     * create pre-defined reply from a cell.
     *
     * @param cellPath
     * @param message
     * @param isPesistent remove message from reply list if false
     */
    public static void prepareMessage(CellPath cellPath, Message message, boolean isPesistent) {

        Map<String, List<MessageEnvelope>> messagesByType = _messageQueue.get(cellPath);

        if( messagesByType == null ) {
            messagesByType = new HashMap<String,List<MessageEnvelope>>();
            _messageQueue.put(cellPath, messagesByType);
        }

        String messageType = message.getClass().getName();
        List<MessageEnvelope> messages = messagesByType.get(messageType);
        if(messages == null) {
            messages = new ArrayList<MessageEnvelope>();
            messagesByType.put(messageType, messages);
        }
        messages.add( new MessageEnvelope(message, isPesistent));

    }


    /*
     * Fake nucleus
     */


    @Override
    public CellNucleus getNucleus() {
        return _nucleus;

    }

    public static class NucleusHelper extends CellNucleus {
        public final CellAdapter _cell;
        public NucleusHelper(CellAdapter cell, String name) {
            super(cell, name, "Generic");
            _cell = cell;
        }

        @Override
        public void sendMessage(CellMessage msg, boolean local, boolean remote,
                CellMessageAnswerable callback, long timeout) throws NotSerializableException {

            CellMessage reply;
            try {
                reply = _cell.sendAndWait(msg, timeout);
                if( reply != null ) {
                    callback.answerArrived(msg, reply);
                }
            } catch (NoRouteToCellException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }


    }


}
