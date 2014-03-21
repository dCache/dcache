package org.dcache.tests.cells;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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

public class MockCellEndpoint implements CellEndpoint
{
    private final Map<CellPath, Map<String, List<MessageEnvelope>>> messageQueue = new HashMap<>();
    private final Map<String, Map<Class<?>, MessageAction>> messageActions = new HashMap<>();
    private final Args args;
    private final CellInfo info;

    public MockCellEndpoint(String name, String args)
    {
        this.args = new Args(args);

        info = new CellInfo();
        info.setCellName(name);
        info.setDomainName("mockDomain");
        info.setCellType("generic");
        info.setCreationTime(new Date());
    }

    /**
     *
     * same as <i>prepareMessage( cellPath, message, false);</i>
     *
     * @param cellPath
     * @param message
     */
    public void prepareMessage(CellPath cellPath, Message message)
    {
        prepareMessage(cellPath, message, false);
    }

    /**
     * create pre-defined reply from a cell.
     *
     * @param cellPath
     * @param message
     * @param isPersistent remove message from reply list if false
     */
    public void prepareMessage(CellPath cellPath, Message message, boolean isPersistent)
    {
        Map<String, List<MessageEnvelope>> messagesByType = messageQueue.get(cellPath);

        if (messagesByType == null) {
            messagesByType = new HashMap<>();
            messageQueue.put(cellPath, messagesByType);
        }

        String messageType = message.getClass().getName();
        List<MessageEnvelope> messages = messagesByType.get(messageType);
        if (messages == null) {
            messages = new ArrayList<>();
            messagesByType.put(messageType, messages);
        }
        messages.add(new MessageEnvelope(message, isPersistent));
    }

    public void registerAction(String cellName, Class<?> messageClass, MessageAction action)
    {
        Map<Class<?>, MessageAction> actions = messageActions.get(cellName);
        if (actions == null) {
            actions = new HashMap<>();
            messageActions.put(cellName, actions);
        }
        actions.put(messageClass, action);
    }

    @Override
    public void sendMessage(CellMessage envelope, CellMessageAnswerable callback, Executor executor, long timeout)
            throws SerializationException
    {
        Map<String, List<MessageEnvelope>> messages = messageQueue.get(envelope.getDestinationPath());
        List<MessageEnvelope> envelopes = messages.get(envelope.getMessageObject().getClass().getName());
        MessageEnvelope m = envelopes.get(0);
        if(!m.isPersistent()) {
            envelopes.remove(0);
        }
        callback.answerArrived(envelope, new CellMessage(envelope.getDestinationPath(), m.getMessage()));
    }

    @Override
    public void sendMessage(CellMessage envelope)
            throws NoRouteToCellException
    {
        String destinations = envelope.getDestinationPath().getCellName();

        Map<Class<?>, MessageAction> actions = messageActions.get(destinations);
        if (actions != null) {
            // there is something pre-defined
            MessageAction action = actions.get(envelope.getMessageObject().getClass());
            if (action != null) {
                envelope.revertDirection();
                action.messageArrived(envelope);
            }
        }
    }

    @Override
    public void sendMessageWithRetryOnNoRouteToCell(CellMessage envelope, CellMessageAnswerable callback, Executor executor, long timeout)
            throws SerializationException
    {
        sendMessage(envelope, callback, executor, timeout);
    }

    @Override
    public CellInfo getCellInfo()
    {
        return info;
    }

    @Override
    public Map<String, Object> getDomainContext()
    {
        return Collections.emptyMap();
    }

    @Override
    public Args getArgs()
    {
        return args;
    }

    public interface MessageAction
    {
        void messageArrived(CellMessage message);
    }

    private static class MessageEnvelope
    {
        private final Message message;
        private final boolean isPersistent;

        MessageEnvelope(Message message, boolean isPersistent)
        {
            this.message = message;
            this.isPersistent = isPersistent;
        }

        Message getMessage()
        {
            return message;
        }

        boolean isPersistent()
        {
            return isPersistent;
        }
    }
}
