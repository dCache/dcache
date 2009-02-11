package org.dcache.cells;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;

import diskCacheV111.vehicles.Message;
import diskCacheV111.util.CacheException;

/**
 * Stub class for common cell communication patterns. An instance
 * of the template class incapsulates properties such as the
 * destination and timeout for communication.
 *
 * Operations operate at the dCache Message level, and are able to
 * interpret dCache error messages.
 */
public class CellStub
    implements CellMessageSender
{
    private CellEndpoint _endpoint;
    private CellPath _destination;
    private long _timeout = 30000;

    public CellStub()
    {
    }

    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
    }

    public void setDestination(String destination)
    {
        setDestinationPath(new CellPath(destination));
    }

    public void setDestinationPath(CellPath destination)
    {
        _destination = destination;
    }

    public CellPath getDestinationPath()
    {
        return _destination;
    }

    /**
     * Sets the communication timeout of the stub.
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(long timeout)
    {
        _timeout = timeout;
    }

    /**
     * Returns the communiation timeout of the stub.
     */
    public long getTimeout()
    {
        return _timeout;
    }

    /**
     * Sends <code>message</code> asynchronously, expecting a result
     * of type <code>type</code>. The result is delivered to
     * <code>callback</code>.
     */
    public <T extends Message> void send(Message message, Class<T> type,
                                         MessageCallback<T> callback)
    {
        if (_destination == null)
            throw new IllegalStateException("Destination must be specified");
        send(_destination, message, type, callback);
    }

    /**
     * Sends <code>message</code> asynchronously to
     * <code>destination</code>, expecting a result of type
     * <code>type</code>. The result is delivered to
     * <code>callback</code>.
     */
    public <T extends Message> void send(CellPath destination,
                                         Message message, Class<T> type,
                                         MessageCallback<T> callback)
    {
        message.setReplyRequired(true);
        _endpoint.sendMessage(new CellMessage(destination, message),
                              new CellCallback<T>(type, callback),
                              _timeout);
    }

    /**
     * Adapter class to wrap MessageCallback in CellMessageAnswerable.
     */
    class CellCallback<T extends Message> implements CellMessageAnswerable
    {
        private final MessageCallback<T> _callback;
        private final Class<T> _type;

        CellCallback(Class<T> type, MessageCallback<T> callback)
        {
            _callback = callback;
            _type = type;
        }

        public void answerArrived(CellMessage question, CellMessage answer)
        {
            Object o = answer.getMessageObject();
            if (!question.getMessageObject().getClass().isInstance(o)) {
                _callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  "Unexpected reply: " + o);
            }
            T msg = _type.cast(o);
            if (msg.getReturnCode() == 0) {
                _callback.success(msg);
            } else {
                _callback.failure(msg.getReturnCode(), msg.getErrorObject());
            }
        }

        public void answerTimedOut(CellMessage request)
        {
            _callback.timeout();
        }

        public void exceptionArrived(CellMessage request, Exception exception)
        {
            if (exception instanceof NoRouteToCellException) {
                _callback.noroute();
            } else {
                _callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  exception.toString());
            }
        }
    }
}