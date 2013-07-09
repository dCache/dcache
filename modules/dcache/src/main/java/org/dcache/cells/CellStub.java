package org.dcache.cells;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.CacheExceptionFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Stub class for common cell communication patterns. An instance
 * of the template class encapsulates properties such as the
 * destination and timeout for communication.
 *
 * Operations are aware of the dCache Message class and the
 * CacheException class, and are able to interpret dCache error
 * messages.
 */
public class CellStub
    implements CellMessageSender
{
    private CellEndpoint _endpoint;
    private CellPath _destination;
    private long _timeout = 30000;
    private TimeUnit _timeoutUnit = MILLISECONDS;
    private boolean _retryOnNoRouteToCell;

    public CellStub()
    {
    }

    public CellStub(CellEndpoint endpoint)
    {
        setCellEndpoint(endpoint);
    }

    public CellStub(CellEndpoint endpoint, CellPath destination)
    {
        this(endpoint);
        setDestinationPath(destination);
    }

    public CellStub(CellEndpoint endpoint, CellPath destination, long timeout)
    {
        this(endpoint, destination, timeout, MILLISECONDS);
    }

    public CellStub(CellEndpoint endpoint, CellPath destination, long timeout, TimeUnit unit)
    {
        this(endpoint, destination);
        setTimeout(timeout);
        setTimeoutUnit(unit);
    }

    @Override
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
     * Returns the communication timeout of the stub.
     */
    public long getTimeout()
    {
        return _timeout;
    }

    public void setTimeoutUnit(TimeUnit unit)
    {
        _timeoutUnit = unit;
    }

    public TimeUnit getTimeoutUnit()
    {
        return _timeoutUnit;
    }

    /**
     * Set the value of the retryOnNoRouteCell property, which
     * determines whether to retry on failure to route the message to
     * the destination.
     *
     * If set to false, failure to send the message will cause a
     * TimeoutCacheException to be reported right away. If set to
     * true, failure to send the message to the destination cell will
     * be retried until the timeout has been reached. Once the timeout
     * is reached, a TimeoutCacheException is thrown. This is useful
     * for destinations for which communication failure is known to be
     * temporary.
     *
     * Limitations: This property currently only has an effect on the
     * sendAndWait method. Asynchronous message delivery always
     * reports a no route error in case of communication failure.
     */
    public void setRetryOnNoRouteToCell(boolean retry)
    {
        _retryOnNoRouteToCell = retry;
    }

    /**
     * Returns the value of the retryOnNoRouteCell property, which
     * determines whether to retry on failure to route the message to
     * the destination.
     */
    public boolean getRetryOnNoRouteToCell()
    {
        return _retryOnNoRouteToCell;
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  msg     the message object to send
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(T msg)
        throws CacheException, InterruptedException
    {
        return sendAndWait(msg, _timeout);
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  msg     the message object to send
     * @param  timeout in milliseconds to wait for a reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(T msg, long timeout)
        throws CacheException, InterruptedException
    {
        return sendAndWait(_destination, msg, timeout);
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(CellPath path, T msg)
        throws CacheException, InterruptedException
    {
        msg.setReplyRequired(true);

        T reply = (T) sendAndWait(path, msg, msg.getClass());

        if (reply.getReturnCode() != 0) {
            throw CacheExceptionFactory.exceptionOf(reply);
        }

        return reply;
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T extends Serializable> T
                      sendAndWait(Serializable msg, Class<T> type)
        throws CacheException, InterruptedException
    {
        return sendAndWait(_destination, msg, type);
    }


    public <T extends Serializable> T sendAndWait(CellPath path,
                                                  Serializable msg,
                                                  Class<T> type)
       throws CacheException, InterruptedException
    {
       return sendAndWait(path, msg, type, _timeout);
    }


    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  timeout  the time to wait for the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T extends Serializable> T
                      sendAndWait(Serializable msg, Class<T> type, long timeout)
        throws CacheException, InterruptedException
    {
        return sendAndWait(_destination, msg, type, timeout);
    }



    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @param  timeout  the time to wait for the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(CellPath path, T msg, long timeout)
        throws CacheException, InterruptedException
    {
        msg.setReplyRequired(true);

        T reply = (T) sendAndWait(path, msg, msg.getClass(), timeout);

        if (reply.getReturnCode() != 0) {
            throw CacheExceptionFactory.exceptionOf(reply);
        }

        return reply;
    }


    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  timeout  the time to wait for the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T extends Serializable> T sendAndWait(CellPath path,
                                                  Serializable msg,
                                                  Class<T> type,
                                                  long timeout)
        throws CacheException, InterruptedException
    {
        CellMessage replyMessage;
        try {
            CellMessage envelope = new CellMessage(path, msg);
            if (_retryOnNoRouteToCell) {
                replyMessage =
                    _endpoint.sendAndWaitToPermanent(envelope, timeout);
            } else {
                replyMessage =
                    _endpoint.sendAndWait(envelope, timeout);
            }
        } catch (NoRouteToCellException e) {
            /* From callers point of view a timeout due to a lost
             * message or a missing route to the destination is pretty
             * much the same, so we report this as a timeout. The
             * error message gives the details.
             */
            throw new TimeoutCacheException(e.getMessage());
        }

        if (replyMessage == null) {
            String errmsg = String.format("Request to %s timed out.", path);
            throw new TimeoutCacheException(errmsg);
        }

        Object replyObject = replyMessage.getMessageObject();
        if (!(type.isInstance(replyObject))) {
            String errmsg = "Got unexpected message of class " +
                replyObject.getClass() + " from " +
                replyMessage.getSourcePath();
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     errmsg);
        }

        return (T)replyObject;
    }

    /**
     * Sends <code>message</code> asynchronously, expecting a result
     * of type <code>type</code>. The result is delivered to
     * <code>callback</code>.
     */
    public <T extends Serializable> void send(Serializable message,
                                              Class<T> type,
                                              MessageCallback<T> callback)
    {
        if (_destination == null) {
            throw new IllegalStateException("Destination must be specified");
        }
        send(_destination, message, type, callback);
    }

    /**
     * Sends <code>message</code> asynchronously to
     * <code>destination</code>, expecting a result of type
     * <code>type</code>. The result is delivered to
     * <code>callback</code>.
     */
    public <T extends Serializable> void send(CellPath destination,
                                              Serializable message,
                                              Class<? extends T> type,
                                              MessageCallback<T> callback)
    {
        if (message instanceof Message) {
            ((Message) message).setReplyRequired(true);
        }
        _endpoint.sendMessage(new CellMessage(destination, message),
                              new CellCallback<>(type, callback),
                              _timeout);
    }

    /**
     * Sends <code>message</code> to <code>destination</code>.
     */
    public void send(Serializable message)
        throws NoRouteToCellException
    {
        if (_destination == null) {
            throw new IllegalStateException("Destination must be specified");
        }
        send(_destination, message);
    }


    /**
     * Sends <code>message</code> to <code>destination</code>.
     */
    public void send(CellPath destination, Serializable message)
        throws NoRouteToCellException
    {
        _endpoint.sendMessage(new CellMessage(destination, message));
    }

    /**
     * Adapter class to wrap MessageCallback in CellMessageAnswerable.
     */
    static class CellCallback<T> implements CellMessageAnswerable
    {
        private final MessageCallback<T> _callback;
        private final Class<? extends T> _type;

        CellCallback(Class<? extends T> type, MessageCallback<T> callback)
        {
            _callback = callback;
            _type = type;
        }

        @Override
        public void answerArrived(CellMessage request, CellMessage answer)
        {
            Object o = answer.getMessageObject();
            if (_type.isInstance(o)) {
                _callback.setReply(_type.cast(o));
                if (o instanceof Message) {
                    Message msg = (Message) o;
                    int rc = msg.getReturnCode();
                    if (rc == 0) {
                        _callback.success();
                    } else {
                        _callback.failure(rc, msg.getErrorObject());
                    }
                } else {
                    _callback.success();
                }
            } else if (o instanceof Exception) {
                exceptionArrived(request, (Exception) o);
            } else {
                _callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  "Unexpected reply: " + o);
            }
        }

        @Override
        public void answerTimedOut(CellMessage request)
        {
            _callback.timeout(request.getDestinationPath());
        }

        @Override
        public void exceptionArrived(CellMessage request, Exception exception)
        {
            if (exception instanceof NoRouteToCellException) {
                _callback.noroute(request.getDestinationPath());
            } else if (exception instanceof CacheException) {
                CacheException e = (CacheException) exception;
                _callback.failure(e.getRc(),
                        CacheExceptionFactory.exceptionOf(e.getRc(), e.getMessage()));
            } else {
                _callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  exception.toString());
            }
        }
    }
}
