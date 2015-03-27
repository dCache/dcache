package org.dcache.cells;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.Reply;

import org.dcache.util.CacheExceptionFactory;

/**
 * Encapsulates a Message reply.
 *
 * Similar to dmg.cells.nucleus.DelayedReply, except that MessageReply
 * knows about the dCache Message base class, and that the reply call
 * is non-blocking. The latter means one can safely send the reply
 * from the message delivery thread.
 */
public class MessageReply<T extends Message>
    implements Reply, Future<T>
{
    private CellEndpoint _endpoint;
    private CellMessage _envelope;
    private T _msg;

    @Override
    public synchronized void deliver(CellEndpoint endpoint, CellMessage envelope)
    {
        if (endpoint == null || envelope == null) {
            throw new NullPointerException("Arguments must not be null");
        }
        _endpoint = endpoint;
        _envelope = envelope;
        if (_msg != null) {
            send();
        }
    }

    public boolean isValidIn(long delay)
    {
        return (_envelope == null || delay <= _envelope.getTtl() - _envelope.getLocalAge());
    }

    public void fail(T msg, Exception e)
    {
        if (e instanceof CacheException) {
            CacheException ce = (CacheException) e;
            fail(msg, ce.getRc(), ce.getMessage());
        } else if (e instanceof IllegalArgumentException) {
            fail(msg, CacheException.INVALID_ARGS, e.getMessage());
        } else {
            fail(msg, CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void fail(T msg, int rc, Serializable e)
    {
        msg.setFailed(rc, e);
        reply(msg);
    }

    public synchronized void reply(T msg)
    {
        _msg = msg;
        _msg.setReply();
        if (_envelope != null) {
            send();
        }
        notifyAll();
    }

    protected synchronized void send()
    {
        _envelope.revertDirection();
        _envelope.setMessageObject(_msg);
        _endpoint.sendMessage(_envelope);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    private synchronized T get(T msg)
        throws ExecutionException
    {
        if (msg.getReturnCode() != 0) {
            Exception e;
            Object o = msg.getErrorObject();
            if (o instanceof Exception) {
                e = (Exception) o;
            } else {
                e = CacheExceptionFactory.exceptionOf(msg.getReturnCode(),
                                                      String.valueOf(o));
            }
            throw new ExecutionException(e.getMessage(), e);
        }
        return msg;
    }

    @Override
    public synchronized T get()
        throws InterruptedException, ExecutionException
    {
        while (_msg == null) {
            wait();
        }
        return get(_msg);
    }

    @Override
    public synchronized T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        long expirationTime =
            System.currentTimeMillis() + unit.toMillis(timeout);
        while (_msg == null) {
            long timeLeft = expirationTime - System.currentTimeMillis();
            if (timeLeft <= 0) {
                throw new TimeoutException();
            }
            unit.timedWait(this, timeLeft);
        }
        return get(_msg);
    }

    @Override
    public boolean isCancelled()
    {
        return false;
    }

    @Override
    public synchronized boolean isDone()
    {
        return (_msg != null);
    }
}
