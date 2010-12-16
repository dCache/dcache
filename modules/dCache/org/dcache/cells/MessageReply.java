package org.dcache.cells;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.Reply;
import diskCacheV111.vehicles.Message;
import diskCacheV111.util.CacheException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a Message reply.
 *
 * Similar to dmg.cells.nucleus.DelayedReply, except that MessageReply
 * knows about the dCache Message base class, and that the reply call
 * is non-blocking. The latter means one can safely send the reply
 * from the message delivery thread.
 */
public class MessageReply implements Reply
{
    private static final Logger _logger =
        LoggerFactory.getLogger(MessageReply.class);

    private CellEndpoint _endpoint;
    private CellMessage _envelope;
    private Message _msg;

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

    public void fail(Message msg, Exception e)
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

    public void fail(Message msg, int rc, Object e)
    {
        msg.setFailed(rc, e);
        reply(msg);
    }

    public synchronized void reply(Message msg)
    {
        _msg = msg;
        _msg.setReply();
        if (_envelope != null) {
            send();
        }
    }

    protected void onNoRouteToCell(NoRouteToCellException e)
    {
        _logger.error("Failed to send reply: " + e.getMessage());
    }

    protected synchronized void send()
    {
        try {
            _envelope.setMessageObject(_msg);
            _endpoint.sendMessage(_envelope);
        } catch (NoRouteToCellException e) {
            onNoRouteToCell(e);
        }
    }
}