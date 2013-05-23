package dmg.cells.nucleus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class DelayedReply implements Reply
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedReply.class);

    private CellEndpoint _endpoint;
    private CellMessage _envelope;
    private Serializable _msg;

    @Override
    public synchronized void deliver(CellEndpoint endpoint, CellMessage envelope)
    {
        _endpoint = checkNotNull(endpoint);
        _envelope = checkNotNull(envelope);
        if (_msg != null) {
            send();
        }
    }

    public synchronized void reply(Serializable msg)
    {
        _msg = msg;
        if (_envelope != null) {
            send();
        }
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

    protected void onNoRouteToCell(NoRouteToCellException e)
    {
        LOGGER.error("Failed to send reply: " + e.getMessage());
    }
}
