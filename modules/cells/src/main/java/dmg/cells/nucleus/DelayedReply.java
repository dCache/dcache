package dmg.cells.nucleus;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class DelayedReply implements Reply
{
    private static final long serialVersionUID = -236693000550935733L;

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
        _envelope.revertDirection();
        _envelope.setMessageObject(_msg);
        _endpoint.sendMessage(_envelope);
        _envelope = null;
        _endpoint = null;
    }
}
