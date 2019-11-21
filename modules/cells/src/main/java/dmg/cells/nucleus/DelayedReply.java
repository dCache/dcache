package dmg.cells.nucleus;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class DelayedReply implements Reply
{
    private static final long serialVersionUID = -236693000550935733L;

    private CellEndpoint _endpoint;
    private CellMessage _envelope;
    private Serializable _msg;

    @Override
    public synchronized void deliver(CellEndpoint endpoint, CellMessage envelope)
    {
        _endpoint = requireNonNull(endpoint);
        _envelope = requireNonNull(envelope);
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
        notifyAll();
    }

    protected synchronized void send()
    {
        _envelope.revertDirection();
        _envelope.setMessageObject(_msg);
        _endpoint.sendMessage(_envelope);
        _envelope = null;
        _endpoint = null;
    }

    public synchronized Serializable take() throws InterruptedException
    {
        while (_msg == null) {
            wait();
        }

        return _msg;
    }
}
