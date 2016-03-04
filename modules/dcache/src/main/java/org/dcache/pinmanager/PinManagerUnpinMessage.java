package org.dcache.pinmanager;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import static com.google.common.base.Preconditions.checkNotNull;

public class PinManagerUnpinMessage extends Message
{
    private static final long serialVersionUID = 5504172816234212007L;

    private Long _pinId;
    private String _requestId;
    private final PnfsId _pnfsId;

    public PinManagerUnpinMessage(PnfsId pnfsId)
    {
        checkNotNull(pnfsId);
        _pnfsId = pnfsId;
    }

    public PinManagerUnpinMessage(PnfsId pnfsId, long pinId)
    {
        this(pnfsId);
        _pinId = pinId;
    }

    public void setPinId(long pinId)
    {
        _pinId = pinId;
    }

    public Long getPinId()
    {
        return _pinId;
    }

    public void setRequestId(String requestId)
    {
        _requestId = requestId;
    }

    public String getRequestId()
    {
        return _requestId;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    @Override
    public String toString()
    {
        return "PinManagerUnpinMessage[" + _requestId + "," + _pinId + "," + _pnfsId + "]";
    }
}
