package org.dcache.pinmanager;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

public class PinManagerCountPinsMessage extends Message {

    private static final long serialVersionUID = 7314454233774958888L;
    private final PnfsId _pnfsId;
    private final String _requestId;

    private int count;

    public PinManagerCountPinsMessage(PnfsId pnfsId) {
        _pnfsId = requireNonNull(pnfsId);
        _requestId = null;
    }

    public PinManagerCountPinsMessage(PnfsId pnfsId, String requestId) {
        _pnfsId = requireNonNull(pnfsId);
        _requestId = requestId;
    }

    public int getCount() {
        return count;
    }

    public String getRequestId() {
        return _requestId;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

}
