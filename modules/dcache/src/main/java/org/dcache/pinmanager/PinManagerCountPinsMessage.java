package org.dcache.pinmanager;


import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;


import static com.google.common.base.Preconditions.checkNotNull;


public class PinManagerCountPinsMessage extends Message {


    private static final long serialVersionUID = 7314454233774958888L;
    private final PnfsId _pnfsId;

    private int count;


    public PinManagerCountPinsMessage(PnfsId pnfsId) {
        _pnfsId = checkNotNull(pnfsId);
    }


    public int getCount() {
        return count;
    }


    public void setCount(int count) {
        this.count = count;
    }


    public PnfsId getPnfsId() {
        return _pnfsId;
    }

}
