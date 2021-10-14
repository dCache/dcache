package diskCacheV111.vehicles.transferManager;

import diskCacheV111.vehicles.IoJobInfo;
import javax.annotation.Nullable;

/**
 * Message to query the transfermanager service to discover the current status of a transfer.
 */
public class TransferStatusQueryMessage extends TransferManagerMessage {

    private static final long serialVersionUID = 1L;

    private int _state;
    private IoJobInfo _info;
    private String pool;

    public TransferStatusQueryMessage(long id) {
        setId(id);
    }

    public void setMoverInfo(IoJobInfo info) {
        _info = info;
    }

    /**
     * Returns an IoJobInfo for the mover, or null if no mover is currently active.
     */
    public IoJobInfo getMoverInfo() {
        return _info;
    }

    public void setState(int state) {
        _state = state;
    }

    /**
     * Returns the current status of the request, as defined in the TransferManangerHandler static
     * field members.
     */
    public int getState() {
        return _state;
    }

    public void setPool(String name) {
        pool = name;
    }

    /**
     * Returns the name of the pool handling this transfer, if known.
     */
    @Nullable
    public String getPool() {
        return pool;
    }
}
