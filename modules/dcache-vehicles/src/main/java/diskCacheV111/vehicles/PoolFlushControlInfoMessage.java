package diskCacheV111.vehicles;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.StorageClassFlushInfo;

public abstract class PoolFlushControlInfoMessage extends PoolFlushControlMessage {

    private static final long serialVersionUID = 2092239799456859611L;

    private PoolCellInfo _info;
    private StorageClassFlushInfo[] _flushInfo;

    public PoolFlushControlInfoMessage(String poolName) {
        super(poolName);
        setReplyRequired(true);
    }

    public void setCellInfo(PoolCellInfo info) {
        _info = info;
    }

    public PoolCellInfo getCellInfo() {
        return _info;
    }

    public void setFlushInfos(StorageClassFlushInfo[] flushInfo) {
        _flushInfo = flushInfo;
    }

    public StorageClassFlushInfo[] getFlushInfos() {
        return _flushInfo;
    }
}




