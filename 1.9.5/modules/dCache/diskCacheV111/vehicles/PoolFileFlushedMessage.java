// $Id: PoolFileFlushedMessage.java,v 1.3 2007-07-10 20:57:35 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

/*
 * @Immutable
 */
public class PoolFileFlushedMessage extends PnfsMessage {
	
    private final StorageInfo _storageInfo;
    private final String _poolName;

    private static final long serialVersionUID = 1856537534158868883L;

    public PoolFileFlushedMessage(String poolName, PnfsId pnfsId, StorageInfo storageInfo ) {
    	super(pnfsId);
        _poolName = poolName;
        _storageInfo = storageInfo;
        setReplyRequired(true);
    }

    public PoolFileFlushedMessage(String poolName, StorageInfo storageInfo,
            String pnfsId) {
    	this(poolName, new PnfsId(pnfsId),storageInfo );
    }

    public StorageInfo getStorageInfo() {
        return _storageInfo;
    }
    
    public String getPoolName() {
    	return _poolName;
    }
}
