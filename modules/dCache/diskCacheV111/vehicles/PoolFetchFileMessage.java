// $Id: PoolFetchFileMessage.java,v 1.6 2006-04-10 12:19:03 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

/**
 * restore file from HSM
 */

public class PoolFetchFileMessage extends PoolMessage {

    private PnfsId _pnfsId;
    private StorageInfo _storageInfo;

    private static final long serialVersionUID = 1856537534158868883L;

    public PoolFetchFileMessage(String poolName, StorageInfo storageInfo,
            PnfsId pnfsId) {
        super(poolName);
        _pnfsId = pnfsId;
        _storageInfo = storageInfo;
        setReplyRequired(true);
    }

    public PoolFetchFileMessage(String poolName, StorageInfo storageInfo,
            String pnfsId) {
        super(poolName);
        _pnfsId = new PnfsId(pnfsId);
        _storageInfo = storageInfo;
        setReplyRequired(true);
    }

    public void setPnfsId(PnfsId pnfsId) {
        _pnfsId = pnfsId;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public void setStorageInfo(StorageInfo storageInfo) {
        _storageInfo = storageInfo;
    }

    public StorageInfo getStorageInfo() {
        return _storageInfo;
    }

}
