// $Id:  $
/*
 * CacheRepositoryEntryInfo.java
 *
 * Created on January 18, 2008, 7:47 PM
 *
 */

package diskCacheV111.repository;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.CacheException;
import org.dcache.pool.repository.CacheEntry;

/**
 *
 * @author timur
 */
public class CacheRepositoryEntryInfo implements java.io.Serializable {
    private static final long serialVersionUID = -4494188511917602601L;

    private static final int PRECIOUS_BIT=0;
    private static final int CACHED_BIT=1;
    private static final int RECEIVINGFROMCLIENT_BIT=2;
    private static final int RECEIVINGFROMSTORE_BIT=3;
    private static final int SENDINGTOSTORE_BIT=4;
    private static final int BAD_BIT=5;
    private static final int REMOVED_BIT=6;
    private static final int DESTROYED_BIT=7;
    private static final int STICKY_BIT=8;


    private PnfsId pnfsId;
    private  int bitmask;
    private long lastAccessTime;
    private long creationTime;
    private StorageInfo storageInfo;
    private long size;


    /**
     * Creates a new instance of CacheRepositoryEntryInfo
     */
    public CacheRepositoryEntryInfo() {
    }

    public CacheRepositoryEntryInfo(CacheEntry entry)
    {
        pnfsId = entry.getPnfsId();
        lastAccessTime = entry.getLastAccessTime();
        creationTime = entry.getCreationTime();
        storageInfo = entry.getFileAttributes().getStorageInfo();
        size = entry.getReplicaSize();
        switch (entry.getState()) {
        case PRECIOUS:
            setBit(PRECIOUS_BIT, true);
            break;
        case CACHED:
            setBit(CACHED_BIT, true);
            break;
        case FROM_CLIENT:
            setBit(RECEIVINGFROMCLIENT_BIT, true);
            break;
        case FROM_POOL:
        case FROM_STORE:
            setBit(RECEIVINGFROMSTORE_BIT, true);
            break;
        case BROKEN:
            setBit(BAD_BIT, true);
            break;
        case REMOVED:
            setBit(REMOVED_BIT, true);
            break;
        case NEW:
        case DESTROYED:
            throw new RuntimeException("Bug. An entry should never be in NEW or DESTROYED.");
        }
        setBit(STICKY_BIT, entry.isSticky());
    }

    private void setBit(int bitnum, boolean val) {
        if (val) {
            bitmask |= 1<<bitnum ;
        } else {
            bitmask &= ~(1 << bitnum);
        }
    }

    private boolean getBit(int bitnum) {
        return (bitmask & ( 1 <<bitnum)) != 0;
    }

    private String getBitMaskString() {
        StringBuilder sb = new StringBuilder();

        for(int i=31; i>=0; i--) {
            sb.append(getBit(i) ? '1':'0');
        }
        return sb.toString();
    }

    public int getBitMask() {
        return bitmask;
    }

    public boolean isPrecious()  {
        return getBit(PRECIOUS_BIT);
    }

    public boolean isCached() {
        return getBit(CACHED_BIT);
    }

    public boolean isReceivingFromClient() {
        return getBit(RECEIVINGFROMCLIENT_BIT);
    }

    public boolean isReceivingFromStore() {
        return getBit(RECEIVINGFROMSTORE_BIT);
    }

    public boolean isSendingToStore(){
        return getBit(SENDINGTOSTORE_BIT);
    }


    public boolean isBad() {
        return getBit(BAD_BIT);
    }


    public boolean isRemoved() {
        return getBit(REMOVED_BIT);
    }

    public boolean isDestroyed() {
        return getBit(DESTROYED_BIT);
    }

    public boolean isSticky()  {
        return getBit(STICKY_BIT);
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }


    public StorageInfo getStorageInfo()  {
        return storageInfo;

    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public long getSize() {
        return size;
    }

    public static void main(String[] args) {
        CacheRepositoryEntryInfo info = new CacheRepositoryEntryInfo();
        System.out.printf(" bitmask = %1$S\n",info.getBitMaskString());
        info.setBit(0,true);
        info.setBit(1,true);
        info.setBit(15,true);
        System.out.printf(" bitmask = %1$S\n",info.getBitMaskString());
        System.out.printf(" bit 0 = %1$b",info.getBit(0));
        System.out.printf(" bit 1 = %1$b",info.getBit(1));
        System.out.printf(" bit 3 = %1$b",info.getBit(3));
        System.out.printf(" bit 14 = %1$b",info.getBit(14));
        System.out.printf(" bit 15 = %1$b",info.getBit(15));
        System.out.printf(" bit 16 = %1$b",info.getBit(16));

    }


}
