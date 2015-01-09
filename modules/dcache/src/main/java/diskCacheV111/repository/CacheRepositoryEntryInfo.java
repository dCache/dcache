package diskCacheV111.repository;

import java.io.Serializable;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.CacheEntry;

public class CacheRepositoryEntryInfo implements Serializable {
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
    private long size;

    public CacheRepositoryEntryInfo(CacheEntry entry)
    {
        pnfsId = entry.getPnfsId();
        lastAccessTime = entry.getLastAccessTime();
        creationTime = entry.getCreationTime();
        size = entry.getReplicaSize();
        switch (entry.getState()) {
        case PRECIOUS:
            bitmask = 1 << PRECIOUS_BIT;
            break;
        case CACHED:
            bitmask = 1 << CACHED_BIT;
            break;
        case FROM_CLIENT:
            bitmask = 1 << RECEIVINGFROMCLIENT_BIT;
            break;
        case FROM_POOL:
        case FROM_STORE:
            bitmask = 1 << RECEIVINGFROMSTORE_BIT;
            break;
        case BROKEN:
            bitmask = 1 << BAD_BIT;
            break;
        case REMOVED:
            bitmask = 1 << REMOVED_BIT;
            break;
        case NEW:
        case DESTROYED:
            throw new RuntimeException("Bug. An entry should never be in NEW or DESTROYED.");
        }
        if (entry.isSticky()) {
            bitmask |= 1<< STICKY_BIT;
        }
    }

    private boolean getBit(int bitnum) {
        return (bitmask & (1 << bitnum)) != 0;
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

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public long getSize() {
        return size;
    }
}
