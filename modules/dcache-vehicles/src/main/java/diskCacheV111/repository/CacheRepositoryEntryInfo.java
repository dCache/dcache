package diskCacheV111.repository;

import diskCacheV111.util.PnfsId;
import java.io.Serializable;

public class CacheRepositoryEntryInfo implements Serializable {

    private static final long serialVersionUID = -4494188511917602601L;

    public static final int PRECIOUS_BIT = 0;
    public static final int CACHED_BIT = 1;
    public static final int RECEIVINGFROMCLIENT_BIT = 2;
    public static final int RECEIVINGFROMSTORE_BIT = 3;
    public static final int SENDINGTOSTORE_BIT = 4;
    public static final int BAD_BIT = 5;
    public static final int REMOVED_BIT = 6;
    public static final int DESTROYED_BIT = 7;
    public static final int STICKY_BIT = 8;


    private final PnfsId pnfsId;
    private final int bitmask;
    private final long lastAccessTime;
    private final long creationTime;
    private final long size;

    public CacheRepositoryEntryInfo(PnfsId pnfsId, int bitmask, long lastAccessTime,
          long creationTime, long size) {
        this.pnfsId = pnfsId;
        this.bitmask = bitmask;
        this.lastAccessTime = lastAccessTime;
        this.creationTime = creationTime;
        this.size = size;
    }

    private boolean getBit(int bitnum) {
        return (bitmask & (1 << bitnum)) != 0;
    }

    public int getBitMask() {
        return bitmask;
    }

    public boolean isPrecious() {
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

    public boolean isSticky() {
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

    public boolean isAvailable() {
        return isCached() || isPrecious();
    }
}
