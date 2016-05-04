package org.dcache.pinmanager.model;

import javax.annotation.concurrent.Immutable;

import java.util.Date;

import diskCacheV111.util.PnfsId;

@Immutable
public final class Pin
{
    public enum State
    {
        PINNING, PINNED, UNPINNING
    }

    private final long id;

    private final long uid;

    private final long gid;

    /** ID provided by the requestor eg. the SRM door. */
    private final String requestId;

    private final Date creationTime;

    private final Date expirationTime;

    private final PnfsId pnfsId;

    /** Name of pool on which the file is pinned. */
    private final String pool;

    /** Owner of sticky flag. */
    private final String sticky;

    private final State state;

    public Pin(long id)
    {
        this.id = id;
        this.creationTime = new Date();
        this.uid = 0;
        this.gid = 0;
        this.requestId = null;
        this.pnfsId = null;
        this.expirationTime = null;
        this.pool = null;
        this.state = null;
        this.sticky = null;
    }

    public Pin(long id, PnfsId pnfsid, String requestId,
               Date createdAt, Date expiresAt, long uid, long gid, State state, String pool, String sticky)
    {
        this.id = id;
        this.pnfsId = pnfsid;
        this.requestId = requestId;
        this.creationTime = createdAt;
        this.expirationTime = expiresAt;
        this.uid = uid;
        this.gid = gid;
        this.state = state;
        this.pool = pool;
        this.sticky = sticky;
    }

    public long getPinId()
    {
        return id;
    }

    public long getUid()
    {
        return uid;
    }

    public long getGid()
    {
        return gid;
    }

    public String getRequestId()
    {
        return requestId;
    }

    public Date getCreationTime()
    {
        return creationTime;
    }

    public Date getExpirationTime()
    {
        return expirationTime;
    }

    public PnfsId getPnfsId()
    {
        return pnfsId;
    }

    public String getPool()
    {
        return pool;
    }

    public String getSticky()
    {
        return sticky;
    }

    public State getState()
    {
        return state;
    }

    public boolean hasRemainingLifetimeLessThan(long lifetime)
    {
        long now = System.currentTimeMillis();
        return (expirationTime != null) &&
               (lifetime == -1 || expirationTime.before(new Date(now + lifetime)));
    }

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        s.append(String.format("[%d] %s", id, pnfsId));
        if (requestId != null) {
            s.append(" (").append(requestId).append(')');
        }
        s.append(" by ").append(uid).append(':').append(gid);
        s.append(String.format(" %tF %<tT", creationTime));
        if (expirationTime != null) {
            s.append(String.format(" to %tF %<tT", expirationTime));
        }
        s.append(" is ").append(state);
        if (pool != null) {
            s.append(" on ").append(pool).append(':').append(sticky);
        }
        return s.toString();
    }
}
