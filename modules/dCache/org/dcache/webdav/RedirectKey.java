package org.dcache.webdav;

import diskCacheV111.util.PnfsId;

/**
 * Keys for the redirect table.
 */
public class RedirectKey
{
    public final PnfsId pnfsid;
    public final String pool;

    RedirectKey(PnfsId pnfsid, String pool)
    {
        if (pnfsid == null || pool == null) {
            throw new IllegalArgumentException("Null arguments are not allowed");
        }
        this.pnfsid = pnfsid;
        this.pool = pool;
    }

    @Override
    public int hashCode()
    {
        int a = pnfsid.hashCode();
        int b = pool.hashCode();
        return (a + b) * (a + b + 1) / 2 + b;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }

        if (!(o instanceof RedirectKey)) {
            return false;
        }
        RedirectKey other = (RedirectKey) o;
        return (other.pnfsid.equals(pnfsid) && other.pool.equals(pool));
    }
}
