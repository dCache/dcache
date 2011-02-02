package org.dcache.pinmanager.model;

import javax.security.auth.Subject;
import java.util.Date;

import org.dcache.auth.Subjects;
import diskCacheV111.util.PnfsId;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class Pin
{
    public enum State
    {
        PINNING, PINNED, UNPINNING
    }

    @PrimaryKey
    protected long _id;

    @Persistent
    protected long _uid;

    @Persistent
    protected long _gid;

    /** ID provided by the requestor eg. the SRM door. */
    @Persistent
    protected String _requestId;

    @Persistent
    protected Date _creationTime;

    @Persistent
    protected Date _expirationTime;

    @Persistent
    protected String _pnfsId;

    /** Name of pool on which the file is pinned. */
    @Persistent
    protected String _pool;

    /** Owner of sticky flag. */
    @Persistent
    protected String _sticky;

    @Persistent
    protected State _state;

    protected Pin()
    {
    }

    public Pin(Subject subject, PnfsId pnfsId)
    {
        _uid = Subjects.getUid(subject);
        _gid = Subjects.getPrimaryGid(subject);
        _creationTime = new Date();
        _pnfsId = pnfsId.toString();
        _state = State.PINNING;
    }

    public long getPinId()
    {
        return _id;
    }

    public long getUid()
    {
        return _uid;
    }

    public long getGid()
    {
        return _gid;
    }

    public void setRequestId(String requestId)
    {
        _requestId = requestId;
    }

    public String getRequestId()
    {
        return _requestId;
    }

    public Date getCreationTime()
    {
        return _creationTime;
    }

    public Date getExpirationTime()
    {
        return _expirationTime;
    }

    public void setExpirationTime(Date date)
    {
        _expirationTime = date;
    }

    public PnfsId getPnfsId()
    {
        return new PnfsId(_pnfsId);
    }

    public String getPool()
    {
        return _pool;
    }

    public void setPool(String pool)
    {
        _pool = pool;
    }

    public String getSticky()
    {
        return _sticky;
    }

    public void setSticky(String sticky)
    {
        _sticky = sticky;
    }

    public State getState()
    {
        return _state;
    }

    public void setState(State state)
    {
        _state = state;
    }

    public boolean hasRemainingLifetimeLessThan(long lifetime)
    {
        long now = System.currentTimeMillis();
        return (_expirationTime != null) &&
            (lifetime == -1 || _expirationTime.before(new Date(now + lifetime)));
    }

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        s.append(String.format("[%d] %s", _id, _pnfsId));
        if (_requestId != null) {
            s.append(" (").append(_requestId).append(')');
        }
        s.append(" by ").append(_uid).append(':').append(_gid);
        s.append(String.format(" %tF %<tT", _creationTime));
        if (_expirationTime != null) {
            s.append(String.format(" to %tF %<tT", _expirationTime));
        }
        s.append(" is ").append(_state);
        if (_pool != null) {
            s.append(" on ").append(_pool).append(':').append(_sticky);
        }
        return s.toString();
    }
}