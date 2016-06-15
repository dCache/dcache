package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;

import static com.google.common.base.Preconditions.checkNotNull;

public class IoDoorEntry implements Serializable
{
    private final long _serialId;
    private final PnfsId _pnfsId;
    private String _pool;
    private final String _status;
    private final long _waitingSince;
    private final String _replyHost;
    private final Subject _subject;

    private static final long serialVersionUID = 7283617314269359997L;

    public IoDoorEntry(long serialId, PnfsId pnfsId, Subject subject,
                       String pool, String status,
                       long waitingSince, String replyHost)
    {
        _serialId = serialId;
        _pnfsId = pnfsId;
        _subject = subject;
        _pool = pool;
        _status = status;
        _waitingSince = waitingSince;
        _replyHost = checkNotNull(replyHost);
    }

    public long getSerialId()
    {
        return _serialId;
    }

    @Nullable
    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    @Nullable
    public String getPool()
    {
        return _pool;
    }

    @Nullable
    public String getStatus()
    {
        return _status;
    }

    /**
     *  Under certain conditions it is possible to receive
     *  transfer info sent before login has been performed
     *  by the door.  It is thus possible that the Subject
     *  also be <code>null</code>.
     */
    @Nullable
    public Subject getSubject()
    {
        return _subject;
    }

    public long getWaitingSince()
    {
        return _waitingSince;
    }

    @Nonnull
    public String getReplyHost()
    {
        return _replyHost;
    }

    public String toString()
    {
        return _serialId + ";" + _pnfsId + ";" + _subject + ";"
                        + _replyHost + ";" + _pool + ";" + _status + ";"
                        + (System.currentTimeMillis() - _waitingSince) + ";";
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_pool != null) {
            _pool = _pool.intern();
        }
    }
}