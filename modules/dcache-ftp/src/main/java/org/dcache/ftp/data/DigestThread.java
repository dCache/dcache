package org.dcache.ftp.data;

import java.security.MessageDigest;

import org.dcache.pool.repository.RepositoryChannel;

public abstract class DigestThread extends Thread
{
    protected RepositoryChannel   _channel;
    protected MessageDigest _digest;
    protected BlockLog      _log;
    protected Exception     _error;
    protected long          _readahead;

    public DigestThread(RepositoryChannel channel, BlockLog log, MessageDigest digest)
    {
        _channel = channel;
        _log     = log;
        _digest  = digest;
        _readahead = Long.MAX_VALUE;
    }

    /**
     * Blocks until up to <code>position</code> bytes of the file have
     * been transferred, or the end of the file was reached.
     */
    protected void advance(long position)
            throws InterruptedException
    {
        _log.setLimit(_readahead == Long.MAX_VALUE
                              ? Long.MAX_VALUE
                              : position + _readahead);
        _log.waitCompleted(position);
    }

    /**
     * The read ahead limit specifies how much ahead of the digest
     * thread the transfer may advance. The point is to avoid that the
     * digest thread falls so far behind, that it has to read data
     * back from disk.
     *
     * If set to Long.MAX_VALUE, then there is no read ahead
     * limit. This is the default.
     */
    public void setReadAhead(long value)
    {
        _readahead = value;
    }

    public long getReadAhead()
    {
        return _readahead;
    }

    @Override
    public abstract void run();

    public Exception getLastError()
    {
        return _error;
    }
}
