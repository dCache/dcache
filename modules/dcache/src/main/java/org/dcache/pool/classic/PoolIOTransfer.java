package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.pool.movers.MoverProtocol;

import dmg.cells.nucleus.NoRouteToCellException;

import java.io.IOException;
import javax.security.auth.Subject;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.ReplicaDescriptor;

/**
 * Abstract bridge between repository and movers. PoolIOTransfer
 * exposes essential parameters of a mover, and encapsulates
 * information needed to complete a transfer.
 *
 * A transfer is divided into two phases: the data moving phase and a
 * cleanup phase. The data moving phase is interuptible during normal
 * operation, whereas cleanup should only be interrupted when pool
 * shutdown is imminent. The cleanup phase must be executed no matter
 * whether the data movement phase succeeded or has even been
 * attempted.
 *
 * PoolIOTransfer does not depent on the repository, but its
 * subclasses do.
 */
public abstract class PoolIOTransfer
{
    protected final MoverProtocol _mover;
    protected final PnfsId _pnfsId;
    protected final ProtocolInfo _protocolInfo;
    protected final StorageInfo _storageInfo;
    protected final Subject _subject;

    public PoolIOTransfer(PnfsId pnfsId,
                          ProtocolInfo protocolInfo,
                          Subject subject,
                          StorageInfo storageInfo,
                          MoverProtocol mover)
    {
        _pnfsId = pnfsId;
        _protocolInfo = protocolInfo;
        _subject = subject;
        _storageInfo = storageInfo;
        _mover = mover;
    }

    public StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public long getTransferTime()
    {
        return _mover.getTransferTime();
    }

    public long getBytesTransferred()
    {
        return _mover.getBytesTransferred();
    }

    public double getTransferRate()
    {
        double bt = _mover.getBytesTransferred();
        long tm = _mover.getTransferTime();
        return tm == 0L ? 0.0 : bt / tm;
    }

    public long getLastTransferred()
    {
        return _mover.getLastTransferred();
    }

    public MoverProtocol getMover() {
        return _mover;
    }

    @Override
        public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(_pnfsId);
        sb.append(" h={")
            .append(_mover.toString())
            .append("} bytes=").append(getBytesTransferred())
            .append(" time/sec=").append(getTransferTime() / 1000L)
            .append(" LM=");

        long lastTransferTime = getLastTransferred();
        if (lastTransferTime == 0L) {
            sb.append(0);
        } else {
            sb.append((System.currentTimeMillis() - lastTransferTime) / 1000L);
        }
        return sb.toString();
    }

    /**
     * Implements the data movement phase.
     */
    public abstract void transfer() throws Exception;

    /**
     * Implements the cleanup phase. Has to be called even when
     * <code>transfer</code> failed or was not invoked.
     */
    public abstract void close()
        throws CacheException, InterruptedException,
               IOException;

    public Subject getSubject()
    {
        return _subject;
    }
    /**
     * Returns the size of the replica that was transferred. Must not
     * be called before <code>close</code>.
     */
    public abstract long getFileSize();

    public abstract ReplicaDescriptor getIoHandle();

    public abstract IoMode getIoMode();
}
