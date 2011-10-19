package org.dcache.chimera.nfsv41.mover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.repository.Allocator;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import java.nio.channels.FileChannel;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.RepositoryChannel;

public class NFSv41ProtocolMover implements ManualMover {

    private final CellEndpoint _cell;
    private long _bytesTransferred = 0;
    private long _lastAccessTime = 0;
    private long _started = 0;

    private IoMode _ioMode = IoMode.READ;
    private static final Logger _log = LoggerFactory.getLogger(NFSv41ProtocolMover.class);


    public NFSv41ProtocolMover(CellEndpoint cell) {
        _cell = cell;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getBytesTransferred()
     */
	@Override
    public long getBytesTransferred() {
        return _bytesTransferred;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getLastTransferred()
     */
	@Override
    public long getLastTransferred() {
        return _lastAccessTime;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getTransferTime()
     */
	@Override
    public long getTransferTime() {

        if (_started == 0) {
            return 0;
        }
        return System.currentTimeMillis() - _started;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#runIO(RandomAccessFile raf, ProtocolInfo protocol,
     *      StorageInfo storage, PnfsId pnfsId,
     *      SpaceMonitor spaceMonitor, int access)
     */
	@Override
    public void runIO(RepositoryChannel raf, ProtocolInfo protocol,
            StorageInfo storage, PnfsId pnfsId,
            Allocator allocator, IoMode access)
            throws Exception {

            throw new RuntimeException("This mover it not suposed to run in legacy mode");
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.movers.MoverProtocol#wasChanged()
     */
	@Override
    public boolean wasChanged() {
        return  _ioMode == IoMode.WRITE && (getBytesTransferred() > 0);
    }


    /**
     * Set number of transfered bytes. The total transfered bytes count will
     * be increased. All negative values  ignored. The last access time is updated.
     *
     * @param bytesTransferred
     */
	@Override
    public void setBytesTransferred(long bytesTransferred) {

        if( bytesTransferred  < 0 ) return;

        _bytesTransferred += bytesTransferred;
        _lastAccessTime = System.currentTimeMillis();
        if(_started == 0) {
            _started = _lastAccessTime;
        }
    }

}
