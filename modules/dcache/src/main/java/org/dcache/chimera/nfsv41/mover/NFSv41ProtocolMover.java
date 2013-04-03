package org.dcache.chimera.nfsv41.mover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

public class NFSv41ProtocolMover implements ManualMover {

    private final CellEndpoint _cell;
    private long _bytesTransferred;
    private final long _started = System.currentTimeMillis();
    private long _lastAccessTime = _started;

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
    public void runIO(FileAttributes fileAttributes, RepositoryChannel raf, ProtocolInfo protocol,
            Allocator allocator, IoMode access)
            throws Exception {

            throw new RuntimeException("This mover it not suposed to run in legacy mode");
    }


    /**
     * Set number of transfered bytes. The total transfered bytes count will
     * be increased. All negative values  ignored. The last access time is updated.
     *
     * @param bytesTransferred
     */
	@Override
    public void setBytesTransferred(long bytesTransferred) {

        if( bytesTransferred  < 0 ) {
            return;
        }

        _bytesTransferred += bytesTransferred;
        _lastAccessTime = System.currentTimeMillis();
    }

}
