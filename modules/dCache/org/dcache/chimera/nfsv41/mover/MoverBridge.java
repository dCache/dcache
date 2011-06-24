package org.dcache.chimera.nfsv41.mover;

import java.nio.channels.FileChannel;
import diskCacheV111.util.PnfsId;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.repository.Allocator;

/**
 *
 * NFSv41 data server interface to dCache MoverProtocol connector.
 *
 * @since 1.9.3
 *
 */
class MoverBridge {

    private final ManualMover _mover;
    private final PnfsId _pnfsId;
    private final FileChannel _fileChannel;
    private final IoMode _ioMode;
    private final Allocator _allocator;
    private final stateid4 _stateid;
    private long _allocated;

    MoverBridge(ManualMover mover, PnfsId pnfsId, stateid4 stateid, FileChannel fileChannel,
            IoMode  ioMode, Allocator allocator) {

        _mover = mover;
        _pnfsId = pnfsId;
        _fileChannel = fileChannel;
        _ioMode = ioMode;
        _allocator = allocator;
        _stateid = stateid;
        _allocated = 0;
    }

    /**
     * @return the mover
     */
    public ManualMover getMover() {
        return _mover;
    }

    /**
     * @return the pnfsId
     */
    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    /**
     *
     * @return NFSv4 stateid associatd with the transfer
     */
    public stateid4 getStateid() {
        return _stateid;
    }

    /**
     * @return the file channel associated with mover
     */
    public FileChannel getFileChannel() {
        return _fileChannel;
    }

    /**
     * @return the ioMode
     */
    public IoMode getIoMode() {
        return _ioMode;
    }


    /**
     * @return space allocator used by mover
     */
    Allocator getAllocator() {
        return _allocator;
    }

    /**
     * Get size of allocated space by mover.
     *
     * @return size in bytes.
     */
    long getAllocated() {
        return _allocated;
    }

    /**
     * Set allocated space.
     *
     * @param allocated bytes.
     */
    void setAllocated(long allocated) {
        _allocated = allocated;
    }

}
