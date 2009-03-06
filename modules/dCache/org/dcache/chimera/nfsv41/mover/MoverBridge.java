package org.dcache.chimera.nfsv41.mover;

import java.nio.channels.FileChannel;
import diskCacheV111.util.PnfsId;
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
    private final int _ioMode;
    private final Allocator _allocator;



    MoverBridge(ManualMover mover, PnfsId pnfsId, FileChannel fileChannel, int  ioMode, Allocator allocator) {

        _mover = mover;
        _pnfsId = pnfsId;
        _fileChannel = fileChannel;
        _ioMode = ioMode;
        _allocator = allocator;
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
     * @return the file channel associated with mover
     */
    public FileChannel getFileChannel() {
        return _fileChannel;
    }

    /**
     * @return the ioMode
     */
    public int getIoMode() {
        return _ioMode;
    }


    /**
     * @return space allocator used by mover
     */
    Allocator getAllocator() {
        return _allocator;
    }

}
