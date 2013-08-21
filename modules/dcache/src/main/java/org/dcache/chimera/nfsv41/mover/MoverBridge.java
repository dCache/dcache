package org.dcache.chimera.nfsv41.mover;

import org.dcache.pool.repository.RepositoryChannel;

/**
 *
 * NFSv41 data server interface to dCache.
 *
 * @since 1.9.3
 *
 */
class MoverBridge {

    private final RepositoryChannel _fileChannel;
    private final NfsMover _mover;

    MoverBridge(NfsMover mover, RepositoryChannel fileChannel) {

        _mover = mover;
        _fileChannel = fileChannel;
    }

    /**
     * @return the mover
     */
    public NfsMover getMover() {
        return _mover;
    }

    /**
     * @return the file channel associated with mover
     */
    public RepositoryChannel getFileChannel() {
        return _fileChannel;
    }

}
