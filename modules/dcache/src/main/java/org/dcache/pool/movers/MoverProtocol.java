package org.dcache.pool.movers;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

public interface MoverProtocol
{

    /**
     * @param allocator Space allocator. May be null for a read-only
     * transfer.
     */
    void runIO(FileAttributes fileAttributes,
               RepositoryChannel diskFile,
               ProtocolInfo protocol,
               Allocator allocator,
               IoMode access)
        throws Exception;

    /**
     * Get number of bytes transfered. The number of bytes may exceed
     * total file size if client does some seek requests in between.
     *
     * @return number of bytes
     */
    long getBytesTransferred();

    /**
     * Get time between transfers begin and end. If Mover is sill
     * active, then current time used as end.
     *
     * @return transfer time in milliseconds.
     */
    long getTransferTime();

    /**
     * Get time of last transfer.
     *
     * @return last access time in milliseconds.
     */
    long getLastTransferred();
}
