package org.dcache.pool.movers;

import java.nio.file.OpenOption;
import java.util.Set;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

public interface MoverProtocol
{

    /**
     * Start mover
     */
    void runIO(FileAttributes fileAttributes,
               RepositoryChannel diskFile,
               ProtocolInfo protocol,
               Set<? extends OpenOption> access)
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
