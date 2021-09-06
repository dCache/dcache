package org.dcache.pool.movers;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.nio.file.OpenOption;
import java.util.List;
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
     * Get the number of bytes expected to be transferred, if known.  Returns
     * null if that value is unknown.
     */
    @Nullable
    default Long getBytesExpected()
    {
        return null;
    }

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

    /**
     * Provide a list of the IP address and port number of all currently active
     * TCP connections.  An empty list indicates that there is current no
     * established connections.  The mover may order the connections in some
     * protocol-specific fashion.  A mover that is unable to provide connection
     * information should return null.
     */
    @Nullable
    default List<InetSocketAddress> remoteConnections()
    {
        return null;
    }
}
