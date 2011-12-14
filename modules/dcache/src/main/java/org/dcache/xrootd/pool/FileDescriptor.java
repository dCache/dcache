package org.dcache.xrootd.pool;

import java.io.IOException;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

/**
 * Encapsulates an open file in the xrootd data server.
 */
public interface FileDescriptor
{
    /**
     * Closes the descriptor. A descriptor can only be closed
     * once. Once closed, the other operations throw
     * IllegalStateException.
     *
     * @throws IllegalStateException if the descriptor is already
     *              closed.
     */
    void close()
        throws IllegalStateException;

    /**
     * Returns a reader object for a given read request.The reader
     * provides read access to the file and can generate response
     * objects for the request.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    Reader read(ReadRequest msg)
        throws IllegalStateException;

    /**
     * Forces unwritten data to disk.
     *
     * @throws IllegalStateException if the descriptor is closed.
     * @throws IOException if the operation failed.
     */
    void sync(SyncRequest msg)
        throws IllegalStateException, IOException;

    /**
     * Writes data to the file.
     *
     * @throws IllegalStateException if the descriptor is closed.
     * @throws IOException if the operation failed.
     * @throws InterruptedException if preallocation on the pool fails
     */
    void write(WriteRequest msg)
        throws IllegalStateException, IOException, InterruptedException;

    /**
     * Returns the FileChannel of this descriptor.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    RepositoryChannel getChannel();

    /**
     * Get the mover associated with this file descriptor.
     * @return The mover that owns this file descriptor.
     */
    XrootdProtocol_3 getMover();
}
