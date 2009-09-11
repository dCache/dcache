package org.dcache.xrootd2.pool;

import java.io.IOException;
import java.nio.channels.FileChannel;

import diskCacheV111.util.CacheException;

import org.dcache.xrootd2.protocol.messages.ReadRequest;
import org.dcache.xrootd2.protocol.messages.ReadVRequest;
import org.dcache.xrootd2.protocol.messages.SyncRequest;
import org.dcache.xrootd2.protocol.messages.WriteRequest;

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
     * @throws InterruptedException if the thread is interrupted while
     *              closing the descriptor.
     * @throws CacheException in case of errors during post
     *              processing.
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
     */
    void write(WriteRequest msg)
        throws IllegalStateException, IOException;

    /**
     * Returns the FileChannel of this descriptor. This break the
     * model and is a temporary hack to make vector read work.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    FileChannel getChannel();
}
