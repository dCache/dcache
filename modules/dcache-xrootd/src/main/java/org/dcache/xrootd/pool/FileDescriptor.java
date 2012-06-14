package org.dcache.xrootd.pool;

import java.io.IOException;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

/**
 * Encapsulates an open file in the xrootd data server.
 */
public interface FileDescriptor
{
    /**
     * Returns a reader object for a given read request. The reader
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
    MoverChannel<XrootdProtocolInfo> getChannel();
}
