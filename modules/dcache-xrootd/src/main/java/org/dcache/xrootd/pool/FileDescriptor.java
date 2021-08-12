package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import org.dcache.pool.movers.NettyTransferService;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.XrootdResponse;
import org.dcache.xrootd.util.ByteBuffersProvider;

/**
 * Encapsulates an open file in the xrootd data server.
 */
public interface FileDescriptor
{
    /**
     * Reads data from the file. Reads until the buffer is full or the
     * end of file has been reached.
     *
     * @throws ClosedChannelException if the descriptor is closed.
     * @throws IOException if the operation failed.
     */
    void read(ByteBuffer buffer, long position) throws IOException;

    /**
     * Forces unwritten data to disk.
     *
     * @throws ClosedChannelException if the descriptor is closed.
     * @throws IOException if the operation failed.
     * @return the response, or <code>null</code> if the response is delayed.
     */
    XrootdResponse<SyncRequest> sync(SyncRequest msg)
        throws IOException, InterruptedException;

    /**
     * Writes data to the file.
     *
     * @throws ClosedChannelException if the descriptor is closed.
     * @throws IOException if the operation failed.
     */
    void write(ByteBuffersProvider msg)
        throws IOException;

    /**
     * Returns the FileChannel of this descriptor.
     */
    NettyTransferService<XrootdProtocolInfo>.NettyMoverChannel getChannel();

    /**
     * Whether the file was opened with kXR_posc.
     */
    boolean isPersistOnSuccessfulClose();
}
