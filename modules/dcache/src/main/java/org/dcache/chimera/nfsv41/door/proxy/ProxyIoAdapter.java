package org.dcache.chimera.nfsv41.door.proxy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public interface ProxyIoAdapter extends Closeable {

    /**
     * Reads a sequence of bytes from this channel into the given buffer,
     * starting at the given file position.
     *
     * @param dst The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; must
     * be non-negative
     * @return The number of bytes read, possibly zero, or -1 if the given
     * position is greater than or equal to the file's current size
     * @throws IOException
     */
    int read(ByteBuffer dst, long position) throws IOException;

    /**
     * Writes a sequence of bytes to this channel from the given buffer,
     * starting at the given file position.
     *
     * @param src The buffer from which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; must
     * be non-negative
     * @return The number of bytes written, possibly zero
     * @throws IOException
     */
    int write(ByteBuffer src, long position) throws IOException;

    /**
     * Returns the size of the file access by this adapter.
     * @return size of the files,  measured in bytes
     */
    long size();
}
