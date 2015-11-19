package org.dcache.chimera.nfsv41.door.proxy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.VirtualFileSystem;

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
     * @return {@link ReadResult}
     * @throws IOException
     */
    ReadResult read(ByteBuffer dst, long position) throws IOException;

    /**
     * Writes a sequence of bytes to this channel from the given buffer,
     * starting at the given file position.
     *
     * @param src The buffer from which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; must
     * be non-negative
     * @return {@link VirtualFileSystem.WriteResult}
     * @throws IOException
     */
    VirtualFileSystem.WriteResult write(ByteBuffer src, long position) throws IOException;

    /**
     * Returns open-stateid associated with this proxy-io adapter.
     */
    stateid4 getStateId();

    // FIXME: move into generic NFS code
    class ReadResult {

        private final int bytesRead;
        private final boolean isEof;

        public ReadResult(int bytesRead, boolean isEof) {
            this.bytesRead = bytesRead;
            this.isEof = isEof;
        }

        /**
         * Get number of bytes read.
         * @return number of bytes
         */
        public int getBytesRead() {
            return bytesRead;
        }

        /**
         * Indicated is EOF reached
         * @return true, iff EOF reached.
         */
        public boolean isEof() {
            return isEof;
        }
    }
}
