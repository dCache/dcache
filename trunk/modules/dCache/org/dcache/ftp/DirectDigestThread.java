package org.dcache.ftp;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

public class DirectDigestThread extends DigestThread
{
    public static final int BLOCK_SIZE = 4096;  // 4 kb

    public DirectDigestThread(FileChannel channel, BlockLog log, MessageDigest digest) 
    {
        super(channel, log, digest);
    }

    public void run() 
    {
	try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
	    long position = 0;
            long read;

            advance(position + BLOCK_SIZE);
            read = _channel.read(buffer);
            while (read >= 0) {
                buffer.flip();
                _digest.update(buffer);
                position += read;

                buffer.clear();
                advance(position + BLOCK_SIZE);
                read = _channel.read(buffer);
            }
	} catch (Exception e) {
	    /* In theory we could attempt to resolve problems with the
	     * map() call by trying to map a smaller memory
	     * area. However, if we are so pressed in address space
	     * that we cannot even map MAX_SIZE memory, then we may be
	     * better of freeing some resources by killing the
	     * thread. Over time, it may be better to coordinate the
	     * computation of checksums from several transfer by using
	     * a commong thread pool, thus limiting the number of
	     * concurrent digest threads.
	     */
	    _error = e;
	}
    }
}
