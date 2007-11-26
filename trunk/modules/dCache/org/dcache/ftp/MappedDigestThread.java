package org.dcache.ftp;

import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.security.MessageDigest;

public class MappedDigestThread extends DigestThread
{
    public static final int MIN_SIZE = 1 << 20; // 1MB
    public static final int MAX_SIZE = 1 << 24; // 16MB

    public MappedDigestThread(FileChannel channel, BlockLog log, MessageDigest digest) 
    {
        super(channel, log, digest);
    }

    private long next(long position) 
        throws InterruptedException
    {
        advance(position + MIN_SIZE);
        return Math.min(_log.getCompleted() - position, MAX_SIZE);
    }

    public void run() 
    {
	try {
	    MappedByteBuffer map;
	    long position = 0;
	    long size;

	    size = next(position);
	    while (size > 0) {
		map = _channel.map(FileChannel.MapMode.READ_ONLY, position, size);
		_digest.update(map);
		position += size;
                size = next(position);
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
