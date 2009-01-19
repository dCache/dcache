package org.dcache.ftp;

import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.security.MessageDigest;

public class MappedDigestThread extends DigestThread
{
    public static final int MIN_SIZE = 1 << 20; // 1MB
    public static final int MAX_SIZE = 1 << 24; // 16MB
    public static final int GRANULARITY = MIN_SIZE; // A multiplum of page size

    public MappedDigestThread(FileChannel channel, BlockLog log, MessageDigest digest)
    {
        super(channel, log, digest);
    }

    /**
     * Returns the size of the next block to return relative to
     * <code>position</code>. The block will have a size between
     * MIN_SIZE and MAX_SIZE, except for possibly the last block in
     * the file.
     *
     * The size returned is always a multiplum of GRANULARITY, which
     * means that unless the file size is a multiplum of GRANULARITY,
     * the last (partial) block of the file will not be returned.
     *
     * This method blocks until enough data fitting these contraints
     * are available, or until the complete file has been received, at
     * which point zero may be returned.
     */
    private long next(long position)
        throws InterruptedException
    {
        advance(position + MIN_SIZE);
        return Math.min(_log.getCompleted() - position, MAX_SIZE)
            & ~(GRANULARITY - 1);
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

            /* Process remaining chunk.
             */
            size = _log.getCompleted() - position;
            map = _channel.map(FileChannel.MapMode.READ_ONLY, position, size);
            _digest.update(map);
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
