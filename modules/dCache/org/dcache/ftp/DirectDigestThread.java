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
            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
	    long position = 0;
            long read;

            advance(position + BLOCK_SIZE);
            read = _channel.read(buffer, position);
            while (read >= 0) {
                buffer.flip();
                _digest.update(buffer);
                position += read;

                buffer.clear();
                advance(position + BLOCK_SIZE);
                read = _channel.read(buffer, position);
            }
	} catch (Exception e) {
	    _error = e;
	}
    }
}
