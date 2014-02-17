package org.dcache.ftp.data;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.dcache.pool.repository.RepositoryChannel;

public class DirectDigestThread extends DigestThread
{
    public static final int BLOCK_SIZE = 4096;  // 4 kb

    public DirectDigestThread(RepositoryChannel channel, BlockLog log, MessageDigest digest)
    {
        super(channel, log, digest);
    }

    @Override
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
