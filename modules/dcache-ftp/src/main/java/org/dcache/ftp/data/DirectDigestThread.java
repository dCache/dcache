package org.dcache.ftp.data;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Map;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.ChecksumType;

public class DirectDigestThread extends DigestThread
{
    public static final int BLOCK_SIZE = 4096;  // 4 kb

    public DirectDigestThread(RepositoryChannel channel, BlockLog log, Map<ChecksumType, MessageDigest> digests)
    {
        super(channel, log, digests);
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
                _digests.values().forEach(d -> d.update((ByteBuffer) buffer.duplicate().flip()));
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
