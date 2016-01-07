package org.dcache.xrootd.pool;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.stream.AbstractChunkedReadResponse;

public class ChunkedFileDescriptorReadResponse extends AbstractChunkedReadResponse
{
    private final FileDescriptor descriptor;

    public ChunkedFileDescriptorReadResponse(ReadRequest request,
                                             int maxFrameSize,
                                             FileDescriptor descriptor)
    {
        super(request, maxFrameSize);
        this.descriptor = descriptor;
    }

    @Override
    public ByteBuf read(ByteBufAllocator alloc, long position, int length)
            throws IOException
    {
        ByteBuf chunk = alloc.ioBuffer(length);
        try {
            ByteBuffer buffer = chunk.nioBuffer(0, length);
            descriptor.read(buffer, position);
            chunk.writerIndex(buffer.position());
            return chunk;
        } catch (RuntimeException | IOException e) {
            ReferenceCountUtil.release(chunk);
            throw e;
        }
    }
}
