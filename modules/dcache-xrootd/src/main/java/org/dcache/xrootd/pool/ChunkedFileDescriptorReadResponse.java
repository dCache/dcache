package org.dcache.xrootd.pool;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.stream.AbstractChunkedReadResponse;

import static io.netty.buffer.Unpooled.wrappedBuffer;

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
        ByteBuffer chunk = ByteBuffer.allocate(length);
        descriptor.read(chunk, position);
        chunk.flip();
        return wrappedBuffer(chunk);
    }
}
