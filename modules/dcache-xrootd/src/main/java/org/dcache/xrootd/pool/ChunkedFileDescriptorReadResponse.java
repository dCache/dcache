package org.dcache.xrootd.pool;

import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.stream.AbstractChunkedReadResponse;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

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
    public ChannelBuffer read(long position, int length)
            throws IOException
    {
        ByteBuffer chunk = ByteBuffer.allocate(length);
        descriptor.read(chunk, position);
        chunk.flip();
        return wrappedBuffer(chunk);
    }
}
