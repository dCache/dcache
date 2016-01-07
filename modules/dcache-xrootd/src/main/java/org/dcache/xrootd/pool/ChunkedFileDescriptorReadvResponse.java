package org.dcache.xrootd.pool;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.stream.AbstractChunkedReadvResponse;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileNotOpen;

public class ChunkedFileDescriptorReadvResponse extends AbstractChunkedReadvResponse
{
    private final List<FileDescriptor> descriptors;

    public ChunkedFileDescriptorReadvResponse(ReadVRequest request,
                                              int maxFrameSize,
                                              List<FileDescriptor> descriptors)
    {
        super(request, maxFrameSize);
        this.descriptors = descriptors;
    }

    @Override
    protected long getSize(int fd) throws IOException, XrootdException
    {
        if (fd < 0 || fd >= descriptors.size() || descriptors.get(fd) == null) {
            throw new XrootdException(kXR_FileNotOpen, "Invalid file descriptor");
        }
        return descriptors.get(fd).getChannel().size();
    }

    @Override
    protected ByteBuf read(ByteBufAllocator alloc, int fd, long position, int length)
            throws IOException, XrootdException
    {
        if (fd < 0 || fd >= descriptors.size() || descriptors.get(fd) == null) {
            throw new XrootdException(kXR_FileNotOpen, "Invalid file descriptor");
        }

        FileDescriptor descriptor = descriptors.get(fd);

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
