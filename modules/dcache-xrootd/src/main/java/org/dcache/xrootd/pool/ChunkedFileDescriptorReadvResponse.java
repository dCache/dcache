package org.dcache.xrootd.pool;

import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.stream.AbstractChunkedReadvResponse;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileNotOpen;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

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
    protected ChannelBuffer read(int fd, long position, int length)
            throws IOException, XrootdException
    {
        if (fd < 0 || fd >= descriptors.size() || descriptors.get(fd) == null) {
            throw new XrootdException(kXR_FileNotOpen, "Invalid file descriptor");
        }

        FileDescriptor descriptor = descriptors.get(fd);
        byte[] chunkArray = new byte[length];
        ByteBuffer chunk = ByteBuffer.wrap(chunkArray);
        descriptor.read(chunk, position);
        return wrappedBuffer(chunkArray, 0, chunkArray.length - chunk.remaining());
    }
}
