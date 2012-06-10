package org.dcache.xrootd.pool;

import com.google.common.collect.Lists;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd.protocol.messages.ReadResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VectorReaderTest
{
    private static final int SOME_ID = 1234;
    private static final int SOME_FH = 1;
    private static final int HEADER = 16;

    private List<FileDescriptor> _descriptors;
    private EmbeddedReadRequest[] _requests;

    @Before
    public void setUp() throws Exception
    {
        _descriptors = Lists.newArrayList();
        _requests = new EmbeddedReadRequest[0];
    }

    @Test
    public void shouldReturnItsStreamID() throws Exception
    {
        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);
        assertThat(reader.getStreamID(), is(SOME_ID));
    }

    @Test
    public void shouldReturnSingleResponseIfAllowedByMaxFrameSize()
            throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(10000);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(100).forLength(200);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(300).forLength(100);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);
        ReadResponse response1 = reader.read(1024);
        ReadResponse response2 = reader.read(1024);

        assertThat(response1.getDataLength(), is(HEADER + 200 + HEADER + 100));
        assertThat(response2, is(nullValue()));
    }

    @Test(expected=IllegalStateException.class)
    public void shouldFailReadsBiggerThanMaxFrameSize() throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(10000);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(100).forLength(2000);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);
        reader.read(1024);
    }

    @Test
    public void shouldRespectMaxFrameSize() throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(10000);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(100).forLength(100);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(300).forLength(1000);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);
        ReadResponse response1 = reader.read(1024);
        ReadResponse response2 = reader.read(1024);
        ReadResponse response3 = reader.read(1024);

        assertThat(response1.getDataLength(), is(HEADER + 100));
        assertThat(response2.getDataLength(), is(HEADER + 1000));
        assertThat(response3, is(nullValue()));
    }

    @Test
    public void shouldRespectEndOfFile() throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(10000);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(9700).forLength(500);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);
        ReadResponse response1 = reader.read(1024);
        ReadResponse response2 = reader.read(1024);

        assertThat(response1.getDataLength(), is(HEADER + 300));
        assertThat(response2, is(nullValue()));
    }

    @Test
    public void shouldUsePositionIndependentRead() throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(10000);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(100).forLength(100);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(200).forLength(100);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(400).forLength(1000);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(9700).forLength(1000);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);
        ReadResponse response1 = reader.read(1024);
        ReadResponse response2 = reader.read(1024);
        ReadResponse response3 = reader.read(1024);

        verify(channel(SOME_FH)).read(any(ByteBuffer.class), eq(100L));
        verify(channel(SOME_FH)).read(any(ByteBuffer.class), eq(200L));
        verify(channel(SOME_FH)).read(any(ByteBuffer.class), eq(400L));
        verify(channel(SOME_FH)).read(any(ByteBuffer.class), eq(9700L));
    }

    @Test
    public void shouldPackTruncatedReadsInSingleFrameIfPossible() throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(400);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(100).forLength(200);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(300).forLength(1000);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);

        ReadResponse response1 = reader.read(1024);
        ReadResponse response2 = reader.read(1024);

        assertThat(response1.getDataLength(), is(HEADER + 200 + HEADER + 100));
        assertThat(response2, is(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotOverflowWithLargeRequests() throws Exception
    {
        givenFileDescriptor().withFileHandle(SOME_FH).withSize(Integer.MAX_VALUE);
        givenReadRequest().forFileHandle(SOME_FH).atOffset(0).forLength(Integer.MAX_VALUE);

        VectorReader reader =
                new VectorReader(SOME_ID, _descriptors, _requests);

        ReadResponse response1 = reader.read(1024);
    }

    private FileDescriptorMaker givenFileDescriptor()
    {
        return new FileDescriptorMaker();
    }

    private ReadRequestMaker givenReadRequest()
    {
        int idx = _requests.length;
        _requests = Arrays.copyOf(_requests, _requests.length + 1);
        _requests[idx] = mock(EmbeddedReadRequest.class);
        return new ReadRequestMaker(_requests[idx]);
    }

    private RepositoryChannel channel(int fd)
    {
        return _descriptors.get(fd).getChannel();
    }

    /** A builder of FileDescriptor with a fluent interface. */
    private class FileDescriptorMaker
    {
        private final FileDescriptor fd = mock(FileDescriptor.class);
        private final RepositoryChannel channel = mock(RepositoryChannel.class);

        public FileDescriptorMaker() {
            when(fd.getChannel()).thenReturn(channel);
            when(fd.getMover()).thenReturn(mock(XrootdProtocol_3.class));
        }

        public FileDescriptorMaker withFileHandle(int fh) {
            while (fh >= _descriptors.size()) {
                _descriptors.add(null);
            }
            _descriptors.set(fh, fd);
            return this;
        }

        public FileDescriptorMaker withSize(final long length) throws IOException {
            when(channel.size()).thenReturn(length);
            when(channel.read(any(ByteBuffer.class), anyInt())).thenAnswer(new Answer() {
               public Object answer(InvocationOnMock invocation) {
                   Object[] args = invocation.getArguments();
                   ByteBuffer buffer = (ByteBuffer) args[0];
                   long position = (Long) args[1];

                   if (position >= length) {
                       return -1;
                   }

                   int actualRead = (int) Math.min(buffer.remaining(), length - position);
                   buffer.position(buffer.position() + actualRead);
                   return actualRead;
               }
            });
            return this;
        }
    }

    /** A builder of EmbeddedReadRequest with a fluent interface. */
    private static class ReadRequestMaker
    {
        private EmbeddedReadRequest _request;

        private ReadRequestMaker(EmbeddedReadRequest request) {
            _request = request;
        }

        public ReadRequestMaker forFileHandle(int fh) {
            when(_request.getFileHandle()).thenReturn(fh);
            return this;
        }

        public ReadRequestMaker forLength(int bytes) {
            when(_request.BytesToRead()).thenReturn(bytes);
            return this;
        }

        public ReadRequestMaker atOffset(long position) {
            when(_request.getOffset()).thenReturn(position);
            return this;
        }
    }
}
