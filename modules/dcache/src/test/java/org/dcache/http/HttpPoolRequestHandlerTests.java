package org.dcache.http;

import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.python.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.HttpProtocolInfo;

import org.dcache.pool.movers.ChecksumChannel;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.BYTES;
import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.hamcrest.MockitoHamcrest.*;
import static org.mockito.Mockito.mock;

/**
 *  This class provides unit-tests for how the pool responses to HTTP requests
 */
public class HttpPoolRequestHandlerTests
{

    /* A constant UUID chosen to avoid using random data */
    private static final UUID SOME_UUID =
            UUID.fromString("49571502-60ca-49cd-bfe4-306bfe68037c");

    /* Just some UUID that is different from SOME_UUID */
    private static final UUID ANOTHER_UUID =
            UUID.fromString("f92e2faf-29d7-416c-9637-0ed7ba73fc36");

    private static final String DIGEST = "Digest";
    private static final String CONTENT_DISPOSITION = "Content-Disposition";

    private static final int SOME_CHUNK_SIZE = 4096;

    private HttpPoolRequestHandler _handler;
    private NettyTransferService<HttpProtocolInfo> _server;
    private Map<String,FileInfo> _files;
    private List<Object> _additionalWrites;
    private HttpResponse _response;
    private EmbeddedChannel _channel;

    @Before
    public void setup()
    {
        _server = mock(NettyTransferService.class);
        _handler = new HttpPoolRequestHandler(_server, SOME_CHUNK_SIZE);
        _channel = new EmbeddedChannel(_handler);
        _files = Maps.newHashMap();
        _additionalWrites = new ArrayList<>();
    }

    @Test
    public void shouldIncludeContentLengthForErrorResponse() throws Exception
    {
        whenClientMakes(a(OPTIONS).forUri("/path/to/file"));

        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    @Test
    public void shouldGiveErrorIfRequestHasWrongUuid() throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/file?dcache-http-uuid="+ANOTHER_UUID));

        assertThat(_response.getStatus(), is(BAD_REQUEST));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }


    @Ignore("it's the mover (which is mocked) that verifies path")
    @Test
    public void shouldGiveErrorIfRequestHasWrongPath() throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/another-file?dcache-http-uuid=" + SOME_UUID));

        assertThat(_response.getStatus(), is(BAD_REQUEST));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }


    @Test
    public void shouldDeliverCompleteFileIfReceivesRequestForWholeFile()
            throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "100"));
        assertThat(_response, hasHeader(CONTENT_DISPOSITION,
                "attachment;filename=file"));
        assertThat(_response, not(hasHeader(DIGEST)));
        assertThat(_response, hasHeader(ACCEPT_RANGES, BYTES));
        assertThat(_response, not(hasHeader(CONTENT_RANGE)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0), isCompleteRead("/path/to/file"));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverCompleteFileWithChecksumIfReceivesRequestForWholeFileWithChecksum()
            throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID).
                withAdler32("03da0195"));

        whenClientMakes(a(GET)
                .withHeader("Want-Digest", "adler32")
                .forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "100"));
        assertThat(_response, hasHeader(CONTENT_DISPOSITION,
                "attachment;filename=file"));
        assertThat(_response, hasHeader(DIGEST, "adler32=03da0195"));
        assertThat(_response, hasHeader(ACCEPT_RANGES, BYTES));
        assertThat(_response, not(hasHeader(CONTENT_RANGE)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0), isCompleteRead("/path/to/file"));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverCompleteFileIfReceivesRequestForWholeFileWithQuestionMark()
            throws Exception
    {
        givenPoolHas(file("/path/to/file?here").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file?here")
                .with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/file%3Fhere?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "100"));
        assertThat(_response, hasHeader(CONTENT_DISPOSITION,
                "attachment;filename=\"file?here\""));
        assertThat(_response, not(hasHeader(DIGEST)));
        assertThat(_response, hasHeader(ACCEPT_RANGES, BYTES));
        assertThat(_response, not(hasHeader(CONTENT_RANGE)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0),
                isCompleteRead("/path/to/file?here"));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverCompleteFileIfReceivesRequestForWholeFileWithBackslashQuote()
            throws Exception
    {
        givenPoolHas(file("/path/to/file\\\"here").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file\\\"here")
                .with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/file%5C%22here?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "100"));
        assertThat(_response, hasHeader(CONTENT_DISPOSITION,
                "attachment;filename=\"file\\\\\\\"here\""));
        assertThat(_response, not(hasHeader(DIGEST)));
        assertThat(_response, hasHeader(ACCEPT_RANGES, BYTES));
        assertThat(_response, not(hasHeader(CONTENT_RANGE)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0),
                isCompleteRead("/path/to/file\\\"here"));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverCompleteFileIfReceivesRequestForWholeFileWithNonAsciiName()
            throws Exception
    {
        //  0x16A0 0x16C7 0x16BB is the three-rune word from the start of Rune
        //  poem, available from http://www.ragweedforge.com/poems.html, in
        //  UTF-16.  The same word, in UTF-8, is represented by the 9-byte
        //  sequence 0xe1 0x9a 0xa0 0xe1 0x9b 0x87 0xe1 0x9a 0xbb.
        givenPoolHas(file("/path/to/\u16A0\u16C7\u16BB").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/\u16A0\u16C7\u16BB")
                .with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/%E1%9A%A0%E1%9B%87%E1%9A%BB?dcache-http-uuid="
                + SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "100"));
        assertThat(_response, hasHeader(CONTENT_DISPOSITION,
                "attachment;filename*=UTF-8''%E1%9A%A0%E1%9B%87%E1%9A%BB"));
        assertThat(_response, not(hasHeader(DIGEST)));
        assertThat(_response, hasHeader(ACCEPT_RANGES, BYTES));
        assertThat(_response, not(hasHeader(CONTENT_RANGE)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0),
                isCompleteRead("/path/to/\u16A0\u16C7\u16BB"));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }


    @Test
    public void shouldDeliverPartialFileIfReceivesRequestWithSingleRange()
            throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-499").
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader(ACCEPT_RANGES, "bytes"));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "500"));
        assertThat(_response, hasHeader(CONTENT_RANGE, "bytes 0-499/1024"));
        assertThat(_response, not(hasHeader(DIGEST)));
        assertThat(_response, not(hasHeader(CONTENT_DISPOSITION)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0),
                isPartialRead("/path/to/file", 0, 499));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverPartialFileIfReceivesRequestWithSingleRangeForFileWithChecksum()
            throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID).
                withAdler32("03da0195"));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-499")
                .withHeader("Want-Digest", "adler32")
                .forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader(ACCEPT_RANGES, "bytes"));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "500"));
        assertThat(_response, hasHeader(CONTENT_RANGE, "bytes 0-499/1024"));
        assertThat(_response, hasHeader(DIGEST, "adler32=03da0195"));
        assertThat(_response, not(hasHeader(CONTENT_DISPOSITION)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0),
                isPartialRead("/path/to/file", 0, 499));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverAvailableDataIfReceivesRequestWithSingleRangeButTooBig()
            throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-1024").
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader(ACCEPT_RANGES, "bytes"));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "100"));
        assertThat(_response, hasHeader(CONTENT_RANGE, "bytes 0-99/100"));
        assertThat(_response, not(hasHeader(DIGEST)));
        assertThat(_response, not(hasHeader(CONTENT_DISPOSITION)));

        assertThat(_additionalWrites, hasSize(2));
        assertThat(_additionalWrites.get(0),
                   isCompleteRead("/path/to/file"));
        assertThat(_additionalWrites.get(1), instanceOf(LastHttpContent.class));
    }

    @Test
    public void shouldDeliverPartialFileIfReceivesRequestWithMultipleRanges()
            throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID).
                withAdler32("03da0195"));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-0,-1")
                .withHeader("Want-Digest", "adler32")
                .forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader(ACCEPT_RANGES, "bytes"));
        assertThat(_response, hasHeader(CONTENT_TYPE,
                "multipart/byteranges; boundary=\"__AAAAAAAAAAAAAAAA__\""));
        assertThat(_response, hasHeader(DIGEST, "adler32=03da0195"));
        assertThat(_response, hasHeader(CONTENT_LENGTH, "154"));
        assertThat(_response, not(hasHeader(CONTENT_RANGE)));
        assertThat(_response, not(hasHeader(CONTENT_DISPOSITION)));

        assertThat(_additionalWrites, hasSize(5));
        assertThat(_additionalWrites.get(0), isMultipart().
                emptyLine().
                line("--__AAAAAAAAAAAAAAAA__").
                line("Content-Range: bytes 0-0/1024").
                emptyLine());
        assertThat(_additionalWrites.get(1),
                isPartialRead("/path/to/file", 0, 0));
        assertThat(_additionalWrites.get(2), isMultipart().
                emptyLine().
                line("--__AAAAAAAAAAAAAAAA__").
                line("Content-Range: bytes 1023-1023/1024").
                emptyLine());
        assertThat(_additionalWrites.get(3),
                isPartialRead("/path/to/file", 1023, 1023));
        assertThat(_additionalWrites.get(4), isMultipart().
                emptyLine().
                line("--__AAAAAAAAAAAAAAAA__--"));
    }

    @Test
    public void shouldRejectDeleteRequests() throws Exception
    {
        whenClientMakes(a(DELETE).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    @Test
    public void shouldRejectConnectRequests() throws Exception
    {
        whenClientMakes(a(CONNECT).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    @Test
    public void shouldAcceptHeadRequests() throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));
        whenClientMakes(a(HEAD)
                .forUri("/path/to/file?dcache-http-uuid=" + SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
        assertThat(_response, hasHeader(ACCEPT_RANGES, BYTES));
    }

    @Test
    public void shouldRejectOptionsRequests() throws Exception
    {
        whenClientMakes(a(OPTIONS).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    @Test
    public void shouldRejectPatchRequests() throws Exception
    {
        whenClientMakes(a(PATCH).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    @Test
    public void shouldRejectPostRequests() throws Exception
    {
        whenClientMakes(a(POST).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    @Test
    public void shouldAcceptPutRequests() throws Exception
    {
        givenDoorHasOrganisedWriteOf(file("/path/to/file").with(SOME_UUID));
        whenClientMakes(a(PUT)
                .forUri("/path/to/file?dcache-http-uuid=" + SOME_UUID));
        assertThat(_response.getStatus(), is(CREATED));
    }

    @Test
    public void shouldAcceptPutRequestWithAdlerWantDigest() throws Exception
    {
        givenDoorHasOrganisedWriteOf(file("/path/to/file").with(SOME_UUID));
        whenClientMakes(a(PUT)
                .withHeader("Want-Digest", "adler32")
                .withEntity("Hello, world")
                .forUri("/path/to/file?dcache-http-uuid=" + SOME_UUID));
        assertThat(_response.getStatus(), is(CREATED));
        assertThat(_response.headers().get("Digest"), equalTo("adler32=1bd40469"));
    }

    @Test
    public void shouldAcceptPutRequestWithMd5WantDigest() throws Exception
    {
        givenDoorHasOrganisedWriteOf(file("/path/to/file").with(SOME_UUID));
        whenClientMakes(a(PUT)
                .withHeader("Want-Digest", "md5")
                .withEntity("Hello, world")
                .forUri("/path/to/file?dcache-http-uuid=" + SOME_UUID));
        assertThat(_response.getStatus(), is(CREATED));
        assertThat(_response.headers().get("Digest"), equalTo("md5=vG5vFrigd+9fvI1Z0LkxuQ=="));
    }

    @Test
    public void shouldRejectPutOnRead() throws Exception
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedReadOf(file("/path/to/file").with(SOME_UUID));
        whenClientMakes(a(PUT)
                .forUri("/path/to/file?dcache-http-uuid=" + SOME_UUID));
        assertThat(_response.getStatus(), is(METHOD_NOT_ALLOWED));
    }

    @Test
    public void shouldRejectGetOnWrite() throws Exception
    {
        givenDoorHasOrganisedWriteOf(file("/path/to/file").with(SOME_UUID));
        whenClientMakes(a(GET)
                .forUri("/path/to/file?dcache-http-uuid=" + SOME_UUID));
        assertThat(_response.getStatus(), is(METHOD_NOT_ALLOWED));
    }

    @Test
    public void shouldRejectTraceRequests() throws Exception
    {
        whenClientMakes(a(TRACE).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader(CONTENT_LENGTH));
    }

    private void givenPoolHas(FileInfo file)
    {
        _files.put(file.getPath(), file);
    }

    private void givenDoorHasOrganisedReadOf(final FileInfo file)
            throws URISyntaxException, IOException
    {
        String path = file.getPath();

        file.withSize(sizeOfFile(file));

        NettyTransferService<HttpProtocolInfo>.NettyMoverChannel channel =
            mock(NettyTransferService.NettyMoverChannel.class);

        given(channel.size()).willReturn(file.getSize());

        given((Set<StandardOpenOption>)channel.getIoMode()).willReturn(FileStore.O_READ);
        given(channel.getProtocolInfo())
            .willReturn(new HttpProtocolInfo("Http", 1, 1,
                                             new InetSocketAddress((InetAddress) null, 0),
                                             null, null, path,
                                             new URI("http", "localhost", path, null)));
        given(channel.getFileAttributes()).willReturn(file.getFileAttributes());
        given(channel.release()).willReturn(Futures.immediateCheckedFuture(null));
        given(_server.openFile(eq(file.getUuid()), anyBoolean())).willReturn(channel);
    }

    private void givenDoorHasOrganisedWriteOf(final FileInfo file)
            throws URISyntaxException, IOException
    {
        String path = file.getPath();

        NettyTransferService<HttpProtocolInfo>.NettyMoverChannel channel =
                mock(NettyTransferService.NettyMoverChannel.class);
        given((Set<StandardOpenOption>)channel.getIoMode()).willReturn(FileStore.O_RW);
        given(channel.getProtocolInfo())
                .willReturn(new HttpProtocolInfo("Http", 1, 1,
                        new InetSocketAddress((InetAddress) null, 0),
                        null, null, path,
                        new URI("http", "localhost", path, null)));

        File tmpfile = File.createTempFile("http-pool-test", null);
        RepositoryChannel repoChannel = new FileRepositoryChannel(tmpfile.toPath(),
                EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE));
        ChecksumChannel checksums = new ChecksumChannel(repoChannel, EnumSet.noneOf(ChecksumType.class));

        Answer<Void> acceptChecksum = (i) -> {
                    checksums.addType(i.getArgument(0, ChecksumType.class));
                    return null;
                };
        Mockito.doAnswer(acceptChecksum).when(channel).addChecksumType(any());
        given(channel.write((ByteBuffer)any()))
                .willAnswer((i) -> {
                    ByteBuffer src = i.getArgument(0, ByteBuffer.class);
                    return checksums.write(src);
                });
        given(channel.write((ByteBuffer[])any()))
                .willAnswer((i) -> {
                    ByteBuffer[] src = i.getArgument(0, ByteBuffer[].class);
                    return checksums.write(src);
                });
        given(channel.write(any(), anyInt(), anyInt()))
                .willAnswer((i) -> {
                    ByteBuffer[] arg = i.getArgument(0, ByteBuffer[].class);
                    int offset = i.getArgument(1, Integer.class);
                    int length = i.getArgument(2, Integer.class);
                    return checksums.write(arg, offset, length);
                });


        given(channel.release()).willAnswer((i) -> {
                file.getFileAttributes().setChecksums(checksums.getChecksums());
                return Futures.immediateCheckedFuture(null);});

        given(channel.getFileAttributes()).willReturn(file.getFileAttributes());

        given(_server.openFile(eq(file.getUuid()), anyBoolean())).willReturn(channel);
    }

    private long sizeOfFile(FileInfo file)
    {
        checkState(_files.containsKey(file.getPath()),
                "missing file: " + file.getPath());
        return _files.get(file.getPath()).getSize();
    }

    private static FileInfo file(String path)
    {
        return new FileInfo(path);
    }

    /**
     * Information about some ficticious file in the pool's repository.
     * The methods allow declaration of information via chaining method calls.
     */
    private static class FileInfo
    {
        private final String _path;
        private long _size;
        private UUID _uuid;
        private FileAttributes _attributes = new FileAttributes();

        public FileInfo(String path)
        {
            _path = path;
        }

        public FileInfo withSize(long size)
        {
            _size = size;
            return this;
        }

        public FileInfo with(UUID uuid)
        {
            _uuid = uuid;
            return this;
        }

        public FileInfo withAdler32(String value)
        {
            Checksum checksum = new Checksum(ChecksumType.ADLER32, value);
            _attributes.setChecksums(Sets.newHashSet(checksum));
            return this;
        }

        public FileInfo withMD5(String value)
        {
            Checksum checksum = new Checksum(ChecksumType.MD5_TYPE, value);
            _attributes.setChecksums(Sets.newHashSet(checksum));
            return this;
        }

        public String getPath()
        {
            return _path;
        }

        public String getFileName()
        {
            return FsPath.create(_path).name();
        }

        public UUID getUuid()
        {
            checkState(_uuid != null, "uuid has not been defined");
            return _uuid;
        }

        public long getSize()
        {
            return _size;
        }

        public URI getUri()
        {
            return URI.create(_path);
        }

        public FileAttributes getFileAttributes()
        {
            return _attributes;
        }
    }

    private static RequestInfo a(HttpMethod method)
    {
        return new RequestInfo(method);
    }

    /**
     * Class to hold information about an incoming HTTP request.  Various
     * methods allow chaining of the declaration to add additional information.
     */
    private static class RequestInfo
    {
        private HttpMethod _method;
        private HttpVersion _version = HTTP_1_1;
        private String _uri;
        private Multimap<String,String> _headers = ArrayListMultimap.create();
        private byte[] _entity;

        public RequestInfo(HttpMethod type)
        {
            _method = type;
        }


        public RequestInfo using(HttpVersion version)
        {
            _version = version;
            return this;
        }

        public RequestInfo withHeader(String header, String value)
        {
            _headers.put(header, value);
            return this;
        }

        public RequestInfo forUri(String uri)
        {
            _uri = uri;
            return this;
        }

        public RequestInfo withEntity(String entity)
        {
            _entity = entity.getBytes(StandardCharsets.UTF_8);
            _headers.put("Content-Length", Integer.toString(_entity.length));
            return this;
        }

        private HttpRequest buildRequest()
        {
            checkState(_uri != null, "URI has not been specified in test");

            HttpRequest request = (_entity == null)
                    ? new DefaultFullHttpRequest(_version, _method, _uri)
                    : new DefaultHttpRequest(_version, _method, _uri);

            _headers.asMap().forEach((k,v) -> request.headers().set(k, Lists.newArrayList(v)));

            return request;
        }

        private Optional<LastHttpContent> buildContent()
        {
            return Optional.ofNullable(_entity)
                    .map(Unpooled::wrappedBuffer)
                    .map(DefaultLastHttpContent::new);
        }

        public void sendEventsTo(EmbeddedChannel channel)
        {
            channel.writeInbound(buildRequest());
            buildContent().ifPresent(channel::writeInbound);
        }
    }

    private void whenClientMakes(RequestInfo info) throws Exception
    {
        info.sendEventsTo(_channel);

        _response = (HttpResponse) _channel.readOutbound();
        _additionalWrites.addAll(_channel.outboundMessages());
        _channel.outboundMessages().clear();
        if (!(Iterables.getLast(_additionalWrites, _response) instanceof LastHttpContent)) {
            throw new RuntimeException("Reply lacks LastHttpContent.");
        }
    }


    private static HttpResponseHeaderMatcher hasHeader(String name,
            String value)
    {
        return new HttpResponseHeaderMatcher(name, value);
    }


    private static HttpResponseHeaderMatcher hasHeader(String name)
    {
        return new HttpResponseHeaderMatcher(name);
    }


    /**
     * A Matcher that checks whether an HttpResponse object contains the
     * header specified.  It either matches a header+value tuple or if the
     * response has at least one header irrespective of the value(s).
     */
    private static class HttpResponseHeaderMatcher extends
            BaseMatcher<HttpResponse>
    {
        private final String _name;
        private final String _value;

        /**
         * Create a Matcher that matches only if the response has the specified
         * header with specified value.
         */
        public HttpResponseHeaderMatcher(String name, String value)
        {
            _name = name;
            _value = value;
        }

        /**
         * Create a Matcher that matches if the response contains at least one
         * header of the specified type, irrespective of what value(s) the
         * header has.
         */
        public HttpResponseHeaderMatcher(String name)
        {
            this(name, null);
        }

        @Override
        public boolean matches(Object o)
        {
            if(!(o instanceof HttpResponse)) {
                return false;
            }

            HttpResponse response = (HttpResponse) o;

            if(!response.headers().contains(_name)) {
                return false;
            }

            if(_value != null) {
                List<String> values = response.headers().getAll(_name);
                return values.contains(_value);
            } else {
                return true;
            }
        }

        @Override
        public void describeTo(Description d)
        {
            if(_value == null) {
                d.appendText("At least one header '");
                d.appendText(_name);
                d.appendText("'");
            } else {
                d.appendText("Header '");
                d.appendText(_name);
                d.appendText("' with value '");
                d.appendText(_value);
                d.appendText("'");
            }
        }
    }

    private FileReadSizeMatcher isCompleteRead(String path)
    {
        return new FileReadSizeMatcher(path, 0, sizeOfFile(file(path)) - 1);
    }

    private FileReadSizeMatcher isPartialRead(String path,
                                              long lower, long upper)
    {
        return new FileReadSizeMatcher(path, lower, upper);
    }

    /**
     * This class provides a Matcher for assertThat statements.  It
     * checks whether one of the written objects is from a file and, if so,
     * whether it is all of that file or a partial read.
     */
    private static class FileReadSizeMatcher extends BaseMatcher<Object>
    {
        private static final long DUMMY_VALUE = -1;

        private final long _lower;
        private final long _upper;
        private final String _path;

        /**
         * Create a Matcher that matches only if the read was for part of
         * the contents of the specified file.
         */
        public FileReadSizeMatcher(String path, long lower, long upper)
        {
            _lower = lower;
            _upper = upper;
            _path = path;
        }

        @Override
        public boolean matches(Object o)
        {
            if(!(o instanceof ReusableChunkedNioFile)) {
                return false;
            }

            ReusableChunkedNioFile ci = (ReusableChunkedNioFile) o;

            NettyTransferService<HttpProtocolInfo>.NettyMoverChannel channel =
                    (NettyTransferService<HttpProtocolInfo>.NettyMoverChannel) ci.getChannel();

            if(!_path.equals(channel.getProtocolInfo().getPath())) {
                return false;
            }

            return ci.getOffset() == _lower && ci.getEndOffset() == _upper + 1;
        }

        @Override
        public void describeTo(Description d)
        {
            d.appendText("match a read from ");
            d.appendValue(_lower);
            d.appendText(" to ");
            d.appendValue(_upper);
        }
    }

    private MultipartMatcher isMultipart()
    {
        return new MultipartMatcher();
    }


    /**
     * This class provides a Matcher that matches if the supplied Object is
     * a HeapChannelBuffer containing lines separated by CR-LF 2-byte
     * sequences.  The sequence must end with a CR-LF combination.
     *
     * The matcher also checks the values of these lines.  The expected
     * lines are specified by successive calls to {@code #line} or
     * {@code #emptyLine}.  These calls may be chained.
     */
    private static class MultipartMatcher extends BaseMatcher<Object>
    {
        private static final String CRLF = "\r\n";

        private List<String> _expectedLines = new ArrayList<>();

        public MultipartMatcher emptyLine()
        {
            _expectedLines.add("");
            return this;
        }

        public MultipartMatcher line(String line)
        {
            _expectedLines.add(line);
            return this;
        }

        @Override
        public boolean matches(Object o)
        {
            if (o instanceof HttpContent) {
                o = ((HttpContent) o).content();
            }
            if(!(o instanceof ByteBuf)) {
                return false;
            }

            String rawData = ((ByteBuf) o).toString(CharsetUtil.UTF_8);
            if (!rawData.endsWith(CRLF)) {
                return false;
            }

            String data = rawData.substring(0, rawData.length()-CRLF.length());

            return Iterators.elementsEqual(_expectedLines.iterator(),
                    Splitter.on(CRLF).split(data).iterator());
        }

        @Override
        public void describeTo(Description d)
        {
            d.appendValueList("A multipart header with lines: ", ", ", ".",
                    _expectedLines);
        }
    }


}
