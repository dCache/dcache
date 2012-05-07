package org.dcache.http;


import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.TimeoutCacheException;
import java.net.URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jboss.netty.buffer.HeapChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;
import static org.mockito.BDDMockito.*;

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


    HttpPoolRequestHandler _handler;
    HttpPoolNettyServer _executionServer;
    ChannelHandlerContext _context;
    Map<String,FileInfo> _files;
    List<Object> _additionalWrites;
    HttpResponse _response;

    @Before
    public void setup()
    {
        _context = mock(ChannelHandlerContext.class);
        _executionServer = mock(HttpPoolNettyServer.class);
        _handler = new HttpPoolRequestHandler(_executionServer);
        _files = Maps.newHashMap();
        _additionalWrites = new ArrayList<Object>();
    }


    @Test
    public void shouldGiveErrorIfRequestHasWrongUuid()
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedTransferOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/file?dcache-http-uuid="+ANOTHER_UUID));

        assertThat(_response.getStatus(), is(BAD_REQUEST));
    }


    @Ignore("it's the mover (which is mocked) that verifies path")
    @Test
    public void shouldGiveErrorIfRequestHasWrongPath()
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedTransferOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/another-file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(BAD_REQUEST));
    }


    @Test
    public void shouldDeliverCompleteFileIfReceivesRequestForWholeFile()
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedTransferOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(OK));
        assertThat(_response, hasHeader("Content-Length", "100"));
        assertThat(_response, not(hasHeader("Accept-Ranges")));
        assertThat(_response, not(hasHeader("Content-Range")));

        assertThat(_additionalWrites, hasSize(1));
        assertThat(_additionalWrites.get(0), isCompleteRead("/path/to/file"));
    }

    @Test
    public void shouldDeliverPartialFileIfReceivesRequestWithSingleRange()
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedTransferOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-499").
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader("Accept-Ranges", "bytes"));
        assertThat(_response, hasHeader("Content-Length", "500"));
        assertThat(_response, hasHeader("Content-Range", "bytes 0-499/1024"));

        assertThat(_additionalWrites, hasSize(1));
        assertThat(_additionalWrites.get(0),
                isPartialRead("/path/to/file", 0, 499));
    }

    @Test
    public void shouldDeliverAvailableDataIfReceivesRequestWithSingleRangeButTooBig()
    {
        givenPoolHas(file("/path/to/file").withSize(100));
        givenDoorHasOrganisedTransferOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-1024").
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader("Accept-Ranges", "bytes"));
        assertThat(_response, hasHeader("Content-Length", "100"));
        assertThat(_response, hasHeader("Content-Range", "bytes 0-99/100"));

        assertThat(_additionalWrites, hasSize(1));
        assertThat(_additionalWrites.get(0),
                isCompleteRead("/path/to/file"));
    }

    @Test
    public void shouldDeliverPartialFileIfReceivesRequestWithMultipleRanges()
    {
        givenPoolHas(file("/path/to/file").withSize(1024));
        givenDoorHasOrganisedTransferOf(file("/path/to/file").with(SOME_UUID));

        whenClientMakes(a(GET).withHeader("Range", "bytes=0-0,-1").
                forUri("/path/to/file?dcache-http-uuid="+SOME_UUID));

        assertThat(_response.getStatus(), is(PARTIAL_CONTENT));
        assertThat(_response, hasHeader("Accept-Ranges", "bytes"));
        assertThat(_response, hasHeader("Content-Type",
                "multipart/byteranges; boundary=\"__AAAAAAAAAAAAAAAA__\""));
        assertThat(_response, not(hasHeader("Content-Length")));
        assertThat(_response, not(hasHeader("Content-Range")));

        assertThat(_additionalWrites, hasSize(5));
        assertThat(_additionalWrites.get(0), isMultipart().
                emptyLine().
                line("--__AAAAAAAAAAAAAAAA__").
                line("Content-Length: 1").
                line("Content-Range: bytes 0-0/1024").
                emptyLine());
        assertThat(_additionalWrites.get(1),
                isPartialRead("/path/to/file", 0, 0));
        assertThat(_additionalWrites.get(2), isMultipart().
                emptyLine().
                line("--__AAAAAAAAAAAAAAAA__").
                line("Content-Length: 1").
                line("Content-Range: bytes 1023-1023/1024").
                emptyLine());
        assertThat(_additionalWrites.get(3),
                isPartialRead("/path/to/file", 1023, 1023));
        assertThat(_additionalWrites.get(4), isMultipart().
                emptyLine().
                line("--__AAAAAAAAAAAAAAAA__--"));
    }

    @Test
    public void shouldRejectDeleteRequests()
    {
        whenClientMakes(a(DELETE).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectConnectRequests()
    {
        whenClientMakes(a(CONNECT).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectHeadRequests()
    {
        whenClientMakes(a(HEAD).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectOptionsRequests()
    {
        whenClientMakes(a(OPTIONS).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectPatchRequests()
    {
        whenClientMakes(a(PATCH).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectPostRequests()
    {
        whenClientMakes(a(POST).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectPutRequests()
    {
        whenClientMakes(a(PUT).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    @Test
    public void shouldRejectTraceRequests()
    {
        whenClientMakes(a(TRACE).forUri("/path/to/file"));

        assertThat(_response.getStatus(), is(NOT_IMPLEMENTED));
        assertThat(_response, hasHeader("Connection", "close"));
    }

    private void givenPoolHas(FileInfo file)
    {
        _files.put(file.getPath(), file);
    }

    private void givenDoorHasOrganisedTransferOf(final FileInfo file)
    {
        String path = file.getPath();

        file.withSize(sizeOfFile(file));

        HttpProtocol_2 mover = mock(HttpProtocol_2.class);
        try {
            given(mover.getFileSize()).willReturn(file.getSize());
        } catch (IOException e) {
            throw new RuntimeException("Mock mover threw exception.", e);
        }

        try {
            given(mover.read(withPath(path))).willReturn(file.read());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Mock mover threw exception", e);
        } catch (IOException e) {
            throw new RuntimeException("Mock mover threw exception", e);
        } catch (TimeoutCacheException e) {
            throw new RuntimeException("Mock mover threw exception", e);
        }

        try {
            given(mover.read(withPath(path), anyLong(), anyLong())).willAnswer(
                    new Answer<ChunkedInput>() {
                        @Override
                        public ChunkedInput answer(InvocationOnMock invocation)
                                throws Throwable
                        {
                            Object[] args = invocation.getArguments();
                            return file.read((Long)args[1], (Long)args[2]);
                        }
                    });
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Mock mover threw exception", e);
        } catch (IOException e) {
            throw new RuntimeException("Mock mover threw exception", e);
        } catch (TimeoutCacheException e) {
            throw new RuntimeException("Mock mover threw exception", e);
        }

        given(_executionServer.getMover(file.getUuid())).willReturn(mover);
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

        public String getPath()
        {
            return _path;
        }

        public String getFileName()
        {
            return new FsPath(_path).getName();
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

        public ChunkedInput read()
        {
            return read(0, _size-1);
        }

        public ChunkedInput read(long lower, long upper)
        {
            return new SizeAwareChunkedInput(_path, _size, lower, upper);
        }
    }


    /**
     * The mocked mover will return this implementation of ChunkedInput.
     * This provides a convenient way of keeping track of how much of which
     * files have been read so that we can assert this.
     */
    private static class SizeAwareChunkedInput implements ChunkedInput
    {
        private final long _lower;
        private final long _upper;
        private final long _size;
        private final String _path;

        public SizeAwareChunkedInput(String path, long size, long lower,
                long upper)
        {
            _lower = lower;
            _upper = upper;
            _size = size;
            _path = path;
        }

        public boolean isCompleteRead()
        {
            return _lower == 0 && _upper == _size-1;
        }

        public long getLower()
        {
            return _lower;
        }

        public long getUpper()
        {
            return _upper;
        }

        public String getPath()
        {
            return _path;
        }

        @Override
        public boolean hasNextChunk() throws Exception
        {
            return false;
        }

        @Override
        public Object nextChunk() throws Exception
        {
            return null;
        }

        @Override
        public boolean isEndOfInput() throws Exception
        {
            return true;
        }

        @Override
        public void close() throws Exception
        {
            // ignored.
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
        private Map<String,List<String>> _headers =
                new HashMap<String,List<String>>();

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
            List<String> values = _headers.get(header);
            if( values == null) {
                values = new LinkedList<String>();
                _headers.put(header, values);
            }

            values.add(value);

            return this;
        }

        public RequestInfo forUri(String uri)
        {
            _uri = uri;
            return this;
        }

        public String getUri()
        {
            checkState(_uri != null, "URI has not been specified in test");
            return _uri;
        }

        public Set<Map.Entry<String,List<String>>> getHeaders()
        {
            return _headers.entrySet();
        }

        public Set<String> getHeaderNames()
        {
            return _headers.keySet();
        }

        public HttpVersion getProtocolVersion()
        {
            return _version;
        }

        public HttpMethod getMethod()
        {
            return _method;
        }
    }

    private void whenClientMakes(RequestInfo info)
    {
        Channel channel = mock(Channel.class);

        HttpRequest request = buildMockRequest(info);
        MessageEvent event = buildMockMessageEvent(channel, request);

        _handler.messageReceived(_context, event);

        ArgumentCaptor<Object> writes = ArgumentCaptor.forClass(Object.class);
        verify(channel, atLeast(1)).write(writes.capture());
        List<Object> allWrites = writes.getAllValues();

        _response = (HttpResponse) allWrites.get(0);
        _additionalWrites.addAll(allWrites.subList(1, allWrites.size()));
    }

    private HttpRequest buildMockRequest(RequestInfo info)
    {
        HttpRequest request = mock(HttpRequest.class);

        given(request.getMethod()).willReturn(info.getMethod());

        for(Map.Entry<String,List<String>> entry : info.getHeaders()) {
            String name = entry.getKey();
            List<String> values = entry.getValue();

            given(request.getHeaders(name)).willReturn(values);

            String lastValue = values.isEmpty() ? null :
                    values.get(values.size()-1);

            given(request.getHeader(name)).willReturn(lastValue);
        }

        given(request.getHeaderNames()).willReturn(info.getHeaderNames());
        given(request.getProtocolVersion()).
                willReturn(info.getProtocolVersion());
        given(request.getUri()).willReturn(info.getUri());

        return request;
    }

    private MessageEvent buildMockMessageEvent(Channel channel,
            HttpRequest request)
    {
        ChannelFuture future = mock(ChannelFuture.class);

        // TODO: make this more specific
        given(channel.write(any(ChunkedInput.class))).willReturn(future);

        MessageEvent event = mock(MessageEvent.class);
        given(event.getMessage()).willReturn(request);
        given(event.getChannel()).willReturn(channel);

        return event;
    }


    private static URI withPath(String path)
    {
        return argThat(new UriPathMatcher(path));
    }

    /**
     * A custom argument matcher that matches if the URI has a path
     * equal to the path supplied when constructing this object.
     */
    private static class UriPathMatcher extends ArgumentMatcher<URI>
    {
        private final String _path;

        public UriPathMatcher(String path)
        {
            checkArgument(path != null, "path cannot be null");
            _path = path;
        }

        @Override
        public boolean matches(Object uri) {
            return _path.equals(((URI) uri).getPath());
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

            if(!response.containsHeader(_name)) {
                return false;
            }

            if(_value != null) {
                List<String> values = response.getHeaders(_name);
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

    private static FileReadSizeMatcher isCompleteRead(String path)
    {
        return new FileReadSizeMatcher(path);
    }

    private static FileReadSizeMatcher isPartialRead(String path, long lower,
            long upper)
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

        private final boolean _isCompleteReadExpected;
        private final long _lower;
        private final long _upper;
        private final String _path;

        /**
         * Create a Matcher that matches only if the read was for the complete
         * contents of the specified file.
         */
        public FileReadSizeMatcher(String path)
        {
            _isCompleteReadExpected = true;
            _lower = _upper = DUMMY_VALUE;
            _path = path;
        }

        /**
         * Create a Matcher that matches only if the read was for part of
         * the contents of the specified file.
         */
        public FileReadSizeMatcher(String path, long lower, long upper)
        {
            _isCompleteReadExpected = false;
            _lower = lower;
            _upper = upper;
            _path = path;
        }

        @Override
        public boolean matches(Object o)
        {
            if(!(o instanceof SizeAwareChunkedInput)) {
                return false;
            }

            SizeAwareChunkedInput ci = (SizeAwareChunkedInput) o;

            if(!_path.equals(ci.getPath())) {
                return false;
            }

            if(_isCompleteReadExpected) {
                return ci.isCompleteRead();
            }

            return ci.getLower() == _lower && ci.getUpper() == _upper;
        }

        @Override
        public void describeTo(Description d)
        {
            if(_isCompleteReadExpected) {
                d.appendText("match a complete read");
            } else {
                d.appendText("match a partial read from ");
                d.appendValue(_lower);
                d.appendText(" to ");
                d.appendValue(_upper);
            }
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

        private List<String> _expectedLines = new ArrayList<String>();

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
            if(!(o instanceof HeapChannelBuffer)) {
                return false;
            }

            String rawData = new String(((HeapChannelBuffer)o).array());
            if(!rawData.endsWith(CRLF)) {
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
