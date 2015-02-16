package org.dcache.http;

import com.google.common.base.CharMatcher;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.HttpByteRange;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.util.HttpException;

import org.dcache.pool.movers.AbstractNettyTransferService;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.FileAttributes;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.BYTES;
import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.util.Arrays.asList;
import static org.dcache.util.Checksums.TO_RFC3230;
import static org.dcache.util.StringMarkup.percentEncode;
import static org.dcache.util.StringMarkup.quotedString;

/**
 * HttpPoolRequestHandler - handle HTTP client - server communication.
 */
public class HttpPoolRequestHandler extends HttpRequestHandler
{
    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolRequestHandler.class);

    private static final String DIGEST = "Digest";

    private static final String RANGE_SEPARATOR = "-";
    private static final String RANGE_PRE_TOTAL = "/";
    private static final String RANGE_SP = " ";
    private static final String BOUNDARY = "__AAAAAAAAAAAAAAAA__";
    private static final String MULTIPART_TYPE = "multipart/byteranges; boundary=\"" + HttpPoolRequestHandler.BOUNDARY + "\"";
    private static final String TWO_HYPHENS = "--";
    // See RFC 2045 for definition of 'tspecials'
    private static final CharMatcher TSPECIAL = CharMatcher.anyOf("()<>@,;:\\\"/[]?=");

    /**
     * The mover channels that were opened.
     */
    private final Multiset<MoverChannel<HttpProtocolInfo>> _files =
        HashMultiset.create();

    /**
     * The server in the context of which this handler is executed
     */
    private final AbstractNettyTransferService<HttpProtocolInfo> _server;

    private final int _chunkSize;

    /**
     * The file being uploaded. Even though we only keep the file open
     * for the processing of a single HTTP message, that one message may
     * have been split into several chunks. Hence we have to keep a
     * reference to the file in between channel events.
     */
    private MoverChannel<HttpProtocolInfo> _writeChannel;

    public HttpPoolRequestHandler(AbstractNettyTransferService<HttpProtocolInfo> server, int chunkSize)
    {
        _server = server;
        _chunkSize = chunkSize;
    }

    private static ByteBuf createMultipartFragmentMarker(long lower, long upper, long total)
    {
        return Unpooled.copiedBuffer(CRLF + TWO_HYPHENS + BOUNDARY + CRLF + CONTENT_RANGE + ": " +
                                     BYTES + RANGE_SP + lower + RANGE_SEPARATOR + upper + RANGE_PRE_TOTAL + total + CRLF + CRLF,
                                     StandardCharsets.UTF_8);
    }

    private static ByteBuf createMultipartEnd()
    {
        return Unpooled.copiedBuffer(CRLF + TWO_HYPHENS + BOUNDARY + TWO_HYPHENS + CRLF, StandardCharsets.UTF_8);
    }

    private static String contentDisposition(HttpProtocolInfo.Disposition disposition,
            String filename)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(disposition.toString().toLowerCase());
        appendDispositionParm(sb, "filename", filename);
        // REVISIT consider more info: creation-date, last-modified-date, size
        return sb.toString();
    }

    private static void appendDispositionParm(StringBuilder sb, String name, String value)
    {
        sb.append(';');

        // See RFC 2183 part 2. for description of when and how to encode
        if(value.length() > 78 || !CharMatcher.ASCII.matchesAllOf(value)) {
            appendUsingRfc2231Encoding(sb, name, "UTF-8", null, value);
        } else if(TSPECIAL.matchesAnyOf(value)) {
            appendAsQuotedString(sb, name, value);
        } else {
            sb.append(name).append("=").append(value);
        }
    }

    // RFC 822 defines quoted-string: a simple markup using backslash
    private static void appendAsQuotedString(StringBuilder sb, String name,
            String value)
    {
        sb.append(name).append("=");
        quotedString(sb, value);
    }

    private static void appendUsingRfc2231Encoding(StringBuilder sb, String name,
            String charSet, String language, String value)
    {
        sb.append(name).append("*=");
        if(charSet != null) {
            sb.append(charSet);
        }
        sb.append('\'');
        if(language != null) {
            sb.append(language);
        }
        sb.append('\'');
        percentEncode(sb, value);
    }

    /* RFC 2616: 9.6. The recipient of the entity MUST NOT ignore any
     * Content-* (e.g. Content-Range) headers that it does not
     * understand or implement and MUST return a 501 (Not Implemented)
     * response in such cases.
     */
    private static void checkContentHeader(Collection<String> headerNames,
                                           Collection<String> excludes)
            throws HttpException
    {
        outer: for (String headerName: headerNames) {
            if (headerName.toLowerCase().startsWith("content-")) {
                for (String exclude: excludes) {
                    if (exclude.equalsIgnoreCase(headerName)) {
                        continue outer;
                    }
                }
                throw new HttpException(NOT_IMPLEMENTED.code(),
                        headerName + " is not implemented");
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        _logger.debug("HTTP connection from {} established", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        _logger.debug("HTTP connection from {} closed", ctx.channel().remoteAddress());
        for (MoverChannel<HttpProtocolInfo> file: _files) {
            if (file == _writeChannel) {
                _server.closeFile(file, new FileCorruptedCacheException("Connection lost before end of file."));
            } else {
                _server.closeFile(file);
            }
        }
        _files.clear();
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t)
    {
        if (t instanceof ClosedChannelException) {
            _logger.info("Connection {} unexpectedly closed.", ctx.channel());
        } else if (t instanceof Exception) {
            for (MoverChannel<HttpProtocolInfo> file : _files) {
                CacheException cause;
                if (file == _writeChannel) {
                    cause = new FileCorruptedCacheException("Connection lost before end of file: " + t, t);
                } else {
                    cause = new CacheException(t.toString(), t);
                }
                _server.closeFile(file, cause);
            }
            _files.clear();
            ctx.channel().close();
        } else {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
            ctx.channel().close();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception
    {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent) event;
            if (idleStateEvent.state() == IdleState.ALL_IDLE) {
                if (_logger.isInfoEnabled()) {
                    _logger.info("Connection from {} id idle; disconnecting.",
                                 ctx.channel().remoteAddress());
                }
                ctx.close();
            }
        }
    }

    /**
     * Single GET operation.
     *
     * Finds the correct mover channel using the UUID in the
     * GET. Range queries are supported. The file will be sent to the
     * remote peer in chunks to avoid server side memory issues.
     */
    @Override
    protected void doOnGet(ChannelHandlerContext context,
                           HttpRequest request)
    {
        ChannelFuture future = null;
        MoverChannel<HttpProtocolInfo> file;
        List<HttpByteRange> ranges;
        long fileSize;

        try {
            file = open(request, false);

            if (file.getIoMode() != IoMode.READ) {
                throw new HttpException(METHOD_NOT_ALLOWED.code(),
                        "Resource is not open for reading");
            }

            fileSize = file.size();
            ranges = parseHttpRange(request, 0, fileSize - 1);
        } catch (HttpException e) {
            context.writeAndFlush(createErrorResponse(e.getErrorCode(), e.getMessage()));
            return;
        } catch (URISyntaxException e) {
            context.writeAndFlush(createErrorResponse(BAD_REQUEST, "URI not valid: " + e.getMessage()));
            return;
        } catch (IllegalArgumentException e) {
            context.writeAndFlush(createErrorResponse(BAD_REQUEST, e.getMessage()));
            return;
        } catch (IOException e) {
            context.writeAndFlush(createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
            return;
        }

        try {
            if (ranges == null || ranges.isEmpty()) {
                /*
                 * GET for a whole file
                 */
                future = context.write(new HttpGetResponse(fileSize, file));
                future = context.writeAndFlush(read(file, 0, fileSize - 1));
            } else if (ranges.size() == 1) {
                /* RFC 2616: 14.16. A response to a request for a single range
                 * MUST NOT be sent using the multipart/byteranges media type.
                 */
                HttpByteRange range = ranges.get(0);
                future = context.write(new HttpPartialContentResponse(range.getLower(), range.getUpper(),
                                                                      fileSize, buildDigest(file)));
                future = context.writeAndFlush(read(file, range.getLower(), range.getUpper()));
            } else {
                /*
                 * GET for multiple ranges
                 */

                long totalLen = 0;
                ByteBuf[] fragmentMarkers = new ByteBuf[ranges.size()];
                for (int i = 0; i < ranges.size(); i++) {
                    HttpByteRange range = ranges.get(i);
                    long upper = range.getUpper();
                    long lower = range.getLower();
                    totalLen += upper - lower + 1;

                    ByteBuf buffer = fragmentMarkers[i] = createMultipartFragmentMarker(lower, upper, fileSize);
                    totalLen += buffer.readableBytes();
                }
                ByteBuf endMarker = createMultipartEnd();
                totalLen += endMarker.readableBytes();

                future = context.write(new HttpMultipartResponse(buildDigest(file), totalLen));
                for (int i = 0; i < ranges.size(); i++) {
                    HttpByteRange range = ranges.get(i);
                    context.write(fragmentMarkers[i]);
                    context.write(read(file, range.getLower(), range.getUpper()));
                }
                future = context.writeAndFlush(endMarker);
            }
        } finally {
            if (future != null) {
                if (isKeepAlive()) {
                    future.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                } else {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
            }
        }
    }

    @Override
    protected void doOnPut(ChannelHandlerContext context, HttpRequest request)
    {
        ChannelFuture future = null;
        MoverChannel<HttpProtocolInfo> file = null;
        Exception exception = null;

        try {
            checkContentHeader(request.headers().names(), asList(CONTENT_LENGTH));

            file = open(request, true);

            if (file.getIoMode() != IoMode.WRITE) {
                throw new HttpException(METHOD_NOT_ALLOWED.code(),
                        "Resource is not open for writing");
            }

            if (is100ContinueExpected(request)) {
                context.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            }
            _writeChannel = file;
            file = null;
        } catch (HttpException e) {
            exception = e;
            future = context.writeAndFlush(
                    createErrorResponse(HttpResponseStatus.valueOf(e.getErrorCode()), e.getMessage()));
        } catch (URISyntaxException e) {
            exception = e;
            future = context.writeAndFlush(
                    createErrorResponse(BAD_REQUEST, "URI is not valid: " + e.getMessage()));
        } catch (IllegalArgumentException e) {
            exception = e;
            future = context.writeAndFlush(createErrorResponse(BAD_REQUEST, e.getMessage()));
        } catch (RuntimeException e) {
            exception = e;
            future = context.writeAndFlush(createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
        } finally {
            if (file != null) {
                close(file, exception);
            }
            if (future != null) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    protected void doOnContent(ChannelHandlerContext context, HttpContent content)
    {
        if (_writeChannel != null) {
            Exception exception = null;
            ChannelFuture future = null;
            try {
                ByteBuf data = content.content();
                while (data.isReadable()) {
                    data.readBytes(_writeChannel, data.readableBytes());
                }
                if (content instanceof LastHttpContent) {
                    checkContentHeader(((LastHttpContent) content).trailingHeaders().names(),
                                       asList(CONTENT_LENGTH));
                    future = context.writeAndFlush(new HttpPutResponse(_writeChannel));
                }
            } catch (IOException e) {
                exception = e;
                future = context.channel().writeAndFlush(createErrorResponse(INTERNAL_SERVER_ERROR,
                                                                             e.getMessage()));
            } catch (HttpException e) {
                exception = e;
                future = context.channel().writeAndFlush(
                        createErrorResponse(HttpResponseStatus.valueOf(e.getErrorCode()),
                                            e.getMessage()));
            } finally {
                if (content instanceof LastHttpContent || exception != null) {
                    close(_writeChannel, exception);
                    _writeChannel = null;
                }
                if (future != null && (!isKeepAlive() || exception != null)) {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
            }
        }
    }

    @Override
    protected void doOnHead(ChannelHandlerContext context, HttpRequest request) {
        ChannelFuture future = null;
        MoverChannel<HttpProtocolInfo> file;

        try {
            file = open(request, false);
            context.write(new HttpGetResponse(file.size(), file));
            future = context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        } catch (IOException | IllegalArgumentException e) {
            future = context.writeAndFlush(createErrorResponse(BAD_REQUEST, e.getMessage()));
        } catch (URISyntaxException e) {
            future = context.writeAndFlush(createErrorResponse(BAD_REQUEST,
                                                               "URI not valid: " + e.getMessage()));
        } catch (RuntimeException e) {
            future = context.writeAndFlush(createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
        } finally {
            if (future != null && !isKeepAlive()) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    /**
     * Get the mover channel for a certain HTTP request. The mover
     * channel is identified by UUID generated upon mover start and
     * sent back to the door as a part of the address info.
     *
     * @param request HttpRequest that was sent by the client
     * @param exclusive True if the mover channel exclusively is to be opened
     *                  in exclusive mode. False if the mover channel can be
     *                  shared with other requests.
     * @return Mover channel for specified UUID
     * @throws IllegalArgumentException Request did not include UUID or no
     *         mover channel found for UUID in the request
     */
    private MoverChannel<HttpProtocolInfo> open(HttpRequest request, boolean exclusive)
            throws IllegalArgumentException, URISyntaxException
    {
        QueryStringDecoder queryStringDecoder =
            new QueryStringDecoder(request.getUri());

        Map<String, List<String>> params = queryStringDecoder.parameters();
        if (!params.containsKey(HttpTransferService.UUID_QUERY_PARAM)) {
            if(!request.getUri().equals("/favicon.ico")) {
                _logger.error("Received request without UUID in the query " +
                        "string. Request-URI was {}", request.getUri());
            }

            throw new IllegalArgumentException("Query string does not include any UUID.");
        }

        List<String> uuidList = params.get(HttpTransferService.UUID_QUERY_PARAM);
        if (uuidList.isEmpty()) {
            throw new IllegalArgumentException("UUID parameter does not include any value.");
        }

        UUID uuid = UUID.fromString(uuidList.get(0));
        MoverChannel<HttpProtocolInfo> file = _server.openFile(uuid, exclusive);
        if (file == null) {
            throw new IllegalArgumentException("Request is no longer valid. " +
                                               "Please resubmit to door.");
        }

        URI uri = new URI(request.getUri());
        FsPath requestedFile = new FsPath(uri.getPath());
        FsPath transferFile = new FsPath(file.getProtocolInfo().getPath());

        if (!requestedFile.equals(transferFile)) {
            _logger.warn("Received an illegal request for file {}, while serving {}",
                    requestedFile,
                    transferFile);
            throw new IllegalArgumentException("The file you specified does " +
                    "not match the UUID you specified!");
        }

        _files.add(file);

        return file;
    }

    private void close(MoverChannel<HttpProtocolInfo> channel, Exception exception)
    {
        if (exception == null) {
            _server.closeFile(channel);
        } else {
            _server.closeFile(channel, exception);
        }
        _files.remove(channel);
    }

    /**
     * Read the resources requested in HTTP-request from the pool. Return a
     * ChunkedInput pointing to the requested portions of the file.
     *
     * Renew the keep-alive heartbeat, meaning that the last transferred time
     * will be updated, resetting the keep-alive timeout.
     *
     * @param file the mover channel to read from
     * @param lowerRange The lower delimiter of the requested byte range of the
     *                   file
     * @param upperRange The upper delimiter of the requested byte range of the
     *                   file
     * @return ChunkedInput View upon the file suitable for sending with
     *         netty and representing the requested parts.
     */
    private ChunkedInput read(MoverChannel<HttpProtocolInfo> file,
                              long lowerRange, long upperRange)
    {
        /* need to count position 0 as well */
        long length = (upperRange - lowerRange) + 1;

        return new ReusableChunkedNioFile(file, lowerRange, length, _chunkSize);
    }

    private static String buildDigest(MoverChannel<HttpProtocolInfo> file)
    {
        FileAttributes attributes = file.getFileAttributes();
        return attributes.getChecksumsIfPresent().transform(TO_RFC3230).or("");
    }

    private static class HttpGetResponse extends DefaultHttpResponse
    {
        public HttpGetResponse(long fileSize, MoverChannel<HttpProtocolInfo> file)
        {
            super(HTTP_1_1, OK);
            HttpProtocolInfo protocolInfo = file.getProtocolInfo();
            headers().add(ACCEPT_RANGES, BYTES);
            headers().add(CONTENT_LENGTH, fileSize);
            String digest = buildDigest(file);
            if(!digest.isEmpty()) {
                headers().add(DIGEST, digest);
            }
            headers().add("Content-Disposition",
                          contentDisposition(protocolInfo.getDisposition(),
                                             new FsPath(protocolInfo.getPath()).getName()));
            if (protocolInfo.getLocation() != null) {
                headers().add(CONTENT_LOCATION, protocolInfo.getLocation());
            }
        }
    }

    private static class HttpPartialContentResponse extends DefaultHttpResponse
    {
        public HttpPartialContentResponse(long lower,
                                          long upper,
                                          long total,
                                          String digest)
        {
            super(HTTP_1_1, PARTIAL_CONTENT);

            String contentRange = BYTES + RANGE_SP + lower + RANGE_SEPARATOR +
                                  upper + RANGE_PRE_TOTAL + total;

            headers().add(ACCEPT_RANGES, BYTES);
            headers().add(CONTENT_LENGTH, String.valueOf((upper - lower) + 1));
            headers().add(CONTENT_RANGE, contentRange);
            if (!digest.isEmpty()) {
                headers().add(DIGEST, digest);
            }
        }
    }

    private static class HttpMultipartResponse extends DefaultHttpResponse
    {
        public HttpMultipartResponse(String digest, long totalBytes)
        {
            super(HTTP_1_1, PARTIAL_CONTENT);
            headers().add(ACCEPT_RANGES, BYTES);
            headers().add(CONTENT_LENGTH, totalBytes);
            headers().add(CONTENT_TYPE, MULTIPART_TYPE);
            if(!digest.isEmpty()) {
                headers().add(DIGEST, digest);
            }
        }
    }

    private static class HttpPutResponse extends HttpTextResponse
    {
        public HttpPutResponse(MoverChannel<HttpProtocolInfo> file)
                throws IOException
        {
            /* RFC 2616: 9.6. If a new resource is created, the origin server
             * MUST inform the user agent via the 201 (Created) response.
             */

            /* RFC 2616: 10.2.2. The newly created resource can be referenced
             * by the URI(s) returned in the entity of the response, with the
             * most specific URI for the resource given by a Location header
             * field. The response SHOULD include an entity containing a list
             * of resource characteristics and location(s) from which the user
             * or user agent can choose the one most appropriate. The entity
             * format is specified by the media type given in the Content-Type
             * header field.
             */
            super(CREATED, file.size() + " bytes uploaded\r\n");

            /* RFC 2616: 14.30. For 201 (Created) responses, the Location is
            * that of the new resource which was created by the request.
            */
            if (file.getProtocolInfo().getLocation() != null) {
                headers().set(LOCATION, file.getProtocolInfo().getLocation());
            }

        }
    }
}
