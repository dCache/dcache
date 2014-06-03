package org.dcache.http;

import com.google.common.base.CharMatcher;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.HttpByteRange;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.util.HttpException;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

import static java.util.Arrays.asList;
import static org.dcache.util.Checksums.TO_RFC3230;
import static org.dcache.http.HttpRequestHandler.CRLF;
import static org.dcache.util.StringMarkup.percentEncode;
import static org.dcache.util.StringMarkup.quotedString;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.BYTES;
import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

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

    private static final ChannelBuffer CONTINUE = ChannelBuffers.copiedBuffer(
            "HTTP/1.1 100 Continue\r\n\r\n", CharsetUtil.US_ASCII);

    /**
     * The mover channels that were opened.
     */
    private final Multiset<MoverChannel<HttpProtocolInfo>> _files =
        HashMultiset.create();

    /**
     * The server in the context of which this handler is executed
     */
    private final HttpPoolNettyServer _server;

    private final int _chunkSize;

    /**
     * The file being uploaded. Even though we only keep the file open
     * for the processing of a single HTTP message, that one message may
     * have been split into several chunks. Hence we have to keep a
     * reference to the file in between channel events.
     */
    private MoverChannel<HttpProtocolInfo> _writeChannel;

    public HttpPoolRequestHandler(HttpPoolNettyServer server, int chunkSize) {
        _server = server;
        _chunkSize = chunkSize;
    }

    /**
     * Send a partial HTTP response, ranging from lower to upper
     * @param lower
     * @param upper
     * @return partial HttpResponse
     */
    private static ChannelFuture sendPartialHeader(
            ChannelHandlerContext context,
            long lower,
            long upper,
            long total,
            String digest)
    {
        HttpResponse response =
                new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);

        String contentRange = BYTES + RANGE_SP + lower + RANGE_SEPARATOR +
                upper + RANGE_PRE_TOTAL + total;

        response.headers().add(ACCEPT_RANGES, BYTES);
        response.headers().add(CONTENT_LENGTH, String.valueOf((upper - lower) + 1));
        response.headers().add(CONTENT_RANGE, contentRange);
        if(!digest.isEmpty()) {
            response.headers().add(DIGEST, digest);
        }

        return context.getChannel().write(response);
    }

    private static ChannelFuture sendMultipartHeader(
            ChannelHandlerContext context, String digest, long totalBytes)
    {
        HttpResponse response =
                new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);

        response.headers().add(ACCEPT_RANGES, BYTES);
        response.headers().add(CONTENT_LENGTH, totalBytes);
        response.headers().add(CONTENT_TYPE, MULTIPART_TYPE);
        if(!digest.isEmpty()) {
            response.headers().add(DIGEST, digest);
        }
        return context.getChannel().write(response);
    }

    private static CharSequence generateMultipartFragmentMarker(long lower, long upper, long total, String boundary) {
        StringBuilder sb = new StringBuilder(64);
        sb.append(CRLF);
        sb.append(TWO_HYPHENS).append(boundary).append(CRLF);
        sb.append(CONTENT_RANGE).append(": ")
                .append(BYTES)
                .append(RANGE_SP)
                .append(lower)
                .append(RANGE_SEPARATOR)
                .append(upper)
                .append(RANGE_PRE_TOTAL)
                .append(total)
                .append(CRLF);
        sb.append(CRLF);
        return sb;
    }

    private static ChannelFuture sendMultipartFragment(
            ChannelHandlerContext context,
            long lower,
            long upper,
            long total)
    {

        CharSequence marker = generateMultipartFragmentMarker(lower, upper, total, BOUNDARY);
        ChannelBuffer buffer = ChannelBuffers
                .copiedBuffer(marker, CharsetUtil.UTF_8);
        return context.getChannel().write(buffer);
    }

    private static ChannelFuture sendMultipartEnd(ChannelHandlerContext context)
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append(CRLF);
        sb.append(TWO_HYPHENS).append(BOUNDARY).append(TWO_HYPHENS).append(CRLF);

        ChannelBuffer buffer = ChannelBuffers.copiedBuffer(sb, CharsetUtil.UTF_8);
        return context.getChannel().write(buffer);
    }

    private static ChannelFuture sendGetResponse(ChannelHandlerContext context,
                                                 MoverChannel<HttpProtocolInfo> file)
            throws IOException
    {
        FsPath path = new FsPath(file.getProtocolInfo().getPath());

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().add(ACCEPT_RANGES, BYTES);
        response.headers().add(CONTENT_LENGTH, file.size());
        String digest = buildDigest(file);
        if(!digest.isEmpty()) {
            response.headers().add(DIGEST, digest);
        }
        response.headers().add("Content-Disposition", contentDisposition(path
                .getName()));
        if (file.getProtocolInfo().getLocation() != null) {
            response.headers().add(CONTENT_LOCATION, file.getProtocolInfo().getLocation());
        }

        return context.getChannel().write(response);
    }

    private static ChannelFuture sendHeadResponse(ChannelHandlerContext context,
            MoverChannel<HttpProtocolInfo> file)
            throws IOException {
        FsPath path = new FsPath(file.getProtocolInfo().getPath());

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().add(ACCEPT_RANGES, BYTES);
        response.headers().add(CONTENT_LENGTH, file.size());
        String digest = buildDigest(file);
        if (!digest.isEmpty()) {
            response.headers().add(DIGEST, digest);
        }
        response.headers().add("Content-Disposition", contentDisposition(path
                .getName()));
        if (file.getProtocolInfo().getLocation() != null) {
            response.headers().add(CONTENT_LOCATION, file.getProtocolInfo().getLocation());
        }

        return context.getChannel().write(response);
    }

    private static String contentDisposition(String filename)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("attachment");

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
                throw new HttpException(NOT_IMPLEMENTED.getCode(),
                        headerName + " is not implemented");
            }
        }
    }

    private static ChannelFuture sendPutResponse(
            ChannelHandlerContext ctx,
            MoverChannel<HttpProtocolInfo> file)
            throws IOException
    {
        /* RFC 2616: 9.6. If a new resource is created, the origin server
        * MUST inform the user agent via the 201 (Created) response.
        */
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CREATED);

        /* RFC 2616: 10.2.2. The newly created resource can be referenced
        * by the URI(s) returned in the entity of the response, with the
        * most specific URI for the resource given by a Location header
        * field. The response SHOULD include an entity containing a list
        * of resource characteristics and location(s) from which the user
        * or user agent can choose the one most appropriate. The entity
        * format is specified by the media type given in the Content-Type
        * header field.
        */
        response.setContent(ChannelBuffers.copiedBuffer(
                String.valueOf(file.size()) + " bytes uploaded\r\n", CharsetUtil.UTF_8));
        setContentLength(response, response.getContent().readableBytes());
        setHeader(response, CONTENT_TYPE, "text/plain; charset=UTF-8");

        /* RFC 2616: 14.30. For 201 (Created) responses, the Location is
        * that of the new resource which was created by the request.
        */
        if (file.getProtocolInfo().getLocation() != null) {
            setHeader(response, LOCATION, file.getProtocolInfo().getLocation());
        }

        return ctx.getChannel().write(response);
    }

    private static ChannelFuture conditionalSendError(
            ChannelHandlerContext ctx,
            HttpMethod method,
            ChannelFuture future,
            HttpResponseStatus statusCode,
            String message)
    {
        if (future == null) {
            return sendHTTPError(ctx, statusCode, message);
        } else {
            _logger.warn("Failure in HTTP {}: {}", method, message);
            ctx.getChannel().close();
            return null;
        }
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
    {
        _logger.debug("HTTP connection from {} established", ctx.getChannel().getRemoteAddress());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx,
                              ChannelStateEvent event)
    {
        _logger.debug("HTTP connection from {} closed", ctx.getChannel().getRemoteAddress());
        for (MoverChannel<HttpProtocolInfo> file: _files) {
            _server.close(file);
        }
        _files.clear();
    }

    @Override
    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent event)
        throws Exception
    {
        if (event.getState() == IdleState.ALL_IDLE) {
            if (_logger.isInfoEnabled()) {
                long idleTime = System.currentTimeMillis() -
                    event.getLastActivityTimeMillis();
                _logger.info("Connection from {} has been idle for {} ms; disconnecting.",
                        ctx.getChannel().getRemoteAddress(), idleTime);
            }

            ctx.getChannel().close();
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
                           MessageEvent event,
                           HttpRequest request)
    {
        ChannelFuture future = null;
        MoverChannel<HttpProtocolInfo> file;

        try {
            file = open(request, false);

            if (file.getIoMode() != IoMode.READ) {
                throw new HttpException(METHOD_NOT_ALLOWED.getCode(),
                        "Resource is not open for reading");
            }

            long fileSize = file.size();
            List<HttpByteRange> ranges =
                    parseHttpRange(request, 0, fileSize - 1);

            ChunkedInput responseContent;
            if (ranges == null || ranges.isEmpty()) {
                /*
                 * GET for a whole file
                 */
                responseContent = read(file);
                future = sendGetResponse(context, file);
                future = event.getChannel().write(responseContent);
            } else if( ranges.size() == 1){
                /* RFC 2616: 14.16. A response to a request for a single range
                 * MUST NOT be sent using the multipart/byteranges media type.
                 */
                HttpByteRange range = ranges.get(0);
                future = sendPartialHeader(context,
                        range.getLower(),
                        range.getUpper(),
                        fileSize,
                        buildDigest(file));
                responseContent = read(file,
                                       range.getLower(),
                                       range.getUpper());
                future = event.getChannel().write(responseContent);
            } else {
                /*
                 * GET for multiple ranges
                 */

                /*
                 * calculate responce size
                 */
                long totalLen = 0;
                for (HttpByteRange range : ranges) {
                    long upper = range.getUpper();
                    long lower = range.getLower();
                    totalLen += upper - lower + 1;
                    CharSequence marker = generateMultipartFragmentMarker(lower, upper, fileSize, BOUNDARY);
                    totalLen += marker.length();
                }
                totalLen += TWO_HYPHENS.length()*2 + BOUNDARY.length() + CRLF.length()*2; // see sendMultipartEnd for details.

                future = sendMultipartHeader(context, buildDigest(file), totalLen);
                for(HttpByteRange range: ranges) {
                    responseContent = read(file,
                                           range.getLower(),
                                           range.getUpper());
                    sendMultipartFragment(context,
                            range.getLower(),
                            range.getUpper(),
                            fileSize);
                    event.getChannel().write(responseContent);
                }
                future = sendMultipartEnd(context);
            }
        } catch (HttpException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, HttpResponseStatus.valueOf(e.getErrorCode()),
                    e.getMessage());
        } catch (IOException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, e.getMessage());
        } catch (URISyntaxException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, "URI not valid: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, e.getMessage());
        } catch (RuntimeException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, INTERNAL_SERVER_ERROR, e.getMessage());
        } finally {
            if (future != null && !isKeepAlive()) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    protected void doOnPut(ChannelHandlerContext context, MessageEvent event,
                           HttpRequest request)
    {
        ChannelFuture future = null;
        MoverChannel<HttpProtocolInfo> file = null;
        Exception exception = null;

        try {
            checkContentHeader(request.getHeaderNames(), asList(CONTENT_LENGTH));

            file = open(request, true);

            if (file.getIoMode() != IoMode.WRITE) {
                throw new HttpException(METHOD_NOT_ALLOWED.getCode(),
                        "Resource is not open for writing");
            }

            if (request.isChunked()) {
                if (is100ContinueExpected(request)) {
                    context.getChannel().write(CONTINUE.duplicate());
                }
                _writeChannel = file;
                file = null;
            } else {
                long length = write(file, request.getContent());

                /* Check for incomplete data. We can assume the client already
                 * disconnected if this happens, however we must generate an
                 * exception to propagate the failure to billing.
                 *
                 * TODO: If we know the checksum or length a priori then we
                 * usually mark the file as broken when things don't match up,
                 * however there is no way to do this from inside a mover.
                 */
                if (getContentLength(request, length) != length) {
                    throw new HttpException(BAD_REQUEST.getCode(), "Incomplete entity");
                }

                future = sendPutResponse(context, file);
            }
        } catch (HttpException e) {
            exception = e;
            future = conditionalSendError(context, request.getMethod(),
                    future, HttpResponseStatus.valueOf(e.getErrorCode()), e.getMessage());
        } catch (IOException e) {
            exception = e;
            future = conditionalSendError(context, request.getMethod(),
                    future, INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (URISyntaxException e) {
            exception = e;
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, "URI is not valid: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            exception = e;
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, e.getMessage());
        } catch (RuntimeException e) {
            exception = e;
            future = conditionalSendError(context, request.getMethod(),
                    future, INTERNAL_SERVER_ERROR, e.getMessage());
        } finally {
            if (file != null) {
                close(file, exception);
            }
            if (future != null && (!isKeepAlive() || request.isChunked())) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    protected void doOnChunk(ChannelHandlerContext context, MessageEvent event,
                             HttpChunk chunk)
    {
        if (_writeChannel != null) {
            Exception exception = null;
            ChannelFuture future = null;
            try {
                write(_writeChannel, chunk.getContent());
                if (chunk.isLast()) {
                    if (chunk instanceof HttpChunkTrailer) {
                        checkContentHeader(((HttpChunkTrailer) chunk).getHeaderNames(),
                                asList(CONTENT_LENGTH));
                    }
                    future = sendPutResponse(context, _writeChannel);
                }
            } catch (IOException e) {
                exception = e;
                future = conditionalSendError(context, HttpMethod.PUT,
                        future, INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (HttpException e) {
                exception = e;
                future = conditionalSendError(context, HttpMethod.PUT,
                        future, HttpResponseStatus.valueOf(e.getErrorCode()), e.getMessage());
            } finally {
                if (chunk.isLast() || exception != null) {
                    close(_writeChannel, exception);
                    _writeChannel = null;
                }
                if (future != null && (!isKeepAlive() || !chunk.isLast())) {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
            }
        }
    }

    @Override
    protected void doOnHead(ChannelHandlerContext context,
            MessageEvent event,
            HttpRequest request) {
        ChannelFuture future = null;
        MoverChannel<HttpProtocolInfo> file;

        try {
            file = open(request, false);
            future = sendHeadResponse(context, file);
        } catch (IOException | IllegalArgumentException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, e.getMessage());
        } catch (URISyntaxException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, BAD_REQUEST, "URI not valid: " + e.getMessage());
        } catch (RuntimeException e) {
            future = conditionalSendError(context, request.getMethod(),
                    future, INTERNAL_SERVER_ERROR, e.getMessage());
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

        Map<String, List<String>> params = queryStringDecoder.getParameters();
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
        MoverChannel<HttpProtocolInfo> file = _server.open(uuid, exclusive);
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
        _server.close(channel, exception);
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

    /**
     * @see #read(MoverChannel<HttpProtocolInfo>, long, long)
     */
    private ChunkedInput read(MoverChannel<HttpProtocolInfo> file)
        throws IOException
    {
        return read(file, 0, file.size() - 1);
    }

    private long write(RepositoryChannel file, ChannelBuffer channelBuffer)
            throws IOException
    {
        long bytes = 0;
        for (ByteBuffer buffer: channelBuffer.toByteBuffers()) {
            while (buffer.hasRemaining()) {
                bytes += file.write(buffer);
            }
        }
        return bytes;
    }

    private static String buildDigest(MoverChannel<HttpProtocolInfo> file)
    {
        FileAttributes attributes = file.getFileAttributes();
        return attributes.getChecksumsIfPresent().transform(TO_RFC3230).or("");
    }
}
