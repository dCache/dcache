package org.dcache.http;

import static org.dcache.util.StringMarkup.percentEncode;
import static org.dcache.util.StringMarkup.quotedString;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.BYTES;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Multiset;
import com.google.common.collect.HashMultiset;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
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

import diskCacheV111.util.HttpByteRange;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.HttpProtocolInfo;
import org.dcache.pool.movers.MoverChannel;
import dmg.util.HttpException;

/**
 * HttpPoolRequestHandler - handle HTTP client - server communication.
 */
public class HttpPoolRequestHandler extends HttpRequestHandler
{
    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolRequestHandler.class);

    private static final String RANGE_SEPARATOR = "-";
    private static final String RANGE_PRE_TOTAL = "/";
    private static final String RANGE_SP = " ";
    private static final String BOUNDARY = "__AAAAAAAAAAAAAAAA__";
    private static final String MULTIPART_TYPE = "multipart/byteranges; boundary=\"" + HttpPoolRequestHandler.BOUNDARY + "\"";
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
    private final HttpPoolNettyServer _server;

    private final int _chunkSize;

    public HttpPoolRequestHandler(HttpPoolNettyServer server, int chunkSize) {
        _server = server;
        _chunkSize = chunkSize;
    }

    /**
     * Send a partial HTTP response, ranging from lower to upper
     * @param lower
     * @param upper
     * @return partial HttpResponse
     * @throws java.io.IOException
     */
    private static ChannelFuture sendPartialHeader(
            ChannelHandlerContext context,
            long lower,
            long upper,
            long total)
            throws IOException
    {
        HttpResponse response =
                new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);

        String contentRange = BYTES + RANGE_SP + lower + RANGE_SEPARATOR +
                upper + RANGE_PRE_TOTAL + total;

        response.setHeader(ACCEPT_RANGES, BYTES);
        response.setHeader(CONTENT_LENGTH, String.valueOf((upper - lower) + 1));
        response.setHeader(CONTENT_RANGE, contentRange);

        return context.getChannel().write(response);
    }

    private static ChannelFuture sendMultipartHeader(
            ChannelHandlerContext context)
            throws IOException
    {
        HttpResponse response =
                new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);

        response.setHeader(ACCEPT_RANGES, BYTES);
        response.setHeader(CONTENT_TYPE, MULTIPART_TYPE);

        return context.getChannel().write(response);
    }

    private static ChannelFuture sendMultipartFragment(
            ChannelHandlerContext context,
            long lower,
            long upper,
            long total)
            throws IOException
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append(CRLF);
        sb.append("--").append(BOUNDARY).append(CRLF);
        sb.append(CONTENT_LENGTH).append(": ").append((upper - lower) + 1).append(CRLF);
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

        ChannelBuffer buffer = ChannelBuffers
                .copiedBuffer(sb, CharsetUtil.UTF_8);
        return context.getChannel().write(buffer);
    }

    private static ChannelFuture sendMultipartEnd(ChannelHandlerContext context)
            throws IOException
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append(CRLF);
        sb.append("--").append(BOUNDARY).append("--").append(CRLF);

        ChannelBuffer buffer = ChannelBuffers.copiedBuffer(sb, CharsetUtil.UTF_8);
        return context.getChannel().write(buffer);
    }

    private static ChannelFuture sendGetResponse(ChannelHandlerContext context,
                                                 MoverChannel<HttpProtocolInfo> file)
            throws IOException
    {
        FsPath path = new FsPath(file.getProtocolInfo().getPath());

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setHeader(CONTENT_LENGTH, file.size());
        response.setHeader("Content-Disposition", contentDisposition(path
                .getName()));
        if (file.getProtocolInfo().getLocation() != null) {
            response.setHeader(CONTENT_LOCATION, file.getProtocolInfo().getLocation());
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

    @Override
    public void channelClosed(ChannelHandlerContext ctx,
                              ChannelStateEvent event)
    {
        _logger.debug("Called channelClosed.");
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
                _logger.info("Closing idling connection without opened files." +
                          " Connection has been idle for {} ms.", idleTime);
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
        MoverChannel<HttpProtocolInfo> file = null;

        try {
            file = open(request);

            ChunkedInput responseContent;
            long fileSize = file.size();

            List<HttpByteRange> ranges =
                parseHttpRange(request, 0, fileSize - 1);

            checkRequestPath(file.getProtocolInfo(), new URI(request.getUri()));

            if (ranges == null || ranges.isEmpty()) {
                /*
                 * GET for a whole file
                 */
                responseContent = read(file);
                future = sendGetResponse(context, file);
                future = event.getChannel().write(responseContent);
            } else if( ranges.size() == 1){
                /*
                 * GET for a single range
                 *
                 * rfc2616 is not strong enough about using
                 * multi-range reply for a single range:
                 *
                 *    The multipart/byteranges media type includes two
                 *    or more parts, each with its own Content-Type
                 *    and Content-Range fields. The required boundary
                 *    parameter specifies the boundary string used to
                 *    separate each body-part.
                 *
                 * To keep other'readings' happy, do not send
                 * multi-range reply on single range request.
                 */
                HttpByteRange range = ranges.get(0);
                future = sendPartialHeader(context,
                        range.getLower(),
                        range.getUpper(),
                        fileSize);
                responseContent = read(file,
                                       range.getLower(),
                                       range.getUpper());
                future = event.getChannel().write(responseContent);
            } else {
                /*
                 * GET for multiple ranges
                 */
                future = sendMultipartHeader(context);
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
            if (future == null) {
                future = sendHTTPError(context, HttpResponseStatus.valueOf(e.getErrorCode()), e.getMessage());
            } else {
                _logger.warn("Failure in HTTP GET: {}", e.getMessage());
                event.getChannel().close();
            }
        } catch (TimeoutCacheException e) {
            if (future == null) {
                future = sendHTTPError(context,
                                   REQUEST_TIMEOUT,
                                   e.getMessage());
            } else {
                _logger.warn("Failure in HTTP GET: {}", e.getMessage());
                event.getChannel().close();
            }
        } catch (IllegalArgumentException e) {
            if (future == null) {
                future = sendHTTPError(context, BAD_REQUEST, e.getMessage());
            } else {
                _logger.warn("Failure in HTTP GET: {}", e.getMessage());
                event.getChannel().close();
            }
        } catch (IOException e) {
            if (future == null) {
                future = sendHTTPError(context,
                        INTERNAL_SERVER_ERROR,
                        e.getMessage());
            } else {
                _logger.warn("Failure in HTTP GET: {}", e.getMessage());
                event.getChannel().close();
            }
        } catch (RuntimeException e) {
            if (future == null) {
                future = sendHTTPError(context,
                        INTERNAL_SERVER_ERROR,
                        e.getMessage());
            } else {
                _logger.warn("Failure in HTTP GET: {}", e.getMessage());
                event.getChannel().close();
            }
        } catch (URISyntaxException e) {
            if (future == null) {
                future = sendHTTPError(context, BAD_REQUEST,
                        "URI not valid: " + e.getMessage());
            } else {
                _logger.warn("Failure in HTTP GET: {}", e.getMessage());
                event.getChannel().close();
            }
        } finally {
            if (!isKeepAlive() && future != null) {
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
     * @return Mover channel for specified UUID
     * @throws IllegalArgumentException Request did not include UUID or no
     *         mover channel found for UUID in the request
     */
    private MoverChannel<HttpProtocolInfo> open(HttpRequest request)
        throws IllegalArgumentException
    {
        QueryStringDecoder queryStringDecoder =
            new QueryStringDecoder(request.getUri());

        Map<String, List<String>> params = queryStringDecoder.getParameters();

        if (!params.containsKey(HttpProtocol_2.UUID_QUERY_PARAM)) {
            if(!request.getUri().equals("/favicon.ico")) {
                _logger.error("Received request without UUID in the query " +
                        "string. Request-URI was {}", request.getUri());
            }

            throw new IllegalArgumentException("Query string does not include any UUID.");
        }

        List<String> uuidList = params.get(HttpProtocol_2.UUID_QUERY_PARAM);

        if (uuidList.size() < 1) {
            throw new IllegalArgumentException("UUID parameter does not include any value.");
        }

        UUID uuid = UUID.fromString(uuidList.get(0));
        MoverChannel<HttpProtocolInfo> channel = _server.open(uuid, false);
        if (channel == null) {
            throw new IllegalArgumentException("Request is no longer valid. " +
                                               "Please resubmit to door.");
        }
        _files.add(channel);

        return channel;
    }

    /**
     * Check whether the path in the request matches the protocol info
     * received from the door. This sanity check on the request is in
     * addition to the UUID.
     *
     * @param request The path requested by the client
     * @throws IllegalArgumentException path in request is illegal
     */
    private void checkRequestPath(HttpProtocolInfo protocolInfo, URI uri)
        throws IllegalArgumentException
    {
        FsPath requestedFile = new FsPath(uri.getPath());
        FsPath transferFile = new FsPath(protocolInfo.getPath());

        if (!requestedFile.equals(transferFile)) {
            _logger.warn("Received an illegal request for file {}, while serving {}",
                         requestedFile,
                         transferFile);
            throw new IllegalArgumentException("The file you specified does " +
                                               "not match the UUID you specified!");
        }
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
     * @throws IOException Accessing the file fails
     */
    private ChunkedInput read(MoverChannel<HttpProtocolInfo> file,
                              long lowerRange, long upperRange)
        throws IOException, TimeoutCacheException
    {
        /* need to count position 0 as well */
        long length = (upperRange - lowerRange) + 1;

        return new ReusableChunkedNioFile(file, lowerRange, length, _chunkSize);
    }

    /**
     * @see #read(MoverChannel<HttpProtocolInfo>, long, long)
     */
    private ChunkedInput read(MoverChannel<HttpProtocolInfo> file)
        throws IOException, TimeoutCacheException
    {
        return read(file, 0, file.size() - 1);
    }
}
