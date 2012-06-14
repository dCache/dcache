package org.dcache.http;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import com.google.common.collect.Multiset;
import com.google.common.collect.HashMultiset;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.HttpByteRange;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.HttpProtocolInfo;
import org.dcache.pool.movers.MoverChannel;
import dmg.util.HttpException;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * HttpPoolRequestHandler - handle HTTP client - server communication.
 */
public class HttpPoolRequestHandler extends HttpRequestHandler
{
    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolRequestHandler.class);

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
                FsPath path = new FsPath(file.getProtocolInfo().getPath());
                responseContent = read(file);
                future = sendHTTPFullHeader(context, event, fileSize, path.getName());
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
                future = sendHTTPPartialHeader(context, event,
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
                future = sendHTTPMultipartHeader(context, event);
                for(HttpByteRange range: ranges) {
                    responseContent = read(file,
                                           range.getLower(),
                                           range.getUpper());
                    sendHTTPMultipartFragment(context,
                                              event,
                                              range.getLower(),
                                              range.getUpper(),
                                              fileSize);
                    event.getChannel().write(responseContent);
                }
                future = sendHTTPMultipartEnd(context, event);
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
