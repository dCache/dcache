package org.dcache.http;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.HttpByteRange;

/**
 * HttpPoolRequestHandler - handle HTTP client - server communication and pass
 * on commands received from the client to the HTTP-mover.
 *
 * Similarly, write back responses received from the mover to the client.
 *
 * @author tzangerl
 *
 */
public class HttpPoolRequestHandler extends HttpRequestHandler
{

    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolRequestHandler.class);

    /** the mover that was accessed - will be used to inform the mover that it
     * does not need to wait for further messages for clients that send the
     * "Connection: close" header.
     */
    HttpProtocol_2 _mover;
    /**
     * The server in the context of which this handler is executed
     */
    HttpPoolNettyServer _executionServer;

    public HttpPoolRequestHandler(HttpPoolNettyServer executionServer) {
        _executionServer = executionServer;
    }

    /**
     * If no keep-alive, also tell the associated mover to shut down.
     *
     * @param ctx ChannelHandlerContext
     * @param event The event that caused the channel close
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent event)
        throws IOException {

        if (isKeepAlive()) {

            if (_mover != null) {
                _mover.keepAlive();
            }
        } else {
            _logger.debug("Client requested connection close, doing that.");

            if (_mover != null) {
                _mover.close();
            }
        }
    }

    /**
     * Single GET operation. Find the correct mover using the UUID in the GET
     * and access the file via the mover. Range queries are supported. The
     * resulting file will be sent to the remote peer in chunks to avoid
     * server side memory issues.
     *
     */
    @Override
    protected void doOnGet(ChannelHandlerContext context,
                           MessageEvent event,
                           HttpRequest request) {

        ChannelFuture future = null;

        try {
            HttpProtocol_2 mover = getMoverForRequest(request);
            _mover = mover;

            ChunkedInput responseContent;
            long fileSize = mover.getFileSize();

            HttpByteRange range = parseHttpRange(request,
                                                 0,
                                                 fileSize-1);

            if (range == null) {

                responseContent = mover.read(request.getUri());
                sendHTTPFullHeader(context, event, fileSize);

            } else {

                responseContent = mover.read(request.getUri(),
                                     range.getLower(),
                                     range.getUpper());
                sendHTTPPartialHeader(context,
                                      event,
                                      range.getLower(),
                                      range.getUpper(),
                                      fileSize);

            }

            future = event.getChannel().write(responseContent);

        } catch (java.text.ParseException pe) {
            future = sendHTTPError(context, event, BAD_REQUEST, pe.getMessage());
        } catch (IllegalArgumentException iaex) {
            future = sendHTTPError(context, event, BAD_REQUEST, iaex.getMessage());
        } catch (IOException ioexp) {
            future = sendHTTPError(context,
                                   event,
                                   INTERNAL_SERVER_ERROR,
                                   ioexp.getMessage());
        } catch (RuntimeException rtex) {
            future = sendHTTPError(context,
                                   event,
                                   INTERNAL_SERVER_ERROR,
                                   rtex.getMessage());
        } finally {
            if (future != null && !isKeepAlive()) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    /**
     * Get the mover for a certain HTTP request. The movers are identified by
     * UUIDs generated upon mover start and sent back to the door as a part
     * of the address info.
     *
     *
     * @param request HttpRequest that was sent by the client
     * @return HTTP-mover for specified UUID
     * @throws IllegalArgumentException Request did not include UUID or no
     *         mover found for UUID in the request
     */
    private HttpProtocol_2 getMoverForRequest(HttpRequest request)
        throws IllegalArgumentException {
        QueryStringDecoder queryStringDecoder =
            new QueryStringDecoder(request.getUri());

        Map<String, List<String>> params = queryStringDecoder.getParameters();

        if (!params.containsKey(HttpProtocol_2.UUID_QUERY_PARAM)) {
            _logger.error("Received request without UUID in the query " +
                          "string. Request-URI was {}", request.getUri());
            throw new IllegalArgumentException("Query string does not include any UUID.");
        }

        List<String> uuidList = params.get(HttpProtocol_2.UUID_QUERY_PARAM);

        if (uuidList.size() < 1) {
            throw new IllegalArgumentException("UUID parameter does not include any value.");
        }

        UUID uuid = UUID.fromString(uuidList.get(0));
        HttpProtocol_2 mover = _executionServer.getMover(uuid);

        if (mover == null) {
            throw new IllegalArgumentException("Mover for UUID " + uuid + " timed out. " +
                                               "Please send new request to door.");
        }

        return mover;
    }

    public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event)
        throws Exception {

        Throwable t = event.getCause();
        if (t instanceof ClosedChannelException) {
            _logger.info("Connection unexpectedly closed");
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else {
            _logger.warn(t.toString());
        }

    }
}
