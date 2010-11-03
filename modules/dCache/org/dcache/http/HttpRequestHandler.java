package org.dcache.http;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.HttpByteRange;

public class HttpRequestHandler extends SimpleChannelHandler
{
    private static final String RANGE_SEPARATOR = " - ";
    private static final String RANGE_PRE_TOTAL = "/";
    private static final String RANGE_SP = " ";
    private static final String RANGE_ASTERISK = "*";

    private final static Logger _logger =
        LoggerFactory.getLogger(HttpRequestHandler.class);
    private boolean _isKeepAlive;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        if (event.getMessage() instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) event.getMessage();

            String connection = request.getHeader(CONNECTION);

            _logger.debug("Received the following connection string: {}", connection);
            _isKeepAlive = (connection == null || !connection.equals("close"));

            if (request.getMethod() == HttpMethod.GET) {
                doOnGet(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.PUT) {
                doOnPut(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.POST) {
                doOnPost(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.DELETE) {
                doOnDelete(ctx, event, request);
            } else {
                unsupported(ctx, event);
            }
        }
    }

    protected void doOnGet(ChannelHandlerContext context, MessageEvent event,
                                    HttpRequest request) {
        _logger.info("Received a GET request, writing a default response.");
        unsupported(context, event);
    }

    protected void doOnPut(ChannelHandlerContext context, MessageEvent event,
                           HttpRequest request) {
        _logger.info("Received a PUT request, writing a default response.");
        unsupported(context, event);
    }

    protected void doOnPost(ChannelHandlerContext context, MessageEvent event,
                            HttpRequest request) {
        _logger.info("Received a POST request, writing default response.");
        unsupported(context, event);
    }

    protected void doOnDelete(ChannelHandlerContext context, MessageEvent event,
                              HttpRequest request) {
        _logger.info("Received a DELETE request, writing default response.");
        unsupported(context, event);
    }

    /**
     * Send an HTTP error with the given status code and message
     * @param context
     * @param event
     * @param statusCode The HTTP error code for the message
     * @param message Error message to be received by the client. Defaults to
     *        "An unexpected server error has occurred".
     * @return
     */
    protected ChannelFuture sendHTTPError(ChannelHandlerContext context,
                                          MessageEvent event,
                                          HttpResponseStatus statusCode,
                                          String message) {
        _logger.info("Sending error {} with message {} to client.", statusCode, message);

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);
        response.setHeader(CONNECTION, "close");

        if (message == null || message.isEmpty()) {
            message = "An unexpected server error has occurred.";
        }

        message = message + "\n";

        ChannelBuffer buffer = ChannelBuffers.buffer(message.length());
        buffer.writeBytes(message.getBytes());

        response.setContent(buffer);

        return event.getChannel().write(response);
    }

    /**
     * Send a simple HTTP 200 OK message without any further header fields
     * @param context
     * @param event
     * @return
     */
    protected ChannelFuture sendHTTPOK(ChannelHandlerContext context,
                                       MessageEvent event) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        return event.getChannel().write(response);
    }

    /**
     * Send a partial HTTP response, ranging from lower to upper
     * @param lower
     * @param upper
     * @return partial HttpResponse
     * @throws IOException
     */
    protected ChannelFuture sendHTTPPartialHeader(ChannelHandlerContext context,
                                                  MessageEvent event,
                                                  long lower,
                                                  long upper,
                                                  long total)
        throws IOException {
        HttpResponse response =
            new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);

        String contentRange = BYTES + RANGE_SP + lower + RANGE_SEPARATOR +
            upper + RANGE_PRE_TOTAL + total;

        response.setHeader(CONTENT_RANGE, contentRange);
        response.setHeader(CONTENT_LENGTH, String.valueOf(upper - lower));

        return event.getChannel().write(response);
    }

    protected ChannelFuture sendHTTPFullHeader(ChannelHandlerContext context,
                                                  MessageEvent event,
                                                  long contentLength) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setHeader(CONTENT_LENGTH, contentLength);

        return event.getChannel().write(response);
    }

    protected void unsupported(ChannelHandlerContext context, MessageEvent event)
    {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NOT_IMPLEMENTED);

        response.setHeader(CONNECTION, "close");
        ChannelFuture future = event.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event)
        throws Exception {
        event.getCause().printStackTrace();
        event.getChannel().close();
    }

    protected boolean isKeepAlive() {
        return _isKeepAlive;
    }

    /**
     * Parse the HTTPRanges in the request, from the Range Header.
     *
     * Return null if no range was found.
     * @param request
     * @param lowerRange, as imposed by the backing physical file
     * @param upperRange, as imposed by the backing physical file
     * @return First byte range that was parsed
     * @throws ParseException Range is illegal
     * @throws IOException Accessing diskFile failed
     */
    protected HttpByteRange parseHttpRange(HttpRequest request,
                                           long lowerRange,
                                           long upperRange)
        throws ParseException, IOException {
        String rangeHeader = request.getHeader(RANGE);

        HttpByteRange range = null;

        if (rangeHeader == null) {

            range = null;

        } else {

            try {
                List<HttpByteRange> ranges =
                    HttpByteRange.parseRanges(rangeHeader, lowerRange, upperRange);

                if (ranges == null || ranges.size() < 1) {
                } else {
                    range = ranges.get(0);
                }
            } catch (ParseException e) {
                /* ignore errors in the range, if the If-Range header is present */
                if (request.getHeader(IF_RANGE) == null) {
                    throw e;
                }
            }
        }

        return range;
    }

}
