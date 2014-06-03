package org.dcache.http;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.List;

import diskCacheV111.util.HttpByteRange;

import dmg.util.HttpException;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpRequestHandler extends IdleStateAwareChannelHandler
{
    protected static final String CRLF = "\r\n";

    private final static Logger _logger =
        LoggerFactory.getLogger(HttpRequestHandler.class);
    private boolean _isKeepAlive;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        if (event.getMessage() instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) event.getMessage();

            _isKeepAlive = HttpHeaders.isKeepAlive(request);

            if (request.getMethod() == HttpMethod.GET) {
                doOnGet(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.PUT) {
                doOnPut(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.POST) {
                doOnPost(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.DELETE) {
                doOnDelete(ctx, event, request);
            } else if (request.getMethod() == HttpMethod.HEAD) {
                doOnHead(ctx, event, request);
            } else {
                unsupported(ctx, event);
            }
        } else if (event.getMessage() instanceof HttpChunk) {
            doOnChunk(ctx, event, (HttpChunk) event.getMessage());
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

    protected void doOnChunk(ChannelHandlerContext context, MessageEvent event,
                             HttpChunk chunk) {
        _logger.info("Received an HTTP chunk, writing default response.");
        unsupported(context, event);
    }

    protected void doOnHead(ChannelHandlerContext context, MessageEvent event,
           HttpRequest request) {
        _logger.info("Received a HEAD request, writing default response.");
        unsupported(context, event);
    }

    /**
     * Send an HTTP error with the given status code and message.
     *
     * @param context
     * @param statusCode The HTTP error code for the message
     * @param message Error message to be received by the client. Defaults to
     *        "An unexpected server error has occurred".
     * @return
     */
    protected static ChannelFuture sendHTTPError(
            ChannelHandlerContext context,
            HttpResponseStatus statusCode,
            String message)
    {
        _logger.info("Sending error {} with message {} to client.",
                statusCode, message);

        if (message == null || message.isEmpty()) {
            message = "An unexpected server error has occurred.";
        }

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);
        response.headers().add(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setContent(ChannelBuffers.copiedBuffer(
                message + CRLF, CharsetUtil.UTF_8));
        setContentLength(response, response.getContent().readableBytes());

        return context.getChannel().write(response);
    }

    /**
     * Send an HTTP error with the given status code and message. The
     * connection will be closed after the error is sent.
     *
     * @param context
     * @param statusCode The HTTP error code for the message
     * @param message Error message to be received by the client. Defaults to
     *        "An unexpected server error has occurred".
     * @return
     */
    protected static ChannelFuture sendFatalError(
            ChannelHandlerContext context,
            HttpResponseStatus statusCode,
            String message)
    {
        _logger.info("Sending error {} with message {} to client.",
                statusCode, message);

        if (message == null || message.isEmpty()) {
            message = "An unexpected server error has occurred.";
        }

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);
        response.headers().add(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.headers().add(CONNECTION, CLOSE);
        response.setContent(ChannelBuffers.copiedBuffer(
                message + CRLF, CharsetUtil.UTF_8));
        setContentLength(response, response.getContent().readableBytes());

        ChannelFuture future = context.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
        return future;
    }

    protected static ChannelFuture unsupported(
            ChannelHandlerContext context, MessageEvent event)
    {
        return sendHTTPError(context, NOT_IMPLEMENTED,
                "The requested operation is not supported by dCache");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event)
    {
        Throwable t = event.getCause();

        if (t instanceof TooLongFrameException) {
            sendFatalError(ctx, BAD_REQUEST, "Max request length exceeded");
        } else if (event.getChannel().isConnected()) {
            // We cannot know whether the error was generated before or
            // after we sent the response headers - if we already sent
            // response headers then we cannot send an error response now.
            // Better just to close the channel.
            event.getChannel().close();
        }

        if (t instanceof ClosedChannelException) {
            _logger.trace("ClosedChannelException for HTTP channel to {}", ctx.getChannel().getRemoteAddress());
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else {
            _logger.warn(t.toString());
        }
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
     */
    protected List<HttpByteRange> parseHttpRange(HttpRequest request,
                                           long lowerRange,
                                           long upperRange)
        throws HttpException
    {
        String rangeHeader = request.headers().get(RANGE);

        if (rangeHeader != null) {
            try {
                return HttpByteRange.parseRanges(rangeHeader, lowerRange, upperRange);
            } catch (HttpException e) {
                /*
                 * ignore errors in the range, if the If-Range header is present
                 */
                if (request.headers().get(IF_RANGE) == null) {
                    throw e;
                }
            }
        }
        return null;
    }

}
