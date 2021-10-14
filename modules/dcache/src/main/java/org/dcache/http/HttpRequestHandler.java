package org.dcache.http;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_RANGE;
import static io.netty.handler.codec.http.HttpHeaders.Names.RANGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import diskCacheV111.util.HttpByteRange;
import dmg.util.HttpException;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpRequestHandler extends SimpleChannelInboundHandler<Object> {

    protected static final String CRLF = "\r\n";

    private static final Logger LOGGER =
          LoggerFactory.getLogger(HttpRequestHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;

            ChannelFuture future;
            if (request.method() == HttpMethod.GET) {
                future = doOnGet(ctx, request);
            } else if (request.method() == HttpMethod.PUT) {
                future = doOnPut(ctx, request);
            } else if (request.method() == HttpMethod.POST) {
                future = doOnPost(ctx, request);
            } else if (request.method() == HttpMethod.DELETE) {
                future = doOnDelete(ctx, request);
            } else if (request.method() == HttpMethod.HEAD) {
                future = doOnHead(ctx, request);
            } else {
                future = unsupported(ctx);
            }
            if (future != null) {
                future.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                return;
            }
        }
        if (msg instanceof HttpContent) {
            ChannelFuture future = doOnContent(ctx, (HttpContent) msg);
            if (future != null) {
                future.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }
        }
    }

    protected ChannelFuture doOnGet(ChannelHandlerContext context, HttpRequest request) {
        LOGGER.debug("Received a GET request, writing a default response.");
        return unsupported(context);
    }

    protected ChannelFuture doOnPut(ChannelHandlerContext context, HttpRequest request) {
        LOGGER.debug("Received a PUT request, writing a default response.");
        return unsupported(context);
    }

    protected ChannelFuture doOnPost(ChannelHandlerContext context, HttpRequest request) {
        LOGGER.debug("Received a POST request, writing default response.");
        return unsupported(context);
    }

    protected ChannelFuture doOnDelete(ChannelHandlerContext context, HttpRequest request) {
        LOGGER.debug("Received a DELETE request, writing default response.");
        return unsupported(context);
    }

    protected ChannelFuture doOnContent(ChannelHandlerContext context, HttpContent chunk) {
        LOGGER.debug("Received an HTTP chunk, writing default response.");
        return unsupported(context);
    }

    protected ChannelFuture doOnHead(ChannelHandlerContext context, HttpRequest request) {
        LOGGER.debug("Received a HEAD request, writing default response.");
        return unsupported(context);
    }

    protected ChannelFuture unsupported(
          ChannelHandlerContext context) {
        return context.writeAndFlush(createErrorResponse(NOT_IMPLEMENTED,
              "The requested operation is not supported by dCache"));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        if (t instanceof TooLongFrameException) {
            FullHttpResponse response = createErrorResponse(BAD_REQUEST,
                  "Max request length exceeded");
            HttpUtil.setKeepAlive(response, false);
            ctx.channel().writeAndFlush(response);
        } else if (ctx.channel().isActive()) {
            // We cannot know whether the error was generated before or
            // after we sent the response headers - if we already sent
            // response headers then we cannot send an error response now.
            // Better just to close the channel.
            ctx.channel().close();
        }

        if (t instanceof ClosedChannelException) {
            LOGGER.trace("ClosedChannelException for HTTP channel to {}",
                  ctx.channel().remoteAddress());
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else {
            LOGGER.warn(t.toString());
        }
    }

    /**
     * Parse the HTTPRanges in the request, from the Range Header.
     * <p>
     * Return null if no range was found.
     *
     * @param request
     * @param lowerRange, as imposed by the backing physical file
     * @param upperRange, as imposed by the backing physical file
     * @return First byte range that was parsed
     */
    protected List<HttpByteRange> parseHttpRange(HttpRequest request,
          long lowerRange,
          long upperRange)
          throws HttpException {
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

    public static FullHttpResponse createErrorResponse(HttpResponseStatus status, String message) {
        return createErrorResponse(status.code(), message);
    }

    public static FullHttpResponse createErrorResponse(int code, String message) {
        HttpResponseStatus status = new HttpResponseStatus(code, message);
        LOGGER.info("Sending error '{}' to client.", status);
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
        response.headers().set(CONTENT_LENGTH, 0);
        return response;
    }

    protected static class HttpTextResponse extends DefaultFullHttpResponse {

        public HttpTextResponse(HttpResponseStatus status, String message) {
            super(HTTP_1_1, status, Unpooled.copiedBuffer(message + CRLF, CharsetUtil.UTF_8));
            headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
            headers().set(CONTENT_LENGTH, content().readableBytes());
        }
    }
}
