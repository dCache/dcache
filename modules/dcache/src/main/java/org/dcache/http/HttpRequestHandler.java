package org.dcache.http;

import com.google.common.base.CharMatcher;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.IOException;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.HttpByteRange;
import dmg.util.HttpException;
import java.nio.charset.Charset;
import static org.dcache.util.StringMarkup.percentEncode;
import static org.dcache.util.StringMarkup.quotedString;

public class HttpRequestHandler extends IdleStateAwareChannelHandler
{
    private static final String RANGE_SEPARATOR = "-";
    private static final String RANGE_PRE_TOTAL = "/";
    private static final String RANGE_SP = " ";
    private static final String BOUNDARY = "__AAAAAAAAAAAAAAAA__";
    private static final String MULTIPART_TYPE = "multipart/byteranges; boundary=\"" + BOUNDARY + "\"";
    private static final String CRLF = "\r\n";
    private static final Charset CHARSET_TO_USE = Charset.forName("UTF-8");

    // See RFC 2045 for definition of 'tspecials'
    private static final CharMatcher TSPECIAL = CharMatcher.anyOf("()<>@,;:\\\"/[]?=");

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
        buffer.writeBytes(message.getBytes(CHARSET_TO_USE));

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

        response.setHeader(ACCEPT_RANGES, BYTES);
        response.setHeader(CONTENT_LENGTH, String.valueOf((upper - lower) + 1));
        response.setHeader(CONTENT_RANGE, contentRange);

        return event.getChannel().write(response);
    }

    protected ChannelFuture sendHTTPMultipartHeader(ChannelHandlerContext context,
            MessageEvent event)
            throws IOException {
        HttpResponse response =
                new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);

        response.setHeader(ACCEPT_RANGES, BYTES);
        response.setHeader(CONTENT_TYPE, MULTIPART_TYPE);

        return event.getChannel().write(response);
    }

    protected ChannelFuture sendHTTPMultipartFragment(ChannelHandlerContext context,
            MessageEvent event,
            long lower,
            long upper,
            long total)
            throws IOException {

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

        String response = sb.toString();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(response.getBytes(CHARSET_TO_USE));
        return event.getChannel().write(buffer);
    }

    protected ChannelFuture sendHTTPMultipartEnd(ChannelHandlerContext context,
            MessageEvent event)
            throws IOException {

        StringBuilder sb = new StringBuilder(64);
        sb.append(CRLF);
        sb.append("--").append(BOUNDARY).append("--").append(CRLF);

        String response = sb.toString();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(response.getBytes(CHARSET_TO_USE));
        return event.getChannel().write(buffer);
    }

    protected ChannelFuture sendHTTPFullHeader(ChannelHandlerContext context,
                                                  MessageEvent event,
                                                  long contentLength,
                                                  String filename) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setHeader(CONTENT_LENGTH, contentLength);
        response.setHeader("Content-Disposition", contentDisposition(filename));

        return event.getChannel().write(response);
    }

    private String contentDisposition(String filename)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("attachment");

        appendDispositionParm(sb, "filename", filename);

        // REVISIT consider more info: creation-date, last-modified-date, size

        return sb.toString();
    }

    private void appendDispositionParm(StringBuilder sb, String name, String value)
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

    protected void unsupported(ChannelHandlerContext context, MessageEvent event)
    {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NOT_IMPLEMENTED);

        response.setHeader(CONNECTION, CLOSE);
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
    protected List<HttpByteRange> parseHttpRange(HttpRequest request,
                                           long lowerRange,
                                           long upperRange)
        throws HttpException, IOException {
        String rangeHeader = request.getHeader(RANGE);

        if (rangeHeader != null) {
            try {
                return HttpByteRange.parseRanges(rangeHeader, lowerRange, upperRange);
            } catch (HttpException e) {
                /*
                 * ignore errors in the range, if the If-Range header is present
                 */
                if (request.getHeader(IF_RANGE) == null) {
                    throw e;
                }
            }
        }
        return null;
    }

}
