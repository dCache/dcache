package org.dcache.http;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCEPT_RANGES;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_MD5;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_RANGE;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Values.BYTES;
import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.INSUFFICIENT_STORAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.util.Objects.requireNonNull;
import static org.dcache.util.Checksums.TO_RFC3230;
import static org.dcache.util.StringMarkup.percentEncode;
import static org.dcache.util.StringMarkup.quotedString;

import com.google.common.base.CharMatcher;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.HttpByteRange;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.util.HttpException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.pool.movers.RepositoryFileRegion;
import org.dcache.pool.repository.OutOfDiskException;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpPoolRequestHandler - handle HTTP client - server communication.
 */
public class HttpPoolRequestHandler extends HttpRequestHandler {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(HttpPoolRequestHandler.class);

    public static final String REFERRER_QUERY_PARAM = "dcache-http-ref";
    private static final String DIGEST = "Digest";

    private static final String RANGE_SEPARATOR = "-";
    private static final String RANGE_PRE_TOTAL = "/";
    private static final String RANGE_SP = " ";
    private static final String BOUNDARY = "__AAAAAAAAAAAAAAAA__";
    private static final String MULTIPART_TYPE =
          "multipart/byteranges; boundary=\"" + HttpPoolRequestHandler.BOUNDARY + "\"";
    private static final String TWO_HYPHENS = "--";
    // See RFC 2045 for definition of 'tspecials'
    private static final CharMatcher TSPECIAL = CharMatcher.anyOf("()<>@,;:\\\"/[]?=");

    private static final List<String> SUPPORTED_CONTENT_HEADERS
          = ImmutableList.of(CONTENT_LENGTH, CONTENT_MD5);

    /**
     * The mover channels that were opened.
     */
    private final Multiset<NettyTransferService<HttpProtocolInfo>.NettyMoverChannel> _files =
          HashMultiset.create();

    /**
     * The server in the context of which this handler is executed
     */
    private final NettyTransferService<HttpProtocolInfo> _server;

    private final int _chunkSize;

    /**
     * Whatever handler should use zero-copy capability.
     */
    private final boolean _useZeroCopy;

    /**
     * The file being uploaded. Even though we only keep the file open for the processing of a
     * single HTTP message, that one message may have been split into several chunks. Hence we have
     * to keep a reference to the file in between channel events.
     */
    private NettyTransferService<HttpProtocolInfo>.NettyMoverChannel _writeChannel;

    private Optional<ChecksumType> _wantedDigest;

    /**
     * A simple data class to encapsulate the errors to return by the mover to the pool for file
     * uploads and downloads, should transfers be aborted.
     */
    private static class FileReleaseErrors {

        private Optional<? extends Exception> errorForDownload = Optional.empty();
        private Optional<? extends Exception> errorForUpload = Optional.empty();

        public FileReleaseErrors downloadsSeeError(Exception e) {
            errorForDownload = Optional.of(requireNonNull(e));
            return this;
        }

        public FileReleaseErrors uploadsSeeError(Exception e) {
            errorForUpload = Optional.of(requireNonNull(e));
            return this;
        }
    }

    public HttpPoolRequestHandler(NettyTransferService<HttpProtocolInfo> server, int chunkSize,
          boolean useZeroCopy) {
        _server = server;
        _chunkSize = chunkSize;
        _useZeroCopy = useZeroCopy;
    }

    private static Optional<String> wantDigest(HttpRequest request) {
        List<String> wantDigests = request.headers().getAll("Want-Digest");
        return wantDigests.isEmpty()
              ? Optional.empty()
              : Optional.of(wantDigests.stream()
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.joining(",")));
    }

    private static Optional<Checksum> contentMd5Checksum(HttpRequest request) throws HttpException {
        try {
            Optional<String> contentMd5 = Optional.ofNullable(request.headers().get(CONTENT_MD5));
            return contentMd5.map(Checksums::parseContentMd5);
        } catch (IllegalArgumentException e) {
            throw new HttpException(BAD_REQUEST.code(), "Bad " + CONTENT_MD5 + " header: " + e);
        }
    }

    private static OptionalLong contentLength(HttpRequest request) throws HttpException {
        try {
            String contentLength = request.headers().get(CONTENT_LENGTH);
            return contentLength == null
                  ? OptionalLong.empty()
                  : OptionalLong.of(Long.parseLong(contentLength));
        } catch (NumberFormatException e) {
            throw new HttpException(BAD_REQUEST.code(), "Bad " + CONTENT_LENGTH + " header: " + e);
        }
    }

    private static ByteBuf createMultipartFragmentMarker(long lower, long upper, long total) {
        return Unpooled.copiedBuffer(CRLF + TWO_HYPHENS + BOUNDARY + CRLF + CONTENT_RANGE + ": " +
                    BYTES + RANGE_SP + lower + RANGE_SEPARATOR + upper + RANGE_PRE_TOTAL + total + CRLF
                    + CRLF,
              StandardCharsets.UTF_8);
    }

    private static ByteBuf createMultipartEnd() {
        return Unpooled.copiedBuffer(CRLF + TWO_HYPHENS + BOUNDARY + TWO_HYPHENS + CRLF,
              StandardCharsets.UTF_8);
    }

    private static String contentDisposition(HttpProtocolInfo.Disposition disposition,
          String filename) {
        StringBuilder sb = new StringBuilder();
        sb.append(disposition.toString().toLowerCase());
        appendDispositionParm(sb, "filename", filename);
        // REVISIT consider more info: creation-date, last-modified-date, size
        return sb.toString();
    }

    private static void appendDispositionParm(StringBuilder sb, String name, String value) {
        sb.append(';');

        // See RFC 2183 part 2. for description of when and how to encode
        if (value.length() > 78 || !CharMatcher.ascii().matchesAllOf(value)) {
            appendUsingRfc2231Encoding(sb, name, "UTF-8", null, value);
        } else if (TSPECIAL.matchesAnyOf(value)) {
            appendAsQuotedString(sb, name, value);
        } else {
            sb.append(name).append("=").append(value);
        }
    }

    // RFC 822 defines quoted-string: a simple markup using backslash
    private static void appendAsQuotedString(StringBuilder sb, String name,
          String value) {
        sb.append(name).append("=");
        quotedString(sb, value);
    }

    private static void appendUsingRfc2231Encoding(StringBuilder sb, String name,
          String charSet, String language, String value) {
        sb.append(name).append("*=");
        if (charSet != null) {
            sb.append(charSet);
        }
        sb.append('\'');
        if (language != null) {
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
          throws HttpException {
        outer:
        for (String headerName : headerNames) {
            if (headerName.toLowerCase().startsWith("content-")) {
                for (String exclude : excludes) {
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
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("HTTP connection from {} established", ctx.channel().remoteAddress());
    }

    private static FileReleaseErrors uploadsSeeError(Exception e) {
        return new FileReleaseErrors().uploadsSeeError(e);
    }

    private static FileReleaseErrors downloadsSeeError(Exception e) {
        return new FileReleaseErrors().downloadsSeeError(e);
    }

    private void releaseAllFiles(FileReleaseErrors errors) {
        for (NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file : _files) {
            Optional<? extends Exception> possibleError = file == _writeChannel
                  ? errors.errorForUpload : errors.errorForDownload;
            if (possibleError.isPresent()) {
                file.release(possibleError.get());
            } else {
                file.release();
            }
        }
        _files.clear();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("HTTP connection from {} closed", ctx.channel().remoteAddress());
        releaseAllFiles(uploadsSeeError(
              new FileCorruptedCacheException("Connection lost before end of file.")));
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        if (t instanceof ClosedChannelException) {
            LOGGER.info("Connection {} unexpectedly closed.", ctx.channel());
        } else if (t instanceof Exception) {
            releaseAllFiles(downloadsSeeError(new CacheException(t.toString(), t))
                  .uploadsSeeError(
                        new FileCorruptedCacheException("Connection lost before end of file: " + t,
                              t)));
            ctx.close();
        } else {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
            ctx.close();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent) event;
            if (idleStateEvent.state() == IdleState.ALL_IDLE) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Connection from {} id idle; disconnecting.",
                          ctx.channel().remoteAddress());
                }
                releaseAllFiles(uploadsSeeError(
                      new FileCorruptedCacheException("Channel idle for too long during upload.")));
                ctx.close();
            }
        }
    }

    private static String escapeNonASCII(String input) {
        StringBuilder sb = new StringBuilder();

        for (byte b : input.getBytes()) {
            int w = b & 0xFF;
            if (b == '\\') {
                sb.append("\\\\");
            } else if (w <= 128) {
                sb.append((char) (b & 0xFF));
            } else {
                sb.append(String.format("\\x%02X", w));
            }
        }

        return sb.toString();
    }

    private static boolean isBadRequest(HttpObject object) {
        DecoderResult dr = object.decoderResult();
        if (dr.isSuccess()) {
            return false;
        }

        String type = object instanceof HttpRequest
              ? (((HttpRequest) object).method() + " request")
              : "entity";

        if (!dr.isFinished()) {
            LOGGER.warn("Client sent incomplete {}", type);
            return true;
        }

        String description;
        Throwable cause = dr.cause();
        if (cause == null) {
            description = "<unknown reason>";
        } else {
            // netty has a habit of copying client input directly into the
            // error message, which might be large and contain binary data.
            String message = cause.getMessage();
            description = message == null
                  ? cause.getClass().getSimpleName()
                  : (message.length() > 80
                        ? escapeNonASCII(message.substring(0, 80)) + "[...]"
                        : escapeNonASCII(message));
        }

        LOGGER.warn("Client sent malformed {}: {}", type, description);
        return true;
    }

    /**
     * Single GET operation.
     * <p>
     * Finds the correct mover channel using the UUID in the GET. Range queries are supported. The
     * file will be sent to the remote peer in chunks to avoid server side memory issues.
     */
    @Override
    protected ChannelFuture doOnGet(ChannelHandlerContext context,
          HttpRequest request) {
        NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file;
        List<HttpByteRange> ranges;
        long fileSize;

        if (isBadRequest(request)) {
            return context.newSucceededFuture();
        }

        try {
            file = open(request, false);

            if (file.getIoMode().contains(StandardOpenOption.WRITE)) {
                throw new HttpException(METHOD_NOT_ALLOWED.code(),
                      "Resource is not open for reading");
            }

            fileSize = file.size();
            ranges = parseHttpRange(request, 0, fileSize - 1);
        } catch (Redirect e) {
            return context.writeAndFlush(e.createResponse());
        } catch (HttpException e) {
            return context.writeAndFlush(createErrorResponse(e.getErrorCode(), e.getMessage()));
        } catch (URISyntaxException e) {
            return context.writeAndFlush(
                  createErrorResponse(BAD_REQUEST, "URI not valid: " + e.getMessage()));
        } catch (IllegalArgumentException e) {
            return context.writeAndFlush(createErrorResponse(BAD_REQUEST, e.getMessage()));
        } catch (IOException e) {
            return context.writeAndFlush(
                  createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
        }

        Optional<String> digest = wantDigest(request)
              .flatMap(h -> Checksums.digestHeader(h, file.getFileAttributes()));

        if (ranges == null || ranges.isEmpty()) {
            /*
             * GET for a whole file
             */
            context.write(new HttpGetResponse(fileSize, file, digest))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            context.write(read(context, file, 0, fileSize - 1))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            ChannelFuture writeAndFlush = context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

            // Release the file immediately after supplying all of the file's content.  We're
            // assuming that the client will not make further requests against this URL.  This is
            // done to send the DoorTransferFinishedMessage in a timely fashion.
            writeAndFlush.addListener(f -> file.release());

            return writeAndFlush;
        } else if (ranges.size() == 1) {
            /* RFC 2616: 14.16. A response to a request for a single range
             * MUST NOT be sent using the multipart/byteranges media type.
             */
            HttpByteRange range = ranges.get(0);
            context.write(new HttpPartialContentResponse(range.getLower(), range.getUpper(),
                        fileSize, digest))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            context.write(read(context, file, range.getLower(), range.getUpper()))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

            // File is released when the client disconnects.  We're assuming that, after this, the
            // client will not make further requests against this URL.
            return context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
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

                ByteBuf buffer = fragmentMarkers[i] = createMultipartFragmentMarker(lower, upper,
                      fileSize);
                totalLen += buffer.readableBytes();
            }
            ByteBuf endMarker = createMultipartEnd();
            totalLen += endMarker.readableBytes();

            context.write(new HttpMultipartResponse(digest, totalLen))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            for (int i = 0; i < ranges.size(); i++) {
                HttpByteRange range = ranges.get(i);
                context.write(fragmentMarkers[i])
                      .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                context.write(read(context, file, range.getLower(), range.getUpper()))
                      .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }

            // File is released when the client disconnects.  We're assuming that, after this, the
            // client will not make further requests against this URL.
            return context.writeAndFlush(new DefaultLastHttpContent(endMarker));
        }
    }

    @Override
    protected ChannelFuture doOnPut(ChannelHandlerContext context, HttpRequest request) {
        NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file = null;
        Exception exception = null;

        if (isBadRequest(request)) {
            return context.newSucceededFuture();
        }

        try {
            checkContentHeader(request.headers().names(), SUPPORTED_CONTENT_HEADERS);

            file = open(request, true);

            if (!file.getIoMode().contains(StandardOpenOption.WRITE)) {
                throw new HttpException(METHOD_NOT_ALLOWED.code(),
                      "Resource is not open for writing");
            }

            contentMd5Checksum(request).ifPresent(file::addChecksum);

            OptionalLong contentLength = contentLength(request);
            if (contentLength.isPresent()) {
                file.truncate(contentLength.getAsLong());
            } else if (file.getFileAttributes().isDefined(FileAttribute.SIZE)) {
                file.truncate(file.getFileAttributes().getSize());
            }

            file.getProtocolInfo().getWantedChecksum().ifPresent(file::addChecksumType);
            _wantedDigest = wantDigest(request).flatMap(Checksums::parseWantDigest);
            _wantedDigest.ifPresent(file::addChecksumType);

            if (is100ContinueExpected(request)) {
                context.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE))
                      .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }
            _writeChannel = file;
            file = null;
            return null;
        } catch (Redirect e) {
            exception = e;
            return context.writeAndFlush(e.createResponse());
        } catch (HttpException e) {
            exception = e;
            return context.writeAndFlush(
                  createErrorResponse(HttpResponseStatus.valueOf(e.getErrorCode()),
                        e.getMessage()));
        } catch (URISyntaxException e) {
            exception = e;
            return context.writeAndFlush(
                  createErrorResponse(BAD_REQUEST, "URI is not valid: " + e.getMessage()));
        } catch (IllegalArgumentException e) {
            exception = e;
            return context.writeAndFlush(createErrorResponse(BAD_REQUEST, e.getMessage()));
        } catch (IOException | RuntimeException e) {
            exception = e;
            return context.writeAndFlush(
                  createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
        } finally {
            if (file != null) {
                file.release(exception);
                _files.remove(file);
            }
        }
    }

    @Override
    protected ChannelFuture doOnContent(ChannelHandlerContext context, HttpContent content) {
        if (isBadRequest(content)) {
            return context.newSucceededFuture();
        }

        if (_writeChannel != null) {
            try {
                ByteBuf data = content.content();
                while (data.isReadable()) {
                    data.readBytes(_writeChannel, data.readableBytes());
                }
                if (content instanceof LastHttpContent) {
                    checkContentHeader(((LastHttpContent) content).trailingHeaders().names(),
                          Collections.singletonList(CONTENT_LENGTH));

                    context.channel().config().setAutoRead(false);

                    NettyTransferService<HttpProtocolInfo>.NettyMoverChannel writeChannel = _writeChannel;
                    _writeChannel = null;
                    _files.remove(writeChannel);

                    long size = writeChannel.size();
                    URI location = writeChannel.getProtocolInfo().getLocation();

                    ChannelPromise promise = context.newPromise();
                    Futures.addCallback(writeChannel.release(), new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(Void result) {
                            try {
                                Optional<String> digest = _wantedDigest
                                      .flatMap(t -> Checksums.digestHeader(t,
                                            writeChannel.getFileAttributes()));
                                context.writeAndFlush(new HttpPutResponse(size, location, digest),
                                      promise);
                            } catch (IOException e) {
                                context.writeAndFlush(
                                      createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()),
                                      promise);
                            }
                            context.channel().config().setAutoRead(true);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            if (t instanceof FileCorruptedCacheException) {
                                context.writeAndFlush(
                                      createErrorResponse(BAD_REQUEST, t.getMessage()), promise);
                            } else if (t instanceof CacheException) {
                                context.writeAndFlush(
                                      createErrorResponse(INTERNAL_SERVER_ERROR, t.getMessage()),
                                      promise);
                            } else {
                                context.writeAndFlush(
                                      createErrorResponse(INTERNAL_SERVER_ERROR, t.toString()),
                                      promise);
                            }
                            context.channel().config().setAutoRead(true);
                        }
                    }, MoreExecutors.directExecutor());
                    return promise;
                }
            } catch (OutOfDiskException e) {
                _writeChannel.release(e);
                _files.remove(_writeChannel);
                _writeChannel = null;
                return context.writeAndFlush(
                      createErrorResponse(INSUFFICIENT_STORAGE, e.getMessage()));
            } catch (IOException e) {
                _writeChannel.release(e);
                _files.remove(_writeChannel);
                _writeChannel = null;
                return context.writeAndFlush(
                      createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
            } catch (HttpException e) {
                _writeChannel.release(e);
                _files.remove(_writeChannel);
                _writeChannel = null;
                return context.writeAndFlush(
                      createErrorResponse(HttpResponseStatus.valueOf(e.getErrorCode()),
                            e.getMessage()));
            }
        }
        return null;
    }

    @Override
    protected ChannelFuture doOnHead(ChannelHandlerContext context, HttpRequest request) {

        if (isBadRequest(request)) {
            return context.newSucceededFuture();
        }

        try {
            NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file = open(request, false);

            Optional<String> digest = wantDigest(request)
                  .flatMap(h -> Checksums.digestHeader(h, file.getFileAttributes()));
            context.write(new HttpGetResponse(file.size(), file, digest))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            return context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        } catch (Redirect e) {
            return context.writeAndFlush(e.createResponse());
        } catch (IOException | IllegalArgumentException e) {
            return context.writeAndFlush(createErrorResponse(BAD_REQUEST, e.getMessage()));
        } catch (URISyntaxException e) {
            return context.writeAndFlush(createErrorResponse(BAD_REQUEST,
                  "URI not valid: " + e.getMessage()));
        } catch (RuntimeException e) {
            return context.writeAndFlush(
                  createErrorResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    /**
     * Get the mover channel for a certain HTTP request. The mover channel is identified by UUID
     * generated upon mover start and sent back to the door as a part of the address info.
     *
     * @param request   HttpRequest that was sent by the client
     * @param exclusive True if the mover channel exclusively is to be opened in exclusive mode.
     *                  False if the mover channel can be shared with other requests.
     * @return Mover channel for specified UUID
     * @throws IllegalArgumentException Request did not include UUID or no mover channel found for
     *                                  UUID in the request
     */
    private NettyTransferService<HttpProtocolInfo>.NettyMoverChannel open(HttpRequest request,
          boolean exclusive)
          throws IllegalArgumentException, URISyntaxException, Redirect {
        QueryStringDecoder queryStringDecoder =
              new QueryStringDecoder(request.getUri());

        Map<String, List<String>> params = queryStringDecoder.parameters();
        if (!params.containsKey(HttpTransferService.UUID_QUERY_PARAM)) {
            if (!request.getUri().equals("/favicon.ico")) {
                LOGGER.error("Received request without UUID in the query " +
                      "string. Request-URI was {}", request.getUri());
            }

            throw new IllegalArgumentException("Query string does not include any UUID.");
        }

        List<String> uuidList = params.get(HttpTransferService.UUID_QUERY_PARAM);
        if (uuidList.isEmpty()) {
            throw new IllegalArgumentException("UUID parameter does not include any value.");
        }

        UUID uuid = UUID.fromString(uuidList.get(0));
        NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file = _server.openFile(uuid,
              exclusive);
        if (file == null) {
            Optional<URI> referrer = buildReferrer(request, params);
            if (referrer.isPresent()) {
                throw new Redirect(referrer.get(), "Request is no longer valid");
            }

            throw new IllegalArgumentException("Request is no longer valid. " +
                  "Please resubmit to door.");
        }

        URI uri = new URI(request.getUri());
        FsPath requestedFile = FsPath.create(uri.getPath());
        FsPath transferFile = FsPath.create(file.getProtocolInfo().getPath());

        if (!requestedFile.equals(transferFile)) {
            LOGGER.warn("Received an illegal request for file {}, while serving {}",
                  requestedFile,
                  transferFile);
            throw new IllegalArgumentException("The file you specified does " +
                  "not match the UUID you specified!");
        }

        _files.add(file);

        return file;
    }

    /**
     * Reconstruct the URL of the resource targeted by the client when it made the request to the
     * WebDAV door.
     * @param request The HTTP request made to the pool
     * @param params The query parameters, taken from this request.
     * @return Optionally the URL of the targeted resource.
     */
    private Optional<URI> buildReferrer(HttpRequest request, Map<String, List<String>> params) {
        List<String> refList = params.get(REFERRER_QUERY_PARAM);
        if (refList == null) {
            LOGGER.debug("Missing {} param in request", REFERRER_QUERY_PARAM);
            return Optional.empty();
        }

        if (refList.size() != 1) {
            LOGGER.warn("Unexpected number of {} entries: {}", REFERRER_QUERY_PARAM, refList.size());
            return Optional.empty();
        }

        String ref = refList.get(0);

        URI base;
        try {
            base = new URI(ref);
        } catch (URISyntaxException e) {
            LOGGER.warn("Ignoring bad referrer base \"{}\": {}", ref, e.getMessage());
            return Optional.empty();
        }


        URI requestToPool;
        try {
            requestToPool = new URI(request.uri()); // This is (very likely) just the path.
        } catch (URISyntaxException e) {
            LOGGER.warn("Ignoring bad request target \"{}\": {}", request.uri(), e.getMessage());
            return Optional.empty();
        }

        URI requestToDoor = base.resolve(requestToPool.getRawPath());
        return Optional.of(requestToDoor);
    }

    /**
     * Read the resources requested in HTTP-request from the pool. Return a ChunkedInput pointing to
     * the requested portions of the file.
     * <p>
     * Renew the keep-alive heartbeat, meaning that the last transferred time will be updated,
     * resetting the keep-alive timeout.
     *
     * @param file       the mover channel to read from
     * @param lowerRange The lower delimiter of the requested byte range of the file
     * @param upperRange The upper delimiter of the requested byte range of the file
     * @return ChunkedInput View upon the file suitable for sending with netty and representing the
     * requested parts.
     */
    private Object read(ChannelHandlerContext context, NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file,
          long lowerRange, long upperRange) {
        /* need to count position 0 as well */
        long length = (upperRange - lowerRange) + 1;

        if (_useZeroCopy) {
            // disable timeout manager as zero-copy can't keep idle counters in sync
            context.channel().pipeline().remove(IdleStateHandler.class);
            return asFileRegion(file, lowerRange, length);
        }
        return new ReusableChunkedNioFile(file, lowerRange, length, _chunkSize);
    }

    private FileRegion asFileRegion(NettyTransferService<? extends ProtocolInfo>.NettyMoverChannel file,
        long offset, long length) {
            return new RepositoryFileRegion(file, offset, length);
    }


    private static String buildDigest(
          NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file) {
        FileAttributes attributes = file.getFileAttributes();
        return attributes.getChecksumsIfPresent().map(TO_RFC3230::apply).orElse("");
    }

    private static class HttpGetResponse extends DefaultHttpResponse {

        public HttpGetResponse(long fileSize,
              NettyTransferService<HttpProtocolInfo>.NettyMoverChannel file,
              Optional<String> digest) {
            super(HTTP_1_1, OK);
            HttpProtocolInfo protocolInfo = file.getProtocolInfo();
            headers().add(ACCEPT_RANGES, BYTES);
            headers().add(CONTENT_LENGTH, fileSize);
            digest.ifPresent(v -> headers().add(DIGEST, v));
            headers().add("Content-Disposition",
                  contentDisposition(protocolInfo.getDisposition(),
                        FsPath.create(protocolInfo.getPath()).name()));
            if (protocolInfo.getLocation() != null) {
                headers().add(CONTENT_LOCATION, protocolInfo.getLocation());
            }
        }
    }

    private static class HttpPartialContentResponse extends DefaultHttpResponse {

        public HttpPartialContentResponse(long lower,
              long upper,
              long total,
              Optional<String> digest) {
            super(HTTP_1_1, PARTIAL_CONTENT);

            String contentRange = BYTES + RANGE_SP + lower + RANGE_SEPARATOR +
                  upper + RANGE_PRE_TOTAL + total;

            headers().add(ACCEPT_RANGES, BYTES);
            headers().add(CONTENT_LENGTH, String.valueOf((upper - lower) + 1));
            headers().add(CONTENT_RANGE, contentRange);
            digest.ifPresent(v -> headers().add(DIGEST, v));
        }
    }

    private static class HttpMultipartResponse extends DefaultHttpResponse {

        public HttpMultipartResponse(Optional<String> digest, long totalBytes) {
            super(HTTP_1_1, PARTIAL_CONTENT);
            headers().add(ACCEPT_RANGES, BYTES);
            headers().add(CONTENT_LENGTH, totalBytes);
            headers().add(CONTENT_TYPE, MULTIPART_TYPE);
            digest.ifPresent(v -> headers().add(DIGEST, v));
        }
    }

    private static class HttpPutResponse extends HttpTextResponse {

        public HttpPutResponse(long size, URI location, Optional<String> digest)
              throws IOException {
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
            super(CREATED, size + " bytes uploaded\r\n");

            /* RFC 2616: 14.30. For 201 (Created) responses, the Location is
             * that of the new resource which was created by the request.
             */
            if (location != null) {
                headers().set(LOCATION, location);
            }

            digest.ifPresent(v -> headers().add(DIGEST, v));
        }
    }

    private static class Redirect extends Exception {
        private final URI target;

        public Redirect(URI target, String message) {
            super(message);
            this.target = requireNonNull(target);
        }

        public URI target() {
            return target;
        }

        public FullHttpResponse createResponse() {
            return createRedirectResponse(target.toASCIIString(), getMessage());
        }

        @Override
        public String toString() {
            String message = getMessage();
            return message == null
                    ? "Redirect to " + target
                    : "Redirect to " + target + ": " + getMessage();
        }
    }
}
