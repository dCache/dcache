package org.dcache.xrootd2.core;

import static org.jboss.netty.channel.Channels.*;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.dcache.xrootd2.protocol.messages.*;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import org.apache.log4j.Logger;

/**
 * A SimpleChannelHandler which provides an individual handler method
 * for each xrootd request type.
 *
 * Default respons to all requests is kXR_Unsupported. Sub-classes
 * may override handler methods to implement request handling.
 */
@ChannelPipelineCoverage("all")
public class XrootdRequestHandler extends SimpleChannelHandler
{
    private final static Logger _log =
        Logger.getLogger(XrootdRequestHandler.class);

    public XrootdRequestHandler()
    {
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
    {
        AbstractRequestMessage msg = (AbstractRequestMessage) e.getMessage();

        if (msg instanceof AuthenticationRequest) {
            doOnAuthentication(ctx, e, (AuthenticationRequest) msg);
        } else if (msg instanceof LoginRequest) {
            doOnLogin(ctx, e, (LoginRequest) msg);
        } else if (msg instanceof OpenRequest) {
            doOnOpen(ctx, e, (OpenRequest) msg);
        } else if (msg instanceof StatRequest) {
            doOnStat(ctx, e, (StatRequest) msg);
        } else if (msg instanceof StatxRequest) {
            doOnStatx(ctx, e, (StatxRequest) msg);
        } else if (msg instanceof ReadRequest) {
            doOnRead(ctx, e, (ReadRequest) msg);
        } else if (msg instanceof ReadVRequest) {
            doOnReadV(ctx, e, (ReadVRequest) msg);
        } else if (msg instanceof WriteRequest) {
            doOnWrite(ctx, e, (WriteRequest) msg);
        } else if (msg instanceof SyncRequest) {
            doOnSync(ctx, e, (SyncRequest) msg);
        } else if (msg instanceof CloseRequest) {
            doOnClose(ctx, e, (CloseRequest) msg);
        } else if (msg instanceof ProtocolRequest) {
            doOnProtocolRequest(ctx, e, (ProtocolRequest) msg);
        } else if (msg instanceof PrepareRequest) {
            doOnPrepare(ctx, e, (PrepareRequest) msg);
        } else {
            unsupported(ctx, e, msg);
        }
    }

    protected ChannelFuture respond(ChannelHandlerContext ctx,
                                    MessageEvent e,
                                    AbstractResponseMessage msg)
    {
        return e.getChannel().write(msg);
    }

    protected ChannelFuture respondWithError(ChannelHandlerContext ctx,
                                             MessageEvent e,
                                             AbstractRequestMessage msg,
                                             int errorCode, String errMsg)
    {
        return respond(ctx, e,
                       new ErrorResponse(msg.getStreamID(), errorCode, errMsg));
    }

    protected ChannelFuture closeWithError(ChannelHandlerContext ctx,
                                           MessageEvent e,
                                           AbstractRequestMessage msg,
                                           int errorCode, String errMsg)
    {
        ChannelFuture f = respondWithError(ctx, e, msg, errorCode, errMsg);
        f.addListener(ChannelFutureListener.CLOSE);
        return f;
    }

    protected ChannelFuture unsupported(ChannelHandlerContext ctx,
                                        MessageEvent e,
                                        AbstractRequestMessage msg)
    {
        _log.info("Unsupported request: " + msg);
        return respondWithError(ctx, e, msg, kXR_Unsupported,
                                "Request " + msg.getRequestID() + " not supported");
    }

    protected void doOnLogin(ChannelHandlerContext ctx, MessageEvent e, LoginRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnAuthentication(ChannelHandlerContext ctx, MessageEvent e, AuthenticationRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnOpen(ChannelHandlerContext ctx, MessageEvent e, OpenRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnStat(ChannelHandlerContext ctx, MessageEvent e, StatRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnStatx(ChannelHandlerContext ctx, MessageEvent e, StatxRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnRead(ChannelHandlerContext ctx, MessageEvent e, ReadRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnReadV(ChannelHandlerContext ctx, MessageEvent e, ReadVRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnWrite(ChannelHandlerContext ctx, MessageEvent e, WriteRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnSync(ChannelHandlerContext ctx, MessageEvent e, SyncRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnClose(ChannelHandlerContext ctx, MessageEvent e, CloseRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnProtocolRequest(ChannelHandlerContext ctx, MessageEvent e, ProtocolRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnPrepare(ChannelHandlerContext ctx, MessageEvent e,
                               PrepareRequest msg) {
        unsupported(ctx, e, msg);
    }
}