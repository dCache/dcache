package org.dcache.xrootd2.core;

import java.util.Arrays;


import static org.jboss.netty.channel.Channels.*;
import static org.jboss.netty.buffer.ChannelBuffers.*;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

import org.dcache.xrootd2.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd2.protocol.messages.HandshakeRequest;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;
import org.jboss.netty.channel.ChannelHandler.Sharable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ChannelHandler which recognizes the xrootd handshake and
 * generates an appropriate response. Once handshaked, all messages
 * are passed on. Failure to handshake causes the channel to be
 * closed.
 */
@Sharable
public class XrootdHandshakeHandler extends SimpleChannelHandler
{
    private final static Logger _log =
        LoggerFactory.getLogger(XrootdHandshakeHandler.class);

    private final int _serverType;
    private boolean _isHandshaked;

    public XrootdHandshakeHandler(int serverType)
    {
        _serverType = serverType;
        _isHandshaked = false;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception
    {
        AbstractRequestMessage msg = (AbstractRequestMessage) e.getMessage();

        if (!_isHandshaked) {
            if (!(msg instanceof HandshakeRequest)) {
                _log.error("Invalid handshake");
                close(ctx, e.getFuture());
                return;
            }

            byte[] request = ((HandshakeRequest) msg).getHandshake();
            if (!Arrays.equals(request, HANDSHAKE_REQUEST)) {
                _log.error("Received corrupt handshake message ("
                           + request.length + " bytes).");
                close(ctx, e.getFuture());
                return;
            }

            byte[] response;
            switch (_serverType) {
            case LOAD_BALANCER:
                response = HANDSHAKE_RESPONSE_LOADBALANCER;
                break;

            case DATA_SERVER:
                response = HANDSHAKE_RESPONSE_DATASERVER;
                break;

            default:
                _log.error("Unknown server type (" + _serverType + ")");
                close(ctx, e.getFuture());
                return;
            }

            write(ctx, e.getFuture(), wrappedBuffer(response));

            _isHandshaked = true;

            return;
        }

        super.messageReceived(ctx, e);
    }
}