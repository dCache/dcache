package org.dcache.xrootd2.core;

import static org.jboss.netty.channel.Channels.*;
import static org.jboss.netty.buffer.ChannelBuffers.*;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.buffer.ChannelBuffer;

import org.dcache.xrootd2.protocol.messages.AbstractResponseMessage;

/**
 * Downstream ChannelHandler encoding AbstractResponseMessage objects
 * into ChannelBuffer objects.
 */
@ChannelPipelineCoverage("all")
public class XrootdEncoder extends SimpleChannelHandler
{
    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e)
    {
        Object msg = e.getMessage();
        if (msg instanceof AbstractResponseMessage) {
            AbstractResponseMessage response =
                (AbstractResponseMessage) msg;
            ChannelBuffer buffer = response.getBuffer();
            buffer.setInt(4, buffer.readableBytes() - 8);
            msg = buffer;
        }
        write(ctx, e.getChannel(), e.getFuture(), msg);
    }
}