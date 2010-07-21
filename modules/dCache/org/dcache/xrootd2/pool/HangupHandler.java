package org.dcache.xrootd2.pool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.Channel;

/**
 * A SimpleChannelHandler which closes the parent channel upon the
 * first connect. Once that first channel is closed, this fact is
 * signaled through a latch.
 */
@ChannelPipelineCoverage("all")
public class HangupHandler extends SimpleChannelHandler
{
    private final CountDownLatch _latch;

    private final AtomicReference<Channel> _channel =
        new AtomicReference<Channel>();

    public HangupHandler(CountDownLatch latch)
    {
        _latch = latch;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx,
                            ChannelStateEvent event)
        throws Exception
    {
        Channel channel = ctx.getChannel();

        /* Refuse all but the first connect.
         */
        if (!_channel.compareAndSet(null, channel)) {
            channel.close();
            return;
        }

        super.channelOpen(ctx, event);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx,
                              ChannelStateEvent event)
        throws Exception
    {
        if (ctx.getChannel() == _channel.get()) {
            super.channelClosed(ctx, event);
            _latch.countDown();
        }
    }

}