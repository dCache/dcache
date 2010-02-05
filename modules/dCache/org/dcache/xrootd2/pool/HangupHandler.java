package org.dcache.xrootd2.pool;

import java.util.concurrent.CountDownLatch;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelPipelineCoverage;

/**
 * A SimpleChannelHandler which closes the parent channel upon the
 * first connect. Once that first channel is closed, this fact is
 * signaled through a latch.
 */
@ChannelPipelineCoverage("all")
public class HangupHandler extends SimpleChannelHandler
{
    private final CountDownLatch _latch;

    private boolean _connected = false;

    public HangupHandler(CountDownLatch latch)
    {
        _latch = latch;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx,
                            ChannelStateEvent event)
        throws Exception
    {
        /* Refuse all but the first connect.
         */
        if (_connected) {
            ctx.getChannel().close();
            return;
        }

        /* Close parent channel as soon as we got the connection.
         */
        _connected = false;

        super.channelOpen(ctx, event);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx,
                              ChannelStateEvent event)
        throws Exception
    {
        _latch.countDown();
        super.channelClosed(ctx, event);
    }

}