package org.dcache.xrootd.door;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

import org.dcache.util.Args;

import static org.jboss.netty.channel.Channels.close;

/**
 * Channel handler that keeps track of connected channels. Provides
 * administrative commands for listing and killing connections.
 */
@Sharable
public class ConnectionTracker
    extends SimpleChannelHandler
    implements CellCommandListener,
               CellInfoProvider
{
    private Map<Integer, Channel> _channels = new ConcurrentHashMap<>();
    private AtomicInteger _counter = new AtomicInteger();

    public ConnectionTracker()
    {
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx,
                                 ChannelStateEvent e)
        throws Exception
    {
        super.channelConnected(ctx, e);
        Channel channel = e.getChannel();
        _channels.put(channel.getId(), channel);
        _counter.getAndIncrement();
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx,
                                    ChannelStateEvent e)
        throws Exception
    {
        _channels.remove(e.getChannel().getId());
        super.channelDisconnected(ctx, e);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Active : %d", _channels.size()));
        pw.println(String.format("Created: %d", _counter.get()));
    }

    public String ac_connections(Args args)
    {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<Integer,Channel> e: _channels.entrySet()) {
            Channel c = e.getValue();
            s.append(e.getKey()).append(" ").append(c.getRemoteAddress())
                    .append("\n");
        }
        return s.toString();
    }

    public String ac_kill_$_1(Args args)
    {
        int id = Integer.parseInt(args.argv(1));
        Channel channel = _channels.get(id);
        if (channel == null) {
            return "No such connection";
        }

        close(channel);

        return "";
    }
}
