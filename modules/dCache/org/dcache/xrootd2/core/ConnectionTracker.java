package org.dcache.xrootd2.core;

import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jboss.netty.channel.Channels.*;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.Channel;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellInfoProvider;
import dmg.util.Args;
import dmg.cells.nucleus.CellInfo;

import org.apache.log4j.Logger;

/**
 * Channel handler that keeps track of connected channels. Provides
 * administrative commands for listing and killing connections.
 */
@ChannelPipelineCoverage("all")
public class ConnectionTracker
    extends SimpleChannelHandler
    implements CellCommandListener,
               CellInfoProvider
{
    private final static Logger _log =
        Logger.getLogger(ConnectionTracker.class);

    private Map<Integer, Channel> _channels = new ConcurrentHashMap();
    private AtomicInteger _counter = new AtomicInteger();

    public ConnectionTracker()
    {
    }

    public void channelConnected(ChannelHandlerContext ctx,
                                 ChannelStateEvent e)
        throws Exception
    {
        super.channelConnected(ctx, e);
        Channel channel = e.getChannel();
        _channels.put(channel.getId(), channel);
        _counter.getAndIncrement();
    }

    public void channelDisconnected(ChannelHandlerContext ctx,
                                    ChannelStateEvent e)
        throws Exception
    {
        _channels.remove(e.getChannel().getId());
        super.channelDisconnected(ctx, e);
    }

    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Active : %d", _channels.size()));
        pw.println(String.format("Created: %d", _counter.get()));
    }

    public String ac_connections(Args args)
    {
        StringBuffer s = new StringBuffer();
        for (Map.Entry<Integer,Channel> e: _channels.entrySet()) {
            Channel c = e.getValue();
            s.append(e.getKey() + " " +
                     c.getRemoteAddress() + "\n");
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