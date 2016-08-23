package org.dcache.xrootd.door;

import com.google.common.net.HostAndPort;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

import org.dcache.util.Args;

/**
 * Channel handler that keeps track of connected channels. Provides
 * administrative commands for listing and killing connections.
 */
@Sharable
public class ConnectionTracker
    extends ChannelInboundHandlerAdapter
    implements CellCommandListener, CellInfoProvider
{
    private final Map<Channel,String> sessions = new ConcurrentHashMap<>();
    private final Map<Channel, HostAndPort> addresses = new ConcurrentHashMap<>();
    private final LongAdder counter = new LongAdder();

    @Override
    public void channelActive(ChannelHandlerContext ctx)
        throws Exception
    {
        Channel channel = ctx.channel();
        sessions.put(channel, CDC.getSession());
        counter.increment();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
        throws Exception
    {
        try {
            super.channelInactive(ctx);
        } finally {
            sessions.remove(ctx.channel());
            addresses.remove(ctx.channel());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof HAProxyMessage) {
            HAProxyMessage proxyMessage = (HAProxyMessage) msg;
            if (proxyMessage.command() == HAProxyCommand.PROXY) {
                addresses.put(ctx.channel(), HostAndPort.fromParts(proxyMessage.sourceAddress(), proxyMessage.sourcePort()));
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Active : %d", sessions.size()));
        pw.println(String.format("Created: %d", counter.longValue()));
    }

    public String ac_connections(Args args)
    {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<Channel, String> e: sessions.entrySet()) {
            s.append(e.getValue()).append(' ').append(e.getKey());
            HostAndPort hostAndPort = addresses.get(e.getKey());
            if (hostAndPort != null) {
                s.append(' ').append(hostAndPort);
            }
            s.append("\n");
        }
        return s.toString();
    }

    public String ac_kill_$_1(Args args)
    {
        String session = args.argv(0);
        Iterator<String> iterator = sessions.values().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().equals(session)) {
                iterator.remove();
                return "";
            }
        }
        return "No such connection";
    }
}
