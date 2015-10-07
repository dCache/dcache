/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

/**
 * Immutable class representing a port range.
 */
public class NettyPortRange extends PortRange
{
    /**
     * Creates a port range with the given bounds (both inclusive).
     * Zero is excluded from non-empty port ranges.
     *
     * @throws IllegalArgumentException is either bound is not between
     *         0 and 65535, or if <code>high</code> is lower than
     *         <code>low</code>.
     */
    public NettyPortRange(int low, int high)
    {
        super(low, high);
    }

    /**
     * Creates a port range containing a single port.
     */
    public NettyPortRange(int port)
    {
        this(port, port);
    }

    public NettyPortRange(PortRange range)
    {
        this(range.getLower(), range.getUpper());
    }

    /**
     * Parse a port range. A port range consists of either a single
     * integer, or two integers separated by either a comma or a
     * colon.
     *
     * The bounds must be between 0 and 65535, both inclusive.
     *
     * @return The port range represented by <code>s</code>. Returns
     * the range [0,0] if <code>s</code> is null or empty.
     */
    public static NettyPortRange valueOf(String s)
        throws IllegalArgumentException
    {
        return new NettyPortRange(PortRange.valueOf(s));
    }

    /**
     * Binds <code>server</socket> to <code>address</code>. A port is
     * chosen from this port range. If the port range is [0,0], then a
     * free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails.
     */
    public Channel bind(ServerBootstrap server, InetAddress address)
        throws IOException
    {
        int start = random();
        int port = start;
        do {
            try {
                ChannelFuture future = server.bind(new InetSocketAddress(address, port));
                Uninterruptibles.getUninterruptibly(future);
                return future.channel();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof BindException)) {
                    Throwables.propagateIfPossible(e.getCause(), IOException.class);
                    throw Throwables.propagate(e.getCause());
                }
            }
            port = succ(port);
        } while (port != start);

        throw new BindException("No free port within range");
    }

    /**
     * Binds <code>server</socket> to the wildcard
     * <code>address</code>. A port is chosen from this port range. If
     * the port range is [0,0], then a free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public Channel bind(ServerBootstrap socket)
        throws IOException
    {
        return bind(socket, null);
    }

}
