package org.dcache.util;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable class representing a port range.
 */
public class PortRange
{
    /** Pattern matching <PORT>[:<PORT>] */
    private final static Pattern FORMAT =
        Pattern.compile("(\\d+)(?:(?:,|:)(\\d+))?");

    /**
     * Random number generator used when binding sockets.
     */
    private final static Random _random = new Random();

    /**
     * The port range to use.
     */
    private final int _lower;
    private final int _upper;

    /**
     * Creates a port range with the given bounds (both inclusive).
     * Zero is excluded from non-empty port ranges.
     *
     * @throws IllegalArgumentException is either bound is not between
     *         0 and 65535, or if <code>high</code> is lower than
     *         <code>low</code>.
     */
    public PortRange(int low, int high)
    {
        /* Exclude zero from degenerate interval. Zero has a special
         * meaning when binding a port.
         */
        _lower = (low == 0 && high > 0) ? 1 : low;
        _upper = high;

        if (low < 0 || high < low || 65535 < high) {
            throw new IllegalArgumentException("Invalid range");
        }
    }

    /**
     * Creates a port range containing a single port.
     */
    public PortRange(int port)
    {
        this(port, port);
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
    public static PortRange valueOf(String s)
        throws IllegalArgumentException
    {
        try {
            Matcher m = FORMAT.matcher(s);

            if (!m.matches()) {
                throw new IllegalArgumentException("Invalid range: " + s);
            }

            int low = Integer.parseInt(m.group(1));
            int high =
                (m.groupCount() == 1) ? low : Integer.parseInt(m.group(2));

            return new PortRange(low, high);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid range: " + s);
        }
    }

    public int getLower()
    {
        return _lower;
    }

    public int getUpper()
    {
        return _upper;
    }

    /**
     * Returns a random port within the range.
     */
    public int random()
    {
        return _random.nextInt(_upper - _lower + 1) + _lower;
    }

    /**
     * Returns the successor of a port within the range, wrapping
     * around to the lowest port if necessary.
     */
    public int succ(int port)
    {
        return (port < _upper ? port + 1 : _lower);
    }

    /**
     * Binds <code>socket</socket> to <code>endpoint</code>. If the
     * port in <code>endpoint</code> is zero, then a port is chosen
     * from this port range. If the port range is [0,0], then a free
     * port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(ServerSocket socket, InetSocketAddress endpoint)
        throws IOException
    {
        int port = endpoint.getPort();
        PortRange range = (port > 0) ? new PortRange(port) : this;
        return range.bind(socket, endpoint.getAddress(), 0);
    }

    /**
     * Binds <code>socket</socket> to <code>endpoint</code>. If the
     * port in <code>endpoint</code> is zero, then a port is chosen
     * from this port range. If the port range is [0,0], then a free
     * port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(Socket socket, InetSocketAddress endpoint)
        throws IOException
    {
        int port = endpoint.getPort();
        PortRange range = (port > 0) ? new PortRange(port) : this;
        return range.bind(socket, endpoint.getAddress());
    }

    /**
     * Binds <code>socket</socket> to <code>address</code>. A port is
     * chosen from this port range. If the port range is [0,0], then a
     * free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(ServerSocket socket, InetAddress address)
        throws IOException
    {
        return bind(socket, address, 0);
    }

    /**
     * Binds <code>socket</socket> to <code>address</code>. A port is
     * chosen from this port range. If the port range is [0,0], then a
     * free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(ServerSocket socket, InetAddress address, int backlog)
        throws IOException
    {
        int start = random();
        int port = start;
        do {
            try {
                socket.bind(new InetSocketAddress(address, port), backlog);
                return port;
            } catch (BindException e) {
            }
            port = succ(port);
        } while (port != start);

        throw new BindException("No free port within range");
    }

    /**
     * Binds <code>socket</socket> to <code>address</code>. A port is
     * chosen from this port range. If the port range is [0,0], then a
     * free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(Socket socket, InetAddress address)
        throws IOException
    {
        int start = random();
        int port = start;
        do {
            try {
                socket.bind(new InetSocketAddress(address, port));
                return port;
            } catch (BindException e) {
            }
            port = succ(port);
        } while (port != start);

        throw new BindException("No free port within range");
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
                return server.bind(new InetSocketAddress(address, port));
            } catch (ChannelException e) {
                if (!(e.getCause() instanceof BindException)) {
                    throw e;
                }
            }
            port = succ(port);
        } while (port != start);

        throw new BindException("No free port within range");
    }

    /**
     * Binds <code>socket</socket> to the wildcard
     * <code>address</code>. A port is chosen from this port range. If
     * the port range is [0,0], then a free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(ServerSocket socket)
        throws IOException
    {
        return bind(socket, (InetAddress) null);
    }

    /**
     * Binds <code>socket</socket> to the wildcard
     * <code>address</code>. A port is chosen from this port range. If
     * the port range is [0,0], then a free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(ServerSocket socket, int backlog)
        throws IOException
    {
        return bind(socket, (InetAddress) null, backlog);
    }

    /**
     * Binds <code>socket</socket> to the wildcard
     * <code>address</code>. A port is chosen from this port range. If
     * the port range is [0,0], then a free port is chosen by the OS.
     *
     * @throws IOException if the bind operation fails, or if the
     * socket is already bound.
     */
    public int bind(Socket socket)
        throws IOException
    {
        return bind(socket, (InetAddress) null);
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

    @Override
    public String toString()
    {
        return String.format("%d:%d", _lower, _upper);
    }
}
