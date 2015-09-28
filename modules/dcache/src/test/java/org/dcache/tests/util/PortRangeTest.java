package org.dcache.tests.util;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.dcache.util.PortRange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PortRangeTest
{
    private final int PORT = 23456;
    private final InetAddress HOST;

    public PortRangeTest()
        throws UnknownHostException
    {
        HOST = InetAddress.getLocalHost();
    }

    public void assertPortRange(PortRange range, int lower, int upper)
    {
        assertEquals(range.getLower(), lower);
        assertEquals(range.getUpper(), upper);
    }

    public void bind(PortRange range, InetSocketAddress endpoint,
                     int lower, int upper)
        throws IOException
    {
        Socket socket = new Socket();
        range.bind(socket, endpoint);
        assertTrue("Port out of range",
                   lower <= socket.getLocalPort() &&
                   socket.getLocalPort() <= upper);
        assertEquals("Bound to wrong address",
                     socket.getLocalAddress(), endpoint.getAddress());
        socket.close();
    }

    public void bind(PortRange range, InetAddress address,
                     int lower, int upper)
        throws IOException
    {
        Socket socket = new Socket();
        range.bind(socket, address);
        assertTrue("Port out of range",
                   lower <= socket.getLocalPort() &&
                   socket.getLocalPort() <= upper);
        assertEquals("Bound to wrong address",
                     socket.getLocalAddress(), address);
        socket.close();
    }

    public void bind(PortRange range, int lower, int upper)
        throws IOException
    {
        Socket socket = new Socket();
        range.bind(socket);
        assertTrue("Port out of range",
                   lower <= socket.getLocalPort() &&
                   socket.getLocalPort() <= upper);
        assertTrue("Bound to wrong address",
                   socket.getLocalAddress().isAnyLocalAddress());
        socket.close();
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidRange1()
    {
        new PortRange(-1, 10);
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidRange2()
    {
        new PortRange(10, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidRange3()
    {
        new PortRange(2, 100000);
    }


    @Test(expected=IllegalArgumentException.class)
    public void invalidFormat1()
    {
        PortRange.valueOf(" ");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidFormat2()
    {
        PortRange.valueOf("wewe");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidFormat4()
    {
        PortRange.valueOf("1..10");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidFormat5()
    {
        PortRange.valueOf("");
    }

    @Test(expected=NullPointerException.class)
    public void invalidFormat6()
    {
        PortRange.valueOf(null);
    }

    @Test
    public void excludeZero()
    {
        assertPortRange(new PortRange(0,0), 0, 0);
        assertPortRange(new PortRange(0,1), 1, 1);
        assertPortRange(new PortRange(1,1), 1, 1);
        assertPortRange(new PortRange(1,2), 1, 2);
    }

    @Test
    public void valueOf()
    {
        assertPortRange(PortRange.valueOf("2323"), 2323, 2323);
        assertPortRange(PortRange.valueOf("0"), 0, 0);
        assertPortRange(PortRange.valueOf("0:0"), 0, 0);
        assertPortRange(PortRange.valueOf("0:1"), 1, 1);
        assertPortRange(PortRange.valueOf("1:1000"), 1, 1000);
        assertPortRange(PortRange.valueOf("1,1000"), 1, 1000);
    }

    @Test
    public void bind1()
        throws IOException
    {
        bind(new PortRange(0,0), 1, 65536);
        bind(new PortRange(0,0), HOST, 1, 65536);
        bind(new PortRange(0,0), new InetSocketAddress(HOST, 0), 1, 65536);
        bind(new PortRange(0,0), new InetSocketAddress(HOST, PORT),
             PORT, PORT);
        bind(new PortRange(PORT,PORT), PORT, PORT);
        bind(new PortRange(PORT,PORT), HOST, PORT, PORT);
        bind(new PortRange(PORT,PORT), new InetSocketAddress(HOST, 0),
             PORT, PORT);
        bind(new PortRange(PORT+1,PORT+2), new InetSocketAddress(HOST, PORT),
             PORT, PORT);
    }
}
