package org.dcache.utils.net;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

public class InetSocketAddressesTest {

    @Test
    public void testLocalHostV4() throws Exception {
        String uaddr = "127.0.0.1.203.81";
        InetSocketAddress socketAddress = InetSocketAddresses.forUaddrString(uaddr);
        assertEquals("port mismatch", 52049, socketAddress.getPort());
        assertEquals("host mismatch", InetAddress.getByName("127.0.0.1"),
                socketAddress.getAddress());
    }

    @Test
    public void testLocalHostV6() throws Exception {
        String uaddr = "::1.203.81";
        InetSocketAddress socketAddress = InetSocketAddresses.forUaddrString(uaddr);
        assertEquals("port mismatch", 52049, socketAddress.getPort());
        assertEquals("host mismatch", InetAddress.getByName("::1"),
                socketAddress.getAddress());
    }

    @Test
    public void testLocalHostV4Revert() throws Exception {
        String uaddr = "127.0.0.1.203.81";
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName("127.0.0.1"),52049);

        assertEquals("reverce convertion failed", uaddr,
                InetSocketAddresses.uaddrOf(socketAddress));
    }
}
