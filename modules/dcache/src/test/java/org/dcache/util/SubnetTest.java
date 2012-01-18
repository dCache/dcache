package org.dcache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

public class SubnetTest {

    @Test
    public void testCreateWithInetAddressAndMask() throws UnknownHostException {
        Subnet subnet = Subnet.create(InetAddress.getByName("192.168.0.0"), 24);
        assertTrue(subnet.containsHost("192.168.0.1"));
        assertTrue(subnet.contains(InetAddress.getByName("192.168.0.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {InetAddress.getByName("134.102.101.100"), InetAddress.getByName("192.168.0.1"), InetAddress.getByName("cern.ch")}));
        assertFalse(subnet.containsHost("192.168.1.1"));
        assertFalse(subnet.contains(InetAddress.getByName("192.168.1.1")));
        assertFalse(subnet.containsAny(new InetAddress[] {InetAddress.getByName("134.102.101.100"), InetAddress.getByName("cern.ch")}));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateStringWithInvalidCidrPattern() {
        Subnet.create("something");
    }

    @Test
    public void testCreateStringWithValidCidrPatternIPv4() throws UnknownHostException {
        Subnet subnet = Subnet.create("192.168.0.0/24");
        assertTrue(subnet.containsHost("192.168.0.1"));
        assertTrue(subnet.contains(InetAddress.getByName("192.168.0.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {InetAddress.getByName("134.102.101.100"), InetAddress.getByName("192.168.0.1"), InetAddress.getByName("cern.ch")}));
        assertFalse(subnet.containsHost("192.168.1.1"));
        assertFalse(subnet.contains(InetAddress.getByName("192.168.1.1")));
        assertFalse(subnet.containsAny(new InetAddress[] {InetAddress.getByName("134.102.101.100"), InetAddress.getByName("cern.ch")}));
    }

    @Test
    public void testCreate() throws UnknownHostException {
        Subnet subnet = Subnet.create();
        assertTrue(subnet.containsHost("192.168.0.1"));
        assertTrue(subnet.contains(InetAddress.getByName("192.168.0.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {InetAddress.getByName("134.102.101.100"), InetAddress.getByName("192.168.0.1"), InetAddress.getByName("cern.ch")}));
        assertTrue(subnet.containsHost("192.168.1.1"));
        assertTrue(subnet.contains(InetAddress.getByName("192.168.1.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {InetAddress.getByName("134.102.101.100"), InetAddress.getByName("cern.ch")}));
    }

    @Test(expected=NumberFormatException.class)
    public void testCidrWithInvalidMask() {
        Subnet.create("1.2.3.4/abc");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCidrWithInvalidAddress() {
        Subnet.create("foobar/22");
    }

    @Test
    public void testToString() {
        Subnet subnet = Subnet.create();
        assertEquals("all", subnet.toString());

        subnet = Subnet.create("::/0");
        assertEquals("::/0", subnet.toString());

        subnet = Subnet.create("0.0.0.0/0");
        assertEquals("0.0.0.0/0", subnet.toString());

        subnet = Subnet.create("192.168.0.0/24");
        assertEquals("192.168.0.0/24", subnet.toString());

        subnet = Subnet.create("192.168.0.0");
        assertEquals("192.168.0.0/32", subnet.toString());

        subnet = Subnet.create("::1");
        assertEquals("::1/128", subnet.toString());

        subnet = Subnet.create("2000::00ff:ff00");
        assertEquals("2000::ff:ff00/128", subnet.toString());

        // special addresses for documentation starting with 2001:
        // are mapped to IPv4 and inverted:
        subnet = Subnet.create("2001::00ff:ff00");
        assertEquals("255.0.0.255/32", subnet.toString());

        // special addresses starting with 2002 are mapped to IPv4
        // using bytes 2 to 5 (starting counting at 0):
        subnet = Subnet.create("2002:0102:0304::00ff:ff00");
        assertEquals("1.2.3.4/32", subnet.toString());

        // special addresses starting with ::ffff: are mapped to IPv4
        // using bytes 12 to 16 (starting counting at 0):
        subnet = Subnet.create("::ffff:192.168.0.1");
        assertEquals("192.168.0.1/32", subnet.toString());

        subnet = Subnet.create("2003::00ff:ff00");
        assertEquals("2003::ff:ff00/128", subnet.toString());

        // embedded ipv4 addresses:
        subnet = Subnet.create("::ffff:192.168.0.0/120");
        assertEquals("192.168.0.0/24", subnet.toString());

        subnet = Subnet.create("::ffff:c0a8:0/120");
        assertEquals("192.168.0.0/24", subnet.toString());

        subnet = Subnet.create("::ffff:c0a8:1");
        assertEquals("192.168.0.1/32", subnet.toString());

        // compatible ipv4 addresses:
        subnet = Subnet.create("::192.168.0.0/120");
        assertEquals("192.168.0.0/24", subnet.toString());

        subnet = Subnet.create("::c0a8:0/120");
        assertEquals("192.168.0.0/24", subnet.toString());

        subnet = Subnet.create("::c0a8:1");
        assertEquals("192.168.0.1/32", subnet.toString());

        subnet = Subnet.create("1234:1234:1234:1234:1234:1234:1234:1234/96");
        assertEquals("1234:1234:1234:1234:1234:1234::/96", subnet.toString());

        subnet = Subnet.create("1234:1234:1234::1234:1234:1234/96");
        assertEquals("1234:1234:1234::1234:0:0/96", subnet.toString());

        subnet = Subnet.create("0:11:0:11::1");
        assertEquals("0:11:0:11::1/128", subnet.toString());

        subnet = Subnet.create("00ff:ff:00ff:00ff:00ff::1");
        assertEquals("ff:ff:ff:ff:ff::1/128", subnet.toString());
    }
}
