package org.dcache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.dcache.util.Subnet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SubnetTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

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

    @Test
    public void testToString() {
        Subnet subnet = Subnet.create();
        assertEquals("ANY", subnet.toString());

        subnet = Subnet.create("192.168.0.0/24");
        assertEquals("192.168.0.0/24", subnet.toString());

        subnet = Subnet.create("192.168.0.0");
        assertEquals("192.168.0.0/32", subnet.toString());

        subnet = Subnet.create("::1");
        assertEquals("::1/128", subnet.toString());

        subnet = Subnet.create("2001:0:0::10:10/96");
        assertEquals("2001::10:10/96", subnet.toString());

        subnet = Subnet.create("0:11:0:11::1");
        assertEquals("0:11:0:11::1/128", subnet.toString());
    }
}
