package org.dcache.util;

import org.junit.Test;
import org.springframework.util.SerializationUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.net.InetAddresses.forString;
import static org.junit.Assert.*;

public class SubnetTest {

    @Test
    public void testCreateWithInetAddressAndMask() throws UnknownHostException {
        Subnet subnet = Subnet.create(forString("192.168.0.0"), 24);
        assertTrue(subnet.containsHost("192.168.0.1"));
        assertTrue(subnet.contains(forString("192.168.0.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {forString("134.102.101.100"), forString("192.168.0.1"), forString("127.0.0.1")}));
        assertFalse(subnet.containsHost("192.168.1.1"));
        assertFalse(subnet.contains(forString("192.168.1.1")));
        assertFalse(subnet.containsAny(new InetAddress[] {forString("134.102.101.100"), forString("127.0.0.1")}));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateStringWithInvalidCidrPattern() {
        assertFalse(Subnet.isValid("something"));
        Subnet.create("something");
    }

    @Test
    public void testCreateStringWithValidCidrPatternIPv4() throws UnknownHostException {
        assertTrue(Subnet.isValid("192.168.0.0/24"));
        Subnet subnet = Subnet.create("192.168.0.0/24");
        assertTrue(subnet.containsHost("192.168.0.1"));
        assertTrue(subnet.contains(forString("192.168.0.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {forString("134.102.101.100"), forString("192.168.0.1"), forString("127.0.0.1")}));
        assertFalse(subnet.containsHost("192.168.1.1"));
        assertFalse(subnet.contains(forString("192.168.1.1")));
        assertFalse(subnet.containsAny(new InetAddress[] {forString("134.102.101.100"), forString("127.0.0.1")}));
    }

    @Test
    public void testCreate() throws UnknownHostException {
        Subnet subnet = Subnet.create();
        assertTrue(subnet.containsHost("192.168.0.1"));
        assertTrue(subnet.contains(forString("192.168.0.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {forString("134.102.101.100"), forString("192.168.0.1"), forString("127.0.0.1")}));
        assertTrue(subnet.containsHost("192.168.1.1"));
        assertTrue(subnet.contains(forString("192.168.1.1")));
        assertTrue(subnet.containsAny(new InetAddress[] {forString("134.102.101.100"), forString("127.0.0.1")}));
    }

    @Test(expected=NumberFormatException.class)
    public void testCidrWithInvalidMask() {
        assertFalse(Subnet.isValid("1.2.3.4/abc"));
        Subnet.create("1.2.3.4/abc");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCidrWithInvalidAddress() {
        assertFalse(Subnet.isValid("foobar/22"));
        Subnet.create("foobar/22");
    }

    @Test
    public void testToString() {
        Subnet subnet = Subnet.create();
        assertEquals("all", subnet.toString());

        testCreateToString("::/0");
        testCreateToString("0.0.0.0/0");
        testCreateToString("0.0.0.0/0.0.0.0", "0.0.0.0/0");
        testCreateToString("192.168.0.0/24");
        testCreateToString("192.168.0.0", "192.168.0.0/32");
        testCreateToString("192.168.0.0/32");
        testCreateToString("192.168.0.0/255.255.255.255", "192.168.0.0/32");
        testCreateToString("::1", "::1/128");
        testCreateToString("2000::00ff:ff00", "2000::ff:ff00/128");
        testCreateToString("2001:0001::00ff:ff00", "2001:1::ff:ff00/128");

        // special addresses starting with 2002 are mapped to IPv4
        // using bytes 2 to 5 (starting counting at 0):
        testCreateToString("2002:0102:0304::00ff:ff00", "1.2.3.4/32");
        testCreateToString("2002:0102:0304::00ff:ff00/112", "1.2.0.0/16");
        testCreateToString("2003::00ff:ff00", "2003::ff:ff00/128");

        // compatible ipv4 addresses:
        testCreateToString("::192.168.0.0/120", "192.168.0.0/24");
        testCreateToString("::c0a8:0/120", "192.168.0.0/24");
        testCreateToString("::c0a8:1", "192.168.0.1/32");

        // mapped ipv4 addresses:
        testCreateToString("::ffff:192.168.0.0/120", "192.168.0.0/24");
        testCreateToString("::ffff:192.168.0.0/128", "192.168.0.0/32");
        testCreateToString("::ffff:192.168.0.0/64", "192.168.0.0/0");
        testCreateToString("0:0:0::ffff:192.168.0.0/127", "192.168.0.0/31");
        testCreateToString("0:0:0::ffff:192.168.0.0/128", "192.168.0.0/32");
        testCreateToString("1234:1234:1234:1234:1234:1234:1234:1234/96",
                "1234:1234:1234:1234:1234:1234::/96");
        testCreateToString("1234:1234:1234::1234:1234:1234/96",
                "1234:1234:1234::1234:0:0/96");
        testCreateToString("0:11:0:11::1", "0:11:0:11::1/128");
        testCreateToString("00ff:ff:00ff:00ff:00ff::1", "ff:ff:ff:ff:ff::1/128");
        testCreateToString("192.168.0.0/255.255.255.0", "192.168.0.0/24");
        testCreateToString("::ffff:192.168.0.0/128", "192.168.0.0/32");
    }

    private void testCreateToString(String source)
    {
        assertTrue(Subnet.isValid(source));
        assertCreateToString(source, source);
    }

    private void testCreateToString(String source, String expected)
    {
        assertTrue(Subnet.isValid(source));
        assertTrue(Subnet.isValid(expected));
        assertCreateToString(source, expected);
    }

    private void assertCreateToString(String source, String expected)
    {
        assertEquals(expected, Subnet.create(source).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenCreateIsCalledWithDocumentationAddress() {
        // special addresses for documentation starting with 2001:db8
        // are mapped to IPv4 and inverted:
        assertFalse(Subnet.isValid("2001:db8::00ff:ff00"));
        Subnet.create("2001:db8::00ff:ff00");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenCreateIsCalledWithToredoAddress() {
        // special addresses for documentation starting with 2001:db8
        // are mapped to IPv4 and inverted:
        assertFalse(Subnet.isValid("2001::00ff:ff00"));
        Subnet.create("2001::00ff:ff00");
    }

    @Test
    public void serializeSubnetTest() {
        Subnet original = Subnet.create();
        Object copy = SerializationUtils.deserialize(
                SerializationUtils.serialize(original));

        assertTrue(copy instanceof Subnet);
        assertEquals(original, copy);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionForTooBigMaskForIPv4Address() {
        assertFalse(Subnet.isValid("0.0.0.0/33"));
        Subnet.create("0.0.0.0/33");
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionForTooBigMaskForIPv6Address() {
        assertFalse(Subnet.isValid("::/129"));
        Subnet.create("::/129");
    }

    @Test
    public void testMatch() throws UnknownHostException {
        assertTrue(matches("0.0.0.0/0", "131.169.252.76"));
        assertTrue(matches("0.0.0.0/0.0.0.0", "131.169.252.76"));
        assertTrue(matches("128.0.0.0/1", "131.169.252.76"));
        assertTrue(matches("130.0.0.0/7", "131.169.252.76"));
        assertTrue(matches("131.0.0.0/8", "131.169.252.76"));
        assertTrue(matches("131.128.0.0/9", "131.169.252.76"));
        assertTrue(matches("131.168.0.0/15", "131.169.252.76"));
        assertTrue(matches("131.169.0.0/16", "131.169.252.76"));
        assertTrue(matches("131.169.128.0/17", "131.169.252.76"));
        assertTrue(matches("131.169.252.0/23", "131.169.252.76"));
        assertTrue(matches("131.169.252.0/24", "131.169.252.76"));
        assertTrue(matches("131.169.252.0/25", "131.169.252.76"));
        assertTrue(matches("131.169.252.76/31", "131.169.252.76"));
        assertTrue(matches("131.169.252.76/32", "131.169.252.76"));

        assertFalse(matches("128.0.0.0/1", "127.169.252.76"));
        assertFalse(matches("130.0.0.0/7", "1.169.253.76"));
        assertFalse(matches("131.0.0.0/8", "132.169.252.76"));
        assertFalse(matches("131.128.0.0/9", "131.127.128.76"));
        assertFalse(matches("131.168.0.0/15", "131.167.252.76"));
        assertFalse(matches("131.169.0.0/16", "131.168.252.76"));
        assertFalse(matches("131.169.128.0/17", "131.169.127.128"));
        assertFalse(matches("131.169.252.0/23", "131.169.251.76"));
        assertFalse(matches("131.169.252.0/24", "131.169.253.76"));
        assertFalse(matches("131.169.252.0/25", "131.169.252.128"));
        assertFalse(matches("131.169.252.76/31", "131.169.251.77"));
        assertFalse(matches("131.169.252.76/32", "131.169.252.75"));
    }

    private boolean matches(String subnetLabel, String ip)
    {
        assertTrue(Subnet.isValid(subnetLabel));
        assertTrue(Subnet.isValid(ip));
        Subnet subnet = Subnet.create(subnetLabel);
        InetAddress address;
        try {
            address = forString(ip);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
        return subnet.contains(address);
    }

}
