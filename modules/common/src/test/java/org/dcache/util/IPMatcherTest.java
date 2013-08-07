package org.dcache.util;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.net.InetAddresses.forString;
import static org.junit.Assert.*;

public class IPMatcherTest {

    @Test
    public void testIpBased() throws UnknownHostException {


        boolean match = IPMatcher.match(forString("131.169.214.0"),
                InetAddress.getByName("131.169.40.255"),
                16);

        assertTrue("Failed to match host with netmask.", match);

        match = IPMatcher.match(forString("192.168.0.1"), forString("192.168.0.1"), 0);

        assertTrue("Failed to match host with 0-netmask.", match);
    }

    @Test
    public void testIpBasedNegative() throws UnknownHostException {


        boolean match = IPMatcher.match(forString("131.169.214.0"),
                forString("131.169.40.255"),
                24);

        assertFalse("Match should have failed.", match);
    }

    @Test
    public void testIpV6SuccessfulIpNetMatching() throws UnknownHostException {


        boolean match = IPMatcher.match( forString("fe80::BAD:F00D:BAD:F00D") ,
                forString("fe80::0:0:0:0"),
                64);

        assertTrue("Failed to match host with netmask.", match);

        match = IPMatcher.match(forString("feed:bad:f00d:feed:bad:f00d:feed:f00d"), forString("feed:bad:f00d:feed:bad:f00d:feed:f00d"), 0);

        assertTrue("Failed to match host with 0-netmask.", match);
    }

    @Test
    public void testIpV6SuccessfulIpNetMatchingFractionedMask() throws UnknownHostException {


        boolean match = IPMatcher.match( forString("fe80::3FF:F00D:BAD:F00D"),
                forString("fe80::0:0:0:0"),
                70);

        assertTrue("Failed to match host with netmask.", match);
    }

    @Test
    public void testIpV6FailedIpNetMatchingFractionedMask() throws UnknownHostException {


        boolean match = IPMatcher.match( forString("fe80::4FF:F00D:BAD:F00D"),
                forString("fe80::0:0:0:0"),
                70);

        assertFalse("Match should have failed.", match);
    }

    @Test
    public void testIpV6FailedIpNetMatching() throws UnknownHostException {


        boolean match = IPMatcher.match( forString("fe80::BAD:F00D:BAD:F00D") ,
                forString("fe80::0:0:0:0"),
                96);

        assertFalse("Match should have failed.", match);
    }

    @Test
    public void testMatchCompatibleIPv6Address() throws UnknownHostException {
        boolean match = IPMatcher.match(forString("::ffff:192.168.0.3"), forString("192.168.0.0"), 24);

        assertTrue("Failed to match with compatible IPv6 address.", match);
    }

    @Test
    public void testMatchBothCompatibleIPv6Addresses() throws UnknownHostException {
        boolean match = IPMatcher.match(forString("::ffff:192.168.0.3"), forString("::ffff:192.168.0.0"), 24);

        assertTrue("Failed to match compatible IPv6 addresses.", match);
    }

    @Test
    public void testMatchWithCompatibleIPv6Subnet() throws UnknownHostException {
        boolean match = IPMatcher.match(forString("192.168.0.3"), forString("::ffff:192.168.0.0"), 24);

        assertTrue("Failed to match compatible IPv6 subnet.", match);
    }

    @Test
    public void testMatchWithCompatibleIPv6SubnetHexNotation() throws UnknownHostException {
        boolean match = IPMatcher.match(forString("192.168.0.3"), forString("::ffff:c0a8:0"), 24);

        assertTrue("Failed to match compatible IPv6 subnet.", match);
    }

    @Test
    public void testMatchAny() throws UnknownHostException {

        boolean match = IPMatcher.matchAny(new InetAddress[]
                { forString("131.169.213.1"), forString("131.169.215.1") },
                forString("131.169.214.0"), 24);

        assertFalse(match);

        match = IPMatcher.matchAny(new InetAddress[]
                { forString("131.169.213.1"), forString("131.169.214.1"), forString("131.169.215.1") },
                forString("131.169.214.0"), 24);

        assertTrue(match);
    }

    @Test
    public void testMaskZeroMatchesAll() throws UnknownHostException {
        boolean match = IPMatcher.match(forString("1.2.3.4"), forString("9.8.7.6"), 0);
        assertTrue(match);

        match = IPMatcher.match(forString("ffff::1234"), forString("9.8.7.6"), 0);
        assertTrue(match);

        match = IPMatcher.match(forString("9.8.7.6"), forString("ffff::1234"), 0);
        assertTrue(match);

        match = IPMatcher.match(forString("aaaa::4321"), forString("ffff::1234"), 0);
        assertTrue(match);
    }

    @Test
    public void testMatchCidrPattern() throws UnknownHostException {
        assertTrue(IPMatcher.matchCidrPattern(forString("192.168.0.25"), "192.168.0.0/24"));
        assertFalse(IPMatcher.matchCidrPattern(forString("192.168.1.25"), "192.168.0.0/24"));
        assertTrue(IPMatcher.matchCidrPattern(forString("192.168.0.25"), "192.168.0.0/255.255.255.0"));
        assertFalse(IPMatcher.matchCidrPattern(forString("192.168.1.25"), "192.168.0.0/255.255.255.0"));
    }

    @Test
    public void testMaskAddress() throws UnknownHostException {
        assertEquals("255.255.255.255", IPMatcher.maskInetAddress(forString("255.255.255.255"), 32).getHostAddress());
        assertEquals("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", IPMatcher.maskInetAddress(forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), 128).getHostAddress());

        assertEquals("0.0.0.0", IPMatcher.maskInetAddress(forString("255.255.255.255"), 0).getHostAddress());
        assertEquals("0:0:0:0:0:0:0:0", IPMatcher.maskInetAddress(forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), 0).getHostAddress());

        assertEquals("255.255.255.128", IPMatcher.maskInetAddress(forString("255.255.255.255"), 25).getHostAddress());
        assertEquals("ffff:ffff:ffff:ffff:fff8:0:0:0", IPMatcher.maskInetAddress(forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), 77).getHostAddress());
    }
}
