package org.dcache.util;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

@SuppressWarnings("deprecation")
public class IPMatcherTest {

    // FIXME: Depends on DNS entry
    @Test
    public void testHostWildcartByIp() throws UnknownHostException {

        boolean match = IPMatcher.match("*.desy.de", InetAddress.getByName("nairi.desy.de"));
        assertTrue("failed to match host by domain", match);
    }

    @Test
    public void testIpGlobMatchWithNetmask() throws UnknownHostException {
        boolean match = IPMatcher.match("131.169.214.0/24", InetAddress.getByName("131.169.214.1"));

        assertTrue(match);
    }

    @Test
    public void testIpGlobMatch() throws UnknownHostException {
        boolean match = IPMatcher.match("131.169.214.1", InetAddress.getByName("131.169.214.1"));

        assertTrue(match);
    }

    @Test
    public void testIPv6IpGlobMatchWithNetmask() throws UnknownHostException {
        boolean match = IPMatcher.match("fe80::0:0:0:0/64", InetAddress.getByName("fe80::BAD:F00D:BAD:F00D"));

        assertTrue(match);
    }

    @Test
    public void testIPv6IpGlobMatch() throws UnknownHostException {
        boolean match = IPMatcher.match("fe80::BAD:F00D:BAD:F00D", InetAddress.getByName("fe80::BAD:F00D:BAD:F00D"));

        assertTrue(match);
    }

    @Test
    public void testIpBased() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("131.169.214.0") ,
                InetAddress.getByName("131.169.40.255") ,
                16);

        assertTrue("Failed to match host with netmask.", match);

        match = IPMatcher.match(InetAddress.getByName("192.168.0.1"), InetAddress.getByName("192.168.0.1"), 0);

        assertTrue("Failed to match host with 0-netmask.", match);
    }

    @Test
    public void testIpBasedNegative() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("131.169.214.0") ,
                InetAddress.getByName("131.169.40.255") ,
                24);

        assertFalse("Match should have failed.", match);
    }

    @Test
    public void testIpV6SuccessfulIpNetMatching() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("fe80::BAD:F00D:BAD:F00D") ,
                InetAddress.getByName("fe80::0:0:0:0"),
                64);

        assertTrue("Failed to match host with netmask.", match);

        match = IPMatcher.match(InetAddress.getByName("feed:bad:f00d:feed:bad:f00d:feed:f00d"), InetAddress.getByName("feed:bad:f00d:feed:bad:f00d:feed:f00d"), 0);

        assertTrue("Failed to match host with 0-netmask.", match);
    }

    @Test
    public void testIpV6SuccessfulIpNetMatchingFractionedMask() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("fe80::3FF:F00D:BAD:F00D"),
                InetAddress.getByName("fe80::0:0:0:0"),
                70);

        assertTrue("Failed to match host with netmask.", match);
    }

    @Test
    public void testIpV6FailedIpNetMatchingFractionedMask() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("fe80::4FF:F00D:BAD:F00D"),
                InetAddress.getByName("fe80::0:0:0:0"),
                70);

        assertFalse("Match should have failed.", match);
    }

    @Test
    public void testIpV6FailedIpNetMatching() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("fe80::BAD:F00D:BAD:F00D") ,
                InetAddress.getByName("fe80::0:0:0:0"),
                96);

        assertFalse("Match should have failed.", match);
    }

    @Test
    public void testMixed() throws UnknownHostException {

        boolean match = IPMatcher.match( "localhost" ,
                InetAddress.getByName("127.0.0.1") );

        assertTrue("Failed to match localhost.", match);
    }

    @Test
    public void testMatchCompatibleIPv6Address() throws UnknownHostException {
        boolean match = IPMatcher.match(InetAddress.getByName("::ffff:192.168.0.3"), InetAddress.getByName("192.168.0.0"), 24);

        assertTrue("Failed to match with compatible IPv6 address.", match);
    }

    @Test
    public void testMatchBothCompatibleIPv6Addresses() throws UnknownHostException {
        boolean match = IPMatcher.match(InetAddress.getByName("::ffff:192.168.0.3"), InetAddress.getByName("::ffff:192.168.0.0"), 120);

        assertTrue("Failed to match compatible IPv6 addresses.", match);
    }

    @Test
    public void testMatchWithCompatibleIPv6Subnet() throws UnknownHostException {
        boolean match = IPMatcher.match(InetAddress.getByName("192.168.0.3"), InetAddress.getByName("::ffff:192.168.0.0"), 120);

        assertTrue("Failed to match compatible IPv6 subnet.", match);
    }

    @Test
    public void testMatchWithCompatibleIPv6SubnetHexNotation() throws UnknownHostException {
        boolean match = IPMatcher.match(InetAddress.getByName("192.168.0.3"), InetAddress.getByName("::ffff:c0a8:0"), 120);

        assertTrue("Failed to match compatible IPv6 subnet.", match);
    }

    @Test
    public void testMatchAny() throws UnknownHostException {

        boolean match = IPMatcher.matchAny(new InetAddress[]
                { InetAddress.getByName("131.169.213.1"), InetAddress.getByName("131.169.215.1") },
                InetAddress.getByName("131.169.214.0"), 24);

        assertFalse(match);

        match = IPMatcher.matchAny(new InetAddress[]
                { InetAddress.getByName("131.169.213.1"), InetAddress.getByName("131.169.214.1"), InetAddress.getByName("131.169.215.1") },
                InetAddress.getByName("131.169.214.0"), 24);

        assertTrue(match);
    }

    @Test
    public void testMaskZeroMatchesAll() throws UnknownHostException {
        boolean match = IPMatcher.match(InetAddress.getByName("1.2.3.4"), InetAddress.getByName("9.8.7.6"), 0);

        assertTrue(match);

        match = IPMatcher.match(InetAddress.getByName("ffff::1234"), InetAddress.getByName("9.8.7.6"), 0);

        assertTrue(match);

        match = IPMatcher.match(InetAddress.getByName("9.8.7.6"), InetAddress.getByName("ffff::1234"), 0);

        assertTrue(match);

        match = IPMatcher.match(InetAddress.getByName("aaaa::4321"), InetAddress.getByName("ffff::1234"), 0);

        assertTrue(match);
    }

    @Test
    public void testMatchHostname() throws UnknownHostException
    {
        assertTrue(IPMatcher.matchHostname("nairi.desy.de", InetAddress.getByName("nairi.desy.de"), 24));

        assertFalse(IPMatcher.matchHostname("nairi.desy.de", InetAddress.getByName("cern.ch"), 24));
    }

    @Test
    public void testMatchCidrPattern() throws UnknownHostException {
        assertTrue(IPMatcher.matchCidrPattern(InetAddress.getByName("192.168.0.25"), "192.168.0.0/24"));
        assertFalse(IPMatcher.matchCidrPattern(InetAddress.getByName("192.168.1.25"), "192.168.0.0/24"));
    }

    @Test
    public void testMaskAddress() throws UnknownHostException {
        assertEquals("255.255.255.255", IPMatcher.maskInetAddress(InetAddress.getByName("255.255.255.255"), 32).getHostAddress());
        assertEquals("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", IPMatcher.maskInetAddress(InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), 128).getHostAddress());

        assertEquals("0.0.0.0", IPMatcher.maskInetAddress(InetAddress.getByName("255.255.255.255"), 0).getHostAddress());
        assertEquals("0:0:0:0:0:0:0:0", IPMatcher.maskInetAddress(InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), 0).getHostAddress());

        assertEquals("255.255.255.128", IPMatcher.maskInetAddress(InetAddress.getByName("255.255.255.255"), 25).getHostAddress());
        assertEquals("ffff:ffff:ffff:ffff:fff8:0:0:0", IPMatcher.maskInetAddress(InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), 77).getHostAddress());
    }
}
