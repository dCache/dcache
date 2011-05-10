package org.dcache.chimera.nfs;


import java.net.InetAddress;
import java.net.UnknownHostException;

import org.dcache.chimera.nfs.IPMatcher;

import org.junit.Test;

import static org.junit.Assert.*;

public class IPMatcherTest {


    @Test
    public void testHostWildcart() {

        boolean match = IPMatcher.match("*.desy.de", "nairi.desy.de");
        assertTrue("failed to match host by domain", match);

    }

    @Test
    public void testHostWildcart1() {

        boolean match = IPMatcher.match("*", "nairi.desy.de");
        assertTrue("failed to match host by domain", match);

    }

    @Test
    public void testHostWildcart2() {

        boolean match = IPMatcher.match("n*.desy.de", "nairi.desy.de");
        assertTrue("failed to match host by domain", match);

    }


    @Test
    public void testHostWildcart3() {

        boolean match = IPMatcher.match("b*.desy.de", "nairi.desy.de");
        assertFalse("Invalid match of host by domain", match);

    }

    @Test
    public void testDomainWildcart() {

        boolean match = IPMatcher.match("nairi.*.de", "nairi.desy.de");
        assertTrue("failed to match host by domain", match);

    }


    @Test
    public void testDomainWildcart2() {

        boolean match = IPMatcher.match("nairi.d*.de", "nairi.desy.de");
        assertTrue("failed to match host by domain", match);

    }


    @Test
    public void testDomainWildcart3() {

        boolean match = IPMatcher.match("nairi.b*.de", "nairi.desy.de");
        assertFalse("Invalid to match host by domain", match);

    }


    @Test
    public void testExactMatch() {

        boolean match = IPMatcher.match("nairi.desy.de", "nairi.desy.de");
        assertTrue("failed to match host by domain", match);
    }

    /*
     * FIXME: frgale test - depends on DNS record
     */
    @Test
    public void testHostWildcartByIp() throws UnknownHostException {

        boolean match = IPMatcher.match("*.desy.de", InetAddress.getByName("nairi.desy.de"));
        assertTrue("failed to match host by domain", match);
    }


    @Test
    public void testHostWildcartOneChar() {
        boolean match = IPMatcher.match("h?.desy.de", "h1.desy.de");
        assertTrue("failed to match host by domain", match);
    }


    @Test
    public void testHostWildcartOneChar2() {
        boolean match = IPMatcher.match("h?.desy.de", "h11.desy.de");
        assertFalse("one one character have to match", match);
    }


    @Test
    public void testIpBased() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("131.169.214.0") ,
                InetAddress.getByName("131.169.40.255") ,
                16);

        assertTrue("failed to match host by netmask", match);
    }

    @Test
    public void testIpBasedNegative() throws UnknownHostException {


        boolean match = IPMatcher.match( InetAddress.getByName("131.169.214.0") ,
                InetAddress.getByName("131.169.40.255") ,
                24);

        assertFalse("Invalid ip to matched", match);
    }

    @Test
    public void testMixed() throws UnknownHostException {

        boolean match = IPMatcher.match( "localhost" ,
                InetAddress.getByName("127.0.0.1") );

        assertTrue("failed to match localhost", match);
    }

}
