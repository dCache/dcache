/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package diskCacheV111.poolManager;

import org.junit.Before;
import java.net.UnknownHostException;
import org.junit.Test;
import static org.junit.Assert.*;

public class NetHandlerTest {

    private static final String IPV4_HOSTNAME_1 = "192.168.1.1";
    private static final String IPV4_HOSTNAME_2 = "192.168.1.2";
    private static final String IPV4_MASKED_HOSTNAME = "192.168.1.0";
    private static final String IPV4_NETMASK = "24";
    private static final String IPV4_SUBNET = IPV4_HOSTNAME_1+"/"+IPV4_NETMASK;
    private static final String IPV4_MASKED_SUBNET = IPV4_MASKED_HOSTNAME+"/"+IPV4_NETMASK;

    private static final String IPV6_HOSTNAME_1 = "feed::bad:f00d";
    private static final String IPV6_HOSTNAME_2 = "feed::bad:f00b";
    private static final String IPV6_MASKED_HOSTNAME = "feed::bad:f000";
    private static final String IPV6_NETMASK = "120";
    private static final String IPV6_SUBNET = IPV6_HOSTNAME_1+"/"+IPV6_NETMASK;
    private static final String IPV6_MASKED_SUBNET = IPV6_MASKED_HOSTNAME+"/"+IPV6_NETMASK;

    NetHandler netHandler;

    @Before
    public void setUp()
    {
        netHandler = new NetHandler();
        netHandler.add(new NetUnit(IPV4_SUBNET));
        netHandler.add(new NetUnit(IPV6_SUBNET));
    }

    /**
     * Test of add method, of class NetHandler.
     */
    @Test
    public void testAdd() throws UnknownHostException {
        assertNetHandlerContains(new NetUnit(IPV4_SUBNET));
        assertNetHandlerContains(new NetUnit(IPV6_SUBNET));
    }

    /**
     * Test of find method, of class NetHandler.
     */
    @Test
    public void testFind() throws UnknownHostException {
        NetUnit result = netHandler.find(new NetUnit(IPV4_MASKED_SUBNET));
        assertEquals(IPV4_MASKED_SUBNET, result.getCanonicalName());
        result = netHandler.find(new NetUnit(IPV6_MASKED_SUBNET));
        assertEquals(IPV6_MASKED_SUBNET, result.getCanonicalName());
    }

    /**
     * Test of clear method, of class NetHandler.
     */
    @Test
    public void testClear() throws UnknownHostException {
        netHandler.clear();
        assertNetHandlerDoesNotContain(new NetUnit(IPV4_SUBNET));
        assertNetHandlerDoesNotContain(new NetUnit(IPV6_SUBNET));
    }

    /**
     * Test of remove method, of class NetHandler.
     */
    @Test
    public void testRemove() throws UnknownHostException {
        netHandler.remove(new NetUnit(IPV4_MASKED_SUBNET));
        netHandler.remove(new NetUnit(IPV6_MASKED_SUBNET));
        assertNetHandlerDoesNotContain(new NetUnit(IPV4_SUBNET));
        assertNetHandlerDoesNotContain(new NetUnit(IPV6_SUBNET));
    }

    /**
     * Test of match method, of class NetHandler.
     */
    @Test
    public void testMatch() throws Exception {
        NetUnit result = netHandler.match(IPV4_HOSTNAME_2);
        assertEquals(IPV4_MASKED_SUBNET, result.getCanonicalName());
        result = netHandler.match(IPV6_HOSTNAME_2);
        assertEquals(IPV6_MASKED_SUBNET, result.getCanonicalName());
    }

    /**
     * Test of bitsToString method, of class NetHandler.
     */
    @Test
    public void testBitsToString() {
        int bits = Integer.parseInt(IPV4_NETMASK);
        String expResult = IPV4_NETMASK;
        String result = netHandler.bitsToString(bits);
        assertEquals(expResult, result);
        bits = Integer.parseInt(IPV6_NETMASK);
        expResult = IPV6_NETMASK;
        result = netHandler.bitsToString(bits);
        assertEquals(expResult, result);
    }

    @Test
    public void testAddMatch() throws UnknownHostException {
        assertCIDRSubnetMatches("0.0.0.0/0", "131.169.252.76");
        assertCIDRSubnetMatches("128.0.0.0/1", "131.169.252.76");
        assertCIDRSubnetMatches("130.0.0.0/7", "131.169.252.76");
        assertCIDRSubnetMatches("131.0.0.0/8", "131.169.252.76");
        assertCIDRSubnetMatches("131.128.0.0/9", "131.169.252.76");
        assertCIDRSubnetMatches("131.168.0.0/15", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.0.0/16", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.128.0/17", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.252.0/23", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.252.0/24", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.252.0/25", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.252.76/31", "131.169.252.76");
        assertCIDRSubnetMatches("131.169.252.76/32", "131.169.252.76");
    }

    private void assertCIDRSubnetMatches(String subnet, String ip)
    {
        NetHandler nh = new NetHandler();
        NetUnit nu, matchedNu;
        try {
            nu = new NetUnit(subnet);
            nh.add(nu);
            matchedNu = nh.match(ip);
        } catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
        assertNotNull(matchedNu);
        assertEquals(nu.getCanonicalName(), matchedNu.getCanonicalName());
    }

    private void assertNetHandlerContains(NetUnit netUnit) {
        assertNotNull(netHandler.find(netUnit));
    }

    private void assertNetHandlerDoesNotContain(NetUnit netUnit) {
        assertNull(netHandler.find(netUnit));
    }
}
