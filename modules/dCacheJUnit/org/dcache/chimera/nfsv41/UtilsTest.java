/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.chimera.nfsv41;

import java.net.InetSocketAddress;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tigran
 */
public class UtilsTest {


    /**
     * Test of device2Address method, of class Utils.
     */
    @Test
    public void testDevice2Address() throws Exception {

        InetSocketAddress expResult = new InetSocketAddress("127.0.0.2", 2052);
        InetSocketAddress result = Utils.device2Address("127.0.0.2.8.4");
        assertEquals("address decodeing missmatch", expResult, result);

    }


}