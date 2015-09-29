package org.dcache.tests.ftp;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;

import org.dcache.ftp.proxy.ActiveAdapter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ActiveAdapterTest {

    private ActiveAdapter activeAdapter;

    @Before
    public void setUp() throws IOException {
        activeAdapter = new ActiveAdapter(InetAddress.getLoopbackAddress(), null, 0);
    }

    @After
    public void  tearDown() {
        activeAdapter.close();
    }


    @Test
    public void testListenPort() throws Exception {

        assertTrue("Failed to create a listen port", activeAdapter.getInternalAddress().getPort() > 0 );

    }

    @Test
    public void testIsAlive() {

        assertFalse("Can't be alive while not started", activeAdapter.isAlive() );

        activeAdapter.start();

        assertTrue("Failed to start", activeAdapter.isAlive() );

    }

}
