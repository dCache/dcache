package org.dcache.tests.ftp;


import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.ActiveAdapter;
import org.dcache.util.PortRange;

public class ActiveAdapterTest {

    private ActiveAdapter activeAdapter;

    @Before
    public void setUp() throws IOException {
        activeAdapter = new ActiveAdapter(new PortRange(0), null, 0);
    }

    @After
    public void  tearDown() {
        activeAdapter.close();
    }


    @Test
    public void testListenPort() throws Exception {

        assertTrue("Failed to create a listen port", activeAdapter.getPoolListenerPort() > 0 );

    }

    @Test
    public void testIsAlive() {

        assertFalse("Can't be alive while not started", activeAdapter.isAlive() );

        activeAdapter.start();

        assertTrue("Failed to start", activeAdapter.isAlive() );

    }

}
