package org.dcache.gplazma.loader;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import static org.junit.Assert.*;

public class ResourceBlockingClassLoaderTests {

    public static final String RESOURCE_PATH = "META-INF/somedata.xml";

    Utf8DataClassLoader _resourceLoader;
    ResourceBlockingClassLoader _blockingLoader;

    @Before
    public void setUp() {
        _resourceLoader = new Utf8DataClassLoader(RESOURCE_PATH);
        _resourceLoader.addResource( "<?xml version='1.0'?><data/>");

        _blockingLoader = new ResourceBlockingClassLoader(_resourceLoader);
    }

    @Test
    public void testDefaultNonBlocking() {
        assertFalse(_blockingLoader.getIsBlocking());
        assertCanGetData();
    }

    @Test
    public void testSetBlocking() {
        _blockingLoader.setIsBlocking( true);
        assertTrue(_blockingLoader.getIsBlocking());
        assertCannotGetData();
    }

    @Test
    public void testSetBlockingThenNonblocking() {
        _blockingLoader.setIsBlocking( true);
        _blockingLoader.setIsBlocking( false);
        assertFalse(_blockingLoader.getIsBlocking());
        assertCanGetData();
    }

    /*
     * SUPPORT METHODS
     */

    private void assertCanGetData() {
        URL url = _blockingLoader.getResource(RESOURCE_PATH);
        assertNotNull("checking getResource on resource-path", url);

        Enumeration<URL> e;
        try {
            e = _blockingLoader.getResources(RESOURCE_PATH);
        } catch (IOException e1) {
            fail("received IOException: " + e1.getMessage());
            return;
        }

        assertTrue("checking getResources on resource-path", e.hasMoreElements());
    }

    private void assertCannotGetData() {
        URL url = _blockingLoader.getResource(RESOURCE_PATH);
        assertNull("checking getResource on resource-path", url);

        Enumeration<URL> e;
        try {
            e = _blockingLoader.getResources(RESOURCE_PATH);
        } catch (IOException e1) {
            fail("received IOException: " + e1.getMessage());
            return;
        }

        assertFalse("checking getResources on resource-path", e.hasMoreElements());
    }
}
