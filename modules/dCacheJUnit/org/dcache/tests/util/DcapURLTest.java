package org.dcache.tests.util;

import java.io.IOException;

import junit.framework.TestCase;
import org.junit.Test;

import diskCacheV111.util.DCapUrl;


public class DcapURLTest extends TestCase {

	
	@Test
	public void testDcapUrl() throws IllegalArgumentException, IOException {
		
		DCapUrl url = new DCapUrl( "dcap://dcachedoor0.desy.de:22225/pnfs/desy.de/h1/user/tigran/index.dat" ) ;
		
		assertEquals("Invalid protocol", "dcap", url.getProtocol() );
		assertEquals("Invalid protocol", "/pnfs/desy.de/h1/user/tigran/index.dat", url.getFilePart() );
		
	}

    @Test
    public void testDcapUrlWithOptions() throws IllegalArgumentException, IOException {

        DCapUrl url = new DCapUrl("dcap://dcachedoor0.desy.de:22225/pnfs/desy.de/h1/user/tigran/index.dat?filetype=raw");

        assertEquals("Invalid protocol", "dcap", url.getProtocol());
        assertEquals("Invalid protocol", "/pnfs/desy.de/h1/user/tigran/index.dat", url.getFilePart());

    }
}
