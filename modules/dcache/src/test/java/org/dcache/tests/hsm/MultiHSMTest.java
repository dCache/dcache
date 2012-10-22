package org.dcache.tests.hsm;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import diskCacheV111.util.HsmLocation;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.OsmLocationExtractor;
import diskCacheV111.util.EnstoreLocationExtractor;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

public class MultiHSMTest extends junit.framework.TestCase {

	
	/**
	 * test for adding, storing and retrieving location URIs
	 * @throws Exception
	 */
	@Test
	public void testAddGet() throws Exception {
		
		String osmLocationMain = "osm://desy-main/?store=h1&bfid=1234";
		String osmLocationCopy = "osm://desy-copy/?store=h1&bfid=1234";
		String enstoreLocation = "enstore://fnal1/?store=h1&bfid=1234";
			
		StorageInfo storageInfo =  new GenericStorageInfo();
		
		
		storageInfo.addLocation( new URI(osmLocationMain));
		storageInfo.addLocation( new URI(osmLocationCopy));
		storageInfo.addLocation( new URI(enstoreLocation));
		
		assertEquals("Not all antries stored",3,  storageInfo.locations().size() );
		
	}
	
	/**
	 * test reaction of isStores() method when URIs are used
	 * @throws Exception
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testStore() throws Exception {
		
		StorageInfo storageInfo =  new GenericStorageInfo();
		assertEquals("SotrageInfo without URL shoul not declare itself as stored",false,  storageInfo.isStored() );
				
		// legacy case
		
		storageInfo.setIsStored(true);		
		assertEquals("SotrageInfo after  setIsStored(true) shold shoul declare itself as stored",true,  storageInfo.isStored() );

		// legacy case
		storageInfo.setIsStored(false);		
		assertEquals("SotrageInfo after  setIsStored(false) shold shoul not declare itself as stored",false,  storageInfo.isStored() );

		String osmLocationMain = "osm://desy-main/?store=h1&bfid=1234";
		storageInfo.addLocation( new URI(osmLocationMain));
		assertEquals("SotrageInfo with URL shoul declare itself as stored",true,  storageInfo.isStored() );
	}
	
	
	/**
	 * test to load location extractor depending on HSM type
	 * @throws Exception
	 */
	@Test
	public void testExtractor() throws Exception {
		
		String osmLocationMain = "osm://desy-main/?store=h1&bfid=1234";
		String osmLocationCopy = "osm://desy-copy/?store=h1&bfid=1234";
		String enstoreLocation = "enstore://fnal1/?store=h1&bfid=1234";
		
		StorageInfo storageInfo =  new GenericStorageInfo();
		
		
		storageInfo.addLocation( new URI(osmLocationMain));
		storageInfo.addLocation( new URI(osmLocationCopy));
		storageInfo.addLocation( new URI(enstoreLocation));		
		
		for(URI location: storageInfo.locations() ) {
			
			try {
				HsmLocation hsmLocation = HsmLocationExtractorFactory.extractorOf(location);
			}catch(IllegalArgumentException iae ) {
				fail(location.toString() + " : should to be valid");
			}
						
		}				
		
	}
	
	
	@Test
	public void testBadExtractor() throws Exception {
		
		String badLocation     = "exception://fnal1/?store=h1&bfid=1234";
		
		StorageInfo storageInfo =  new GenericStorageInfo();
						
		storageInfo.addLocation( new URI(badLocation));
		
		for(URI location: storageInfo.locations() ) {
			
			try {
				HsmLocation hsmLocation = HsmLocationExtractorFactory.extractorOf(location);
				fail("IllegalArgumentException shold be thrown on  unsupported hsm type");

			}catch(IllegalArgumentException iae ) {
				// OK
			}
						
		}				
		
	}
	
	/**
	 * test parsing OSM storage info. Test URI2levels and levels2URI conversion
	 * @throws Exception
	 */
	@Test
	public void testOsmLocationExtractor() throws Exception {
				
		URI location = new URI("osm://default/?store=h1&group=rawd07&bfid=1234");
		
		Map<Integer,String> levelData = new OsmLocationExtractor(location).toLevels();
		
		assertEquals("OSM storageInfoFormat uses only one level", 1, levelData.size() );
		assertTrue("OSM storageInfoFormat uses level 1 only", levelData.containsKey(1) );
		
		
		// reverse operation
		URI reverseLocation = new OsmLocationExtractor(levelData).location();
		
		assertEquals("reverse opration failed : ", location, reverseLocation );
	}
	
	/**
	 * test parsing OSM storage info. Test URI2levels and levels2URI conversion
	 * @throws Exception
	 */
	@Test
	public void testOsmLocationExtractorLevel2URI() throws Exception {						
		
		Map<Integer,String> levelData = new HashMap<>(1);
		levelData.put(1, "h1 raw08 12345 duplicate");
		
		URI location = new OsmLocationExtractor(levelData).location();
		
		assertTrue("OSM storageInfoFormat uses level 1 only", levelData.containsKey(1) );
						
		assertEquals("reverse opration failed : ", levelData.get(1), new OsmLocationExtractor(location).toLevels().get(1) );
	}	
	
	/**
	 * test parsing OSM storage info. Test URI2levels and levels2URI conversion
	 * @throws Exception
	 */
	@Test
	public void testEnstoreLocationExtractor() throws Exception {
				
		URI location = new URI("enstore://enstore/?volume=VOLUME&location=LOCATION&size=SIZE&origff=FAMILY" +
				"&origname=NAME&mapfile=MAP&pnfsid=PNFSID&pnfsidmap=PNFSIDMAP&bfid=BFID&drive=DRIVE&crc=CRC");
		
		Map<Integer,String> levelData = new EnstoreLocationExtractor(location).toLevels();
		
		assertEquals("ENSTORE storageInfoFormat uses level 1 and 2", 2, levelData.size() );
		assertTrue("ENSTORE storageInfoFormat should contain level 1", levelData.containsKey(1) );
		assertTrue("ENSTORE storageInfoFormat should contain level 4", levelData.containsKey(4) );
		
		// reverse operation
		URI reverseLocation = new EnstoreLocationExtractor(levelData).location();
		
		assertEquals("reverse opration failed : ", location, reverseLocation );
	}

}
