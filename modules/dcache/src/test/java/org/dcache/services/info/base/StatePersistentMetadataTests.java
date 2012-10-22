package org.dcache.services.info.base;


import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class StatePersistentMetadataTests {
	
	StatePersistentMetadata _top;

	Map<String,String> _fooMetadata;
	Map<String,String> _fooBarMetadata;
	Map<String,String> _bazStarMetadata;
	Map<String,String> _topMetadata;
	
	StatePath _fooPath;
	StatePath _fooBarPath;
	StatePath _bazPath;
	StatePath _bazStarPath;
	StatePath _bazRndPath;
	StatePath _topPath;

	/**
	 * Configure a standard StatePersistentMetadata tree
	 */
    @Before
	public void setUp() {
		_top = new StatePersistentMetadata();
		
		// Our default metadata
		_fooMetadata = new HashMap<>();
		_fooBarMetadata = new HashMap<>();
		_bazStarMetadata = new HashMap<>();
		_topMetadata = new HashMap<>();
		
		_fooMetadata.put( "key1_1", "val1_1");
		_fooMetadata.put( "key1_2", "val1_2");
		_fooMetadata.put( "key1_3", "val1_3");

		_fooBarMetadata.put( "key2_1", "val2_1");
		_fooBarMetadata.put( "key2_2", "val2_2");
		_fooBarMetadata.put( "key2_3", "val2_3");

		_bazStarMetadata.put( "key3_1", "val3_1");
		_bazStarMetadata.put( "key3_2", "val3_2");
		_bazStarMetadata.put( "key3_3", "val3_3");
		
		_topMetadata.put( "key4_1", "val4_1");
		_topMetadata.put( "key4_2", "val4_2");
		_topMetadata.put( "key4_3", "val4_3");
		
		_fooPath = new StatePath( "foo");
		_fooBarPath = _fooPath.newChild( "bar");
		_bazPath = new StatePath( "baz");
		_bazStarPath = _bazPath.newChild( "*");
		_bazRndPath = _bazPath.newChild("random"); // useful for checking wildcard match.
		_topPath = null;  // yes, this is a no-op.
				
		_top.add( _fooPath, _fooMetadata);
		_top.add( _fooBarPath, _fooBarMetadata);
		_top.add( _bazStarPath, _bazStarMetadata);
		_top.add( _topPath, _topMetadata);
	}
	
	
	@Test
	public void testStatePersistentMetadata() {
		
		// Create a new  StatePersistentMetadata node and check it looks OK.
		StatePersistentMetadata spm = new StatePersistentMetadata();
		
		Map<String, String> emptyPayload= spm.getMetadata();
		
		assertTrue( "initial payload is not empty", emptyPayload == null || emptyPayload.isEmpty());
	}
	

	@Test
	public void testGetChild() {

		// Check we can find the children we expect
		StatePersistentMetadata fooBranch = getMetadataBranch( _fooPath);
		assertNotNull( "Cannot find branch \""+_fooPath.toString()+"\"", fooBranch);

		StatePersistentMetadata barBranch = getMetadataBranch( _fooBarPath);
		assertNotNull( "Cannot find branch \""+_fooBarPath.toString()+"\"", barBranch);
		
		StatePersistentMetadata bazBranch = getMetadataBranch( _bazPath);
		assertNotNull( "Cannot find branch \""+_bazPath.toString()+"\"", bazBranch);
		
		StatePath rndPath = _bazPath.newChild("random");
		StatePersistentMetadata rndBranch = getMetadataBranch( rndPath);
		assertNotNull( "Cannot find branch \"" + rndPath.toString()+"\"", rndBranch);
		
		// Check we don't find the children we don't expect to find.
		StatePath testPath = rndPath.newChild("other");
		assertNull( "Unexpectedly found metadata at \""+testPath.toString()+"\"", getMetadataBranch( testPath));

		testPath = new StatePath( "other");
		assertNull( "Unexpectedly found metadata at \""+testPath.toString()+"\"", getMetadataBranch( testPath));
	}
	
		
	

	@Test
	public void testAdd() {		
		// Try to provoke some ill-tempered response
		_top.add( null, null);
		_top.add( _fooPath, null);
		
		// Add data to top-level metadata
		Map<String,String> update = new HashMap<>();
		
		update.put("extra1", "extra_value1");
		update.put("extra2", "extra_value2");
		
		// Add this data to a number of places
		_top.add( null, update);
		_top.add( _fooPath, update);
		_top.add( _bazStarPath, update);
		
		// Test that both the original data and the new data is available
		assertTrue( "Failed to updated metadata for (top)", metadataContainsBoth( getMetadataBranch( _topPath), _topMetadata, update));
		assertTrue( "Failed to updated metadata for foo",  metadataContainsBoth( getMetadataBranch( _fooPath), _fooMetadata, update));
		assertTrue( "Failed to updated metadata for baz.*", metadataContainsBoth( getMetadataBranch( _bazRndPath), _bazStarMetadata, update));		
	}
	

	@Test
	public void testGetMetadata() {		
		// Check we can get suitable values back.
		StatePersistentMetadata fooBranch = getMetadataBranch( _fooPath);
		assertNotNull( "Cannot find branch \""+_fooPath.toString()+"\"", fooBranch);

		Map<String,String> fooMetadata = fooBranch.getMetadata();
		assertEquals( "Cannot find same metadata as placed at \"foo\"", _fooMetadata, fooMetadata);
		
		StatePersistentMetadata barBranch = getMetadataBranch( _fooBarPath);
		assertNotNull( "Cannot find branch \""+_fooBarPath.toString()+"\"", barBranch);
		
		Map<String,String> barMetadata = barBranch.getMetadata();
		assertEquals( "Cannot find same metadata as placed at \"foo.bar\"", _fooBarMetadata, barMetadata);
		
		StatePersistentMetadata bazBranch = getMetadataBranch( _bazPath);
		assertNotNull( "Cannot find branch \""+_bazPath.toString()+"\"", bazBranch);
		
		StatePersistentMetadata rndBranch = bazBranch.getChild( "random");
		assertNotNull( "Cannot find branch \"random\", child of baz branch.", rndBranch);
		
		Map<String,String> rndMetadata = rndBranch.getMetadata();
		assertEquals( "Cannot find same metadata as placed at \"baz.random\"", _bazStarMetadata, rndMetadata);

		Map<String,String> topMetadata = _top.getMetadata();
		assertEquals( "Cannot find same metadata as placed at top-level", _topMetadata, topMetadata);
	}
	

	@Test
	public void testAddDefault() {
		StatePersistentMetadata top = new StatePersistentMetadata();
		
		top.addDefault();
		
		// What to check?  Just that someone was added?
		
	}

	
	
	/**
	 *   P R I V A T E   M E T H O D S
	 */
	
	/**
	 * Check that the StatePersistentMetadata object has all of the originalMetadata mappings in addition
	 * to the additionalMetadata.
	 * @param spm
	 * @param firstMetadata
	 * @param secondMetadata
	 * @return true if the StatePersistentMetadata object is as expected, false otherwise.
	 */
	private boolean metadataContainsBoth(StatePersistentMetadata spm, Map<String, String> firstMetadata, Map<String, String> secondMetadata) {

		Map<String,String> expectedMetadata = new HashMap<>();
		
		expectedMetadata.putAll( firstMetadata);
		expectedMetadata.putAll( secondMetadata);
		
		return expectedMetadata.equals( spm.getMetadata());		
	}

	/**
	 * Scan down a StatePersistentMetadata by calling getChild() on each element in a StatePath
	 * in turn
	 * @param top
	 * @param path
	 * @return
	 */
	private StatePersistentMetadata getMetadataBranch( StatePath path) {
		StatePersistentMetadata currentBranch = _top;
		StatePath currentPath = path;
		
		while( currentPath != null) {
			assertNotNull( "failed to find SPM for "+path.toString() + " ( "+currentPath.toString()+" left)", currentBranch);
			
			String nextElement = currentPath.getFirstElement();
			
			currentPath = currentPath.childPath();
			currentBranch=currentBranch.getChild( nextElement);
		}
		
		return currentBranch;
	}

}
