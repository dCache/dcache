package org.dcache.tests.util;

import junit.framework.TestCase;

import org.junit.Test;

import diskCacheV111.util.FsPath;

public class FsPathTest extends TestCase {

	@Test
	public void testFsPath() {
		FsPath path = new FsPath("/pnfs/desy.de");
		
		assertEquals("Incorrest path constructed", path.toString(), "/pnfs/desy.de");
		
		path.add("zeus/users/patrick");
		
		assertEquals("Incorrest path added", path.toString(), "/pnfs/desy.de/zeus/users/patrick");

		path.add("../trude");
		
		assertEquals(".. should change 'current'", path.toString(), "/pnfs/desy.de/zeus/users/trude");
		
		path.add("/");
		
		assertEquals("'/' should remove others", path.toString(), "/");
		
		path.add("pnfs/cern.ch");
		
		assertEquals("Incorrest path added", path.toString(), "/pnfs/cern.ch");
		
		path.add("./../././");
		assertEquals("Incorrest path calculated", path.toString(), "/pnfs");
	}
	
}
