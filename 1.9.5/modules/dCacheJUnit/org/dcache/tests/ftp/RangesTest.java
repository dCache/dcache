package org.dcache.tests.ftp;

import org.junit.Test;

import diskCacheV111.util.Range;
import diskCacheV111.util.Ranges;

public class RangesTest extends junit.framework.TestCase {

	
	@Test
	public void testContinues() {
		
		Ranges ranges = new Ranges();
		ranges.addRange(new Range(20,30));
		ranges.addRange(new Range(0,10));
		ranges.addRange(new Range(9,22));
		
		assertTrue("Range(0-10,9-22,20-30) have to be continues", ranges.isContiguous());
		
	}

	
	@Test
	public void testNotContinues() {
		
		Ranges ranges = new Ranges();
		ranges.addRange(new Range(27,30));
		ranges.addRange(new Range(0,10));
		ranges.addRange(new Range(9,22));
		
		assertFalse("Range(0-10,9-22,27-30) should not be continues", ranges.isContiguous());
		
	}

}
