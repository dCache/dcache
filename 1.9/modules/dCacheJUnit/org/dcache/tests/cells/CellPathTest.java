package org.dcache.tests.cells;

import junit.framework.TestCase;


import org.junit.Test;

import dmg.cells.nucleus.CellPath;

public class CellPathTest extends TestCase {

	
	@Test
	public void testCellPath() {
		
		CellPath path = new CellPath("source");
		path.add("gateway1");
		path.add("gateway2");		
		path.add("destination");
		
		assertEquals("Incorrect path constructed", "[>source@local:gateway1@local:gateway2@local:destination@local]", path.toString());
		
		path.revert();
		assertEquals("Incorrect reverce path constructed", "[>destination@local:gateway2@local:gateway1@local:source@local]", path.toString());
		
		path.mark();
		path.add("extra");
		
		assertEquals("Incorrect with mark constructed", "[>*destination@local:gateway2@local:gateway1@local:source@local:extra@local]", path.toString());

		path.reset();
		assertEquals("Incorrect reset did not restore pre-mark condition", "[>destination@local:gateway2@local:gateway1@local:source@local]", path.toString());
	}
	
}
