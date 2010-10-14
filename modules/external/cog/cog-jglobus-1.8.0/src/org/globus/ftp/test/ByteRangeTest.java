/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.ftp.test;

import org.globus.ftp.ByteRange;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test ByteRange
 **/
public class ByteRangeTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(FeatureListTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(ByteRangeTest.class);
    }

    public ByteRangeTest(String name) {
	super(name);
    }

    public void test() {
	//trivial
	assertMerge(0,0,  0,0,   0,0,     ByteRange.THIS_SUPERSET);
	assertMerge(52,52,  52,52,   52,52,     ByteRange.THIS_SUPERSET);
	assertMerge(12,15,  12,15,   12,15,     ByteRange.THIS_SUPERSET);

	// now testing all cases: adjacent / subset / superset / separate
	// mnemotechnic notation:
	// first range   =   t
	// second range  =   o
	// common subset =   -
	
	// t o
	assertMerge(1,4,   7,8,   1,4,    ByteRange.THIS_BELOW);
	// o t
	assertMerge(7,8,   1,4,   7,8,    ByteRange.THIS_ABOVE);
	// ot
	assertMerge(5,7,   1,4,   1,7,    ByteRange.ADJACENT);
	// to
	assertMerge(1,4,   5,7,   1,7,    ByteRange.ADJACENT);
	// o-t
	assertMerge(3,15, -3,4,   -3,15,  ByteRange.ADJACENT);
	// t-o
	assertMerge(1,2,  2,4,   1,4,     ByteRange.ADJACENT);
	// o-
	assertMerge(3,15, -3,15, -3,15,   ByteRange.THIS_SUBSET);	     
	// t-
	assertMerge(-3,15, 3,15, -3,15,   ByteRange.THIS_SUPERSET);	     
	// t-t	
	assertMerge(-3,30, 3,15, -3,30,   ByteRange.THIS_SUPERSET);
	// o-o
	assertMerge(3,15, -3,30, -3,30,   ByteRange.THIS_SUBSET);
	// -t
	assertMerge(0,2,  0,0,   0,2,     ByteRange.THIS_SUPERSET);
	// -o
	assertMerge(0,0,  0,2,   0,2,     ByteRange.THIS_SUBSET);
	// -
	assertMerge(3,15, 3,15,  3,15,    ByteRange.THIS_SUPERSET);
	
	//more about: separate or adjacent?
	assertMerge(1,1, 2,2, 1,2,    ByteRange.ADJACENT);
	assertMerge(1,1, 4,4, 1,1,    ByteRange.THIS_BELOW);
	assertMerge(-5,5, 6,9, -5,9,  ByteRange.ADJACENT);
	assertMerge(-5,5, 7,9, -5,5,  ByteRange.THIS_BELOW);
	
	assertConstructorError(4,3);
	assertConstructorError(16,2);
	assertConstructorError(20, -21);
    }

    /* test ByteRange(from1,to1).merge(new ByteRange(from2, to2));
       the result should (from3, to3) and expectedReturn should
       be returned.
     */
    private void assertMerge(int from1, int to1,
			     int from2, int to2,
			     int from1after, int to1after,
			     int expectedReturn) {
	logger.debug("checking: (" 
		     + from1 + ".." + to1 +") + ("
		     + from2 + ".." + to2 +") = ("
		     + from1after + ".." + to1after + ")");
	ByteRange br1 = new ByteRange(from1, to1);
	ByteRange br2 = new ByteRange(from2, to2);
	int ret = br1.merge(br2);
	logger.debug("... -> (" + br1.from + ".." + br1.to + ")"); 
	assertTrue(ret == expectedReturn);
	assertTrue(br1.from == from1after);
	assertTrue(br1.to == to1after);
    }

    private void assertConstructorError(int from, int to) {
	logger.debug("checking constructor: (" + from + "," + to + ")");
	boolean threwOk = false;
	try {
	    new ByteRange(from, to);
	} catch (IllegalArgumentException e) {
	    threwOk = true;
	} 
	
	if (! threwOk ) {
	    fail("constructor did not throw an exception when it should have");
	}
	logger.debug("okay, throws exception as expected.");
    }

}

