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

import org.globus.ftp.FeatureList;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test FeatureList
 **/
public class FeatureListTest extends TestCase{

    private static Log logger = 
	LogFactory.getLog(FeatureListTest.class.getName());

    public FeatureListTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite ( ) {
	return new TestSuite(FeatureListTest.class);
    }




    private static String nl = System.getProperty("line.separator");

    private static String featMsg=
	"Extensions supported:" + nl +
	" REST STREAM" + nl +
	" ESTO" + nl +
	" ERET" + nl +
	" MDTM" + nl +
	" SIZE" + nl +
	" PARALLEL" + nl +
	" DCAU" + nl +
	"211 END";
    
    //should contain
    private static final int Y = 1;
    //should not contain
    private static final int N = 2;
    //should throw error
    private static final int E = 3;

    public void testContains() {
	logger.info("testing parsed feature set:");
	FeatureList fl = new FeatureList(featMsg);
	testContains(fl, "REST STREAM", Y);
	testContains(fl, FeatureList.ESTO, Y);
	testContains(fl, FeatureList.DCAU, Y);
	testContains(fl, FeatureList.PARALLEL, Y);
	testContains(fl, FeatureList.PARALLEL, Y);
	testContains(fl, FeatureList.SIZE, Y);
	testContains(fl, FeatureList.MDTM, Y);
	testContains(fl, FeatureList.ERET, Y);

	//reply's 1st and last line
	testContains(fl, "Extensions supported:", N);
	testContains(fl, "211 END", N);

	//bad
	testContains(fl, null, E);
	testContains(fl, "", N);
	testContains(fl, "TVFS", N);


    }
    
    private void testContains(
			      FeatureList fl,
			      String feature,
			      int expectedResult) {
	switch (expectedResult) {
	case Y: 	    
	    assertTrue(fl.contains(feature));
	    logger.info("okay, contains " + feature);
	    break;
	case N:
	    assertTrue( ! fl.contains(feature));
	    logger.info("okay, does not contain " + feature);
	    break;
	case E:
	    boolean threwOk = false;
	    try {
		fl.contains(feature);
	    } catch (IllegalArgumentException e) {
		threwOk = true;
	    } 

	    if (! threwOk ) {
		fail("FeatureList.contains() did not throw an exception when it should have");
	    }
	    logger.info("okay, throws exception as expected.");
	    break;
	}
    }
}
