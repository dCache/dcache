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

import org.globus.ftp.GridFTPRestartMarker;
import org.globus.ftp.ByteRangeList;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test GridFTPRestartMarker
**/
public class GridFTPRestartMarkerTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(GridFTPRestartMarkerTest.class.getName());

    private static String nl = System.getProperty("line.separator");
    private static String space = " ";

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(GridFTPRestartMarkerTest.class);
    }

    public GridFTPRestartMarkerTest(String name) {
	super(name);
    }

    public void testConstructorError() {
	//make sure only IllegalArgumentE gets thrown

	// whole reply rather than just a message
	assertConstructorError("111 Range Marker 0-29");
	assertConstructorError("129-Perf Marker\n");
	// to < from
	assertConstructorError("Range Marker 30-45,30-20,50-51");
	// bad format
	assertConstructorError("Range Marker 30-45,46-2e");
	assertConstructorError("Range Marker -3,70-82");
	assertConstructorError("Range Marker 30-4570-82");
    }

    public void testConstruction() {
	//simple
	testConstruction("30-66");
	testConstruction("30-45,60-71,100-134");
	//adjacent ranges
	testConstruction("0-17,18-50,51-114", "0-114");
	//backwards
	testConstruction("51-114,18-49,0-16", "0-16,18-49,51-114");
	//overlaping ranges
	testConstruction("44-99,1-5,30-37,0-36", "0-37,44-99");

	//real cases (taken from transfer of 500 MB)
	testConstruction("0-134545408","0-134545408");
	testConstruction("134545408-298778624", "134545408-298778624");
	testConstruction("298778624-466747392", "298778624-466747392");
    }

    private void testConstruction(String in) {
	testConstruction(in,in);
    }

    private void testConstruction(String in, String out) {
	logger.info(" constructing: " + in + " -> " + out);
	GridFTPRestartMarker m = 
	    new GridFTPRestartMarker("Range Marker " + in);
	ByteRangeList l = new ByteRangeList();
	l.merge(m.toVector());
	assertTrue(l.toFtpCmdArgument().equals(out));
    }

    private void assertConstructorError(String arg) {
	logger.info("constructing bad: " + arg);
	boolean threwOk = false;
	try {
	    new GridFTPRestartMarker(arg);
	} catch (IllegalArgumentException e) {
	    threwOk = true;
	} 
	
	if (! threwOk ) {
	    fail("constructor did not throw an exception when it should have");
	}
	logger.debug("okay, throws exception as expected.");
    }

}

