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
import org.globus.ftp.PerfMarker;
import org.globus.ftp.exception.PerfMarkerException;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test PerfMarker
 **/
public class PerfMarkerTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(PerfMarkerTest.class.getName());

    private static String nl = System.getProperty("line.separator");
    private static String space = " ";

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(PerfMarkerTest.class);
    }

    public PerfMarkerTest(String name) {
	super(name);
    }

    /**
       Test interesting cases of perf marker construction with
       invalid argument, and ensure that IllegalAgumentException
       is thrown.
     **/
    public void testConstructorError() {
	//make sure only IllegalArgumentE gets thrown

	/* correct:

	String msg = 
	      "Perf Marker" + nl 
	    + " Timestamp: 111222333444.5" + nl
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + " Total Stripe Count: 30" + nl
	    + "112 End" + nl;
	*/

	// no first line
	String msg = 
	      " Timestamp: 111222333444.5" + nl
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + " Total Stripe Count: 30" + nl
	    + "112 End" + nl;
	assertConstructorError(msg);

	// whole reply instead of just message
	msg = "112-Perf Marker" + nl 
	    + " Timestamp: 111222333444.5" + nl
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + " Total Stripe Count: 30" + nl
	    + "112 End" + nl;
	assertConstructorError(msg);

	// no timestamp
	msg = "Perf Marker" + nl 
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + " Total Stripe Count: 30" + nl
	    + "112 End" + nl;
	assertConstructorError(msg);

	// 211 message instead of 112
	msg =
	    "Extensions supported:" + nl +
	    " REST STREAM" + nl +
	    " ESTO" + nl +
	    " ERET" + nl +
	    " MDTM" + nl +
	    " SIZE" + nl +
	    " PARALLEL" + nl +
	    " DCAU" + nl +
	    "211 END";
	assertConstructorError(msg);
	
    }

    /**
       test interesting cases of perf marker construction and examine its
       get() and has() methods.
     **/
    public void testObject()
    throws Exception{
	// simple
	String msg = "Perf Marker" + nl 
	    + " Timestamp: 111222333444.5" + nl
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + " Total Stripe Count: 30" + nl
	    + "112 End" + nl;
	testObject(msg,
			 111222333444.5,
			 true, 5,
			 true, new Long("987654321987654321").longValue(),
			 true, 30);



	// unordered
	msg = "Perf Marker" + nl 
	    + " Total Stripe Count: 30" + nl
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + " Timestamp: 111222333444.5" + nl
	    + "112 End" + nl;
	testObject(msg,
			 111222333444.5,
			 true, 5,
			 true, new Long("987654321987654321").longValue(),
			 true, 30);



	// missing stripe info
	msg = "Perf Marker" + nl 
	    + " Timestamp: 111222333444.5" + nl
	    + " Total Stripe Count: 30" + nl
	    + "112 End" + nl;
	testObject(msg,
			 111222333444.5,
			 false, 0,
			 false, 0,
			 true, 30);




	// missing count
	msg = "Perf Marker" + nl 
	    + " Timestamp: 111222333444.5" + nl
	    + " Stripe Index: 5" + nl
	    + " Stripe Bytes Transferred: 987654321987654321" + nl
	    + "112 End" + nl;
	testObject(msg,
			 111222333444.5,
			 true, 5,
			 true, new Long("987654321987654321").longValue(),
			 false, 0);



	// missing most info
        msg = "Perf Marker" + nl 
	    + " Timestamp: 111222333444.5" + nl
	    + "112 End" + nl;
	testObject(msg,
			 111222333444.5,
			 false, 0,
			 false, 0,
			 false, 0);



	// zero values
	msg = "Perf Marker" + nl 
	    + " Timestamp: 0" + nl
	    + " Stripe Index: 0" + nl
	    + " Stripe Bytes Transferred: 0" + nl
	    + " Total Stripe Count: 0" + nl
	    + "112 End" + nl;
	testObject(msg,
			 0,
			 true, 0,
			 true, 0,
			 true, 0);

    }//testObject


    /**
       test perf marker construction and get() and has() methods.
       "in" is constructor parameter, other params describe the 
       expected object examination behavior.
     **/
    private void testObject(String in,
				  double ts,
				  boolean hasSI, long si, 
				  boolean hasBT, long bt,
				  boolean hasTSC, long tsc) 
    throws Exception{
	logger.info("checking object:\n" + in);
	PerfMarker m = 
	    new PerfMarker(in);

	// time stamp
	assertTrue(m.getTimeStamp() == ts);

	// stripe index
	assertTrue(m.hasStripeIndex() == hasSI);
	if (m.hasStripeIndex()) {
	    assertTrue(m.getStripeIndex() == si);
	    logger.debug("okay, stripe index matches.");
	}else {
	    boolean threwOk = false;
	    try {
		m.getStripeIndex();
	    } catch (PerfMarkerException e) {
		threwOk = true;
	    } 
	
	    if (! threwOk ) {
		fail("method did not throw an exception when it should have");
	    }
	    logger.debug("okay, throws exception as expected.");
	}

	// stripe bytes transferred
	assertTrue(m.hasStripeBytesTransferred() == hasBT);
	if (hasBT) {
	    assertTrue(m.getStripeBytesTransferred() == bt);
	    logger.debug("okay, stripe bytes transf matches.");
	}else {
	    boolean threwOk = false;
	    try {
		m.getStripeBytesTransferred();
	    } catch (PerfMarkerException e) {
		threwOk = true;
	    } 
	
	    if (! threwOk ) {
		fail("method did not throw an exception when it should have");
	    }
	    logger.debug("okay, throws exception as expected.");
	}


	// total stripe count
	assertTrue(m.hasTotalStripeCount() == hasTSC);
	if (hasTSC) {
	    assertTrue(m.getTotalStripeCount() == tsc);
	    logger.debug("okay, stripe count matches.");
	}else {
	    boolean threwOk = false;
	    try {
		m.getTotalStripeCount();
	    } catch (PerfMarkerException e) {
		threwOk = true;
	    } 
	
	    if (! threwOk ) {
		fail("method did not throw an exception when it should have");
	    }
	    logger.debug("okay, throws exception as expected.");
	}
    
    }

    /**
       Assume that arg represent an invalid message;
       ensure that constructor throws IllegalArgumentException.
     **/
    private void assertConstructorError(String arg) {
	logger.info("checking bad construction:\n" + arg);
	boolean threwOk = false;
	try {
	    new PerfMarker(arg);
	} catch (IllegalArgumentException e) {
	    threwOk = true;
	} 
	
	if (! threwOk ) {
	    fail("constructor did not throw an exception when it should have");
	}
	logger.debug("okay, throws exception as expected.");
    }

}

