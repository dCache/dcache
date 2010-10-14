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
import org.globus.ftp.ByteRangeList;

import java.util.Vector;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test ByteRangeList
 **/
public class ByteRangeListTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(ByteRangeListTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(ByteRangeListTest.class);
    }

    public ByteRangeListTest(String name) {
	super(name);
    }

    /**
       Test merging ByteRange to ByteRangeList.
       Systematic test of most cases.
       Tests merge() and toFtpCmdArgument().
       Assure that merge() does not modify parameter objects.
     **/
    public void test1() {
	Vector v = new Vector();
	v.add(new ByteRange(12, 17));
	v.add(new ByteRange(24, 26));
	v.add(new ByteRange(31, 31));
	v.add(new ByteRange(40, 45));
	v.add(new ByteRange(52, 52));
	// t = vector
	// o = new range
	// t t t t t =
	// "12-17 24-26 31-31 40-45 52-52"

	// o t t t t t
	assertMerge1(v,  0,5, "0-5,12-17,24-26,31-31,40-45,52-52");
	// ot t t t t
	assertMerge1(v, 7,11, "7-17,24-26,31-31,40-45,52-52");
	// o-t t t t t
	assertMerge1(v, 3,15, "3-17,24-26,31-31,40-45,52-52");
	// o-o t t t t
	assertMerge1(v, 10,18, "10-18,24-26,31-31,40-45,52-52");
	// o-ot t t t
	assertMerge1(v, 10,23, "10-26,31-31,40-45,52-52");
	// o-o-t t t t
	assertMerge1(v, 10,30, "10-31,40-45,52-52");
	// o-o-o-o- t
	assertMerge1(v, 10,50, "10-50,52-52");
	// o-o-o-o-o-o
	assertMerge1(v, 10,53, "10-53");

	// t-t t t t t
	assertMerge1(v, 13,16, "12-17,24-26,31-31,40-45,52-52");
	// - t t t t
	assertMerge1(v, 12,17, "12-17,24-26,31-31,40-45,52-52");
	// t-o-o-t t t
	assertMerge1(v, 16,31, "12-31,40-45,52-52");
	// -o-o-o-o-o
	assertMerge1(v, 12,100, "12-100");

	// t o t t t t
	assertMerge1(v, 24,26, "12-17,24-26,31-31,40-45,52-52");
	// t ot t t t
	assertMerge1(v, 20,23, "12-17,20-26,31-31,40-45,52-52");
	// t o-o t t t
	assertMerge1(v, 20,29, "12-17,20-29,31-31,40-45,52-52");
	// t o-ot t t
	assertMerge1(v, 20,30, "12-17,20-31,40-45,52-52");
	// t o-o-o-o t
	assertMerge1(v, 20,49, "12-17,20-49,52-52");

	// t to t t t
	assertMerge1(v, 27,27, "12-17,24-27,31-31,40-45,52-52");
	// t tot t t
	assertMerge1(v, 27,30, "12-17,24-31,40-45,52-52");
	// t to-ot t
	assertMerge1(v, 26,40, "12-17,24-45,52-52");

	// t t t tot
	assertMerge1(v, 46,51, "12-17,24-26,31-31,40-52");
	// t t t t o-t
	assertMerge1(v, 51,52, "12-17,24-26,31-31,40-45,51-52");
	// t t t t -o
	assertMerge1(v, 52,53, "12-17,24-26,31-31,40-45,52-53");
	// t t t t t o
	assertMerge1(v, 54,67, "12-17,24-26,31-31,40-45,52-52,54-67");
    }

    /**
    	real case (taken from transfer of 500 MB)
     **/
    public void test2() {
	Vector v = new Vector();
	v.add(new ByteRange(0, 134545408));
	v.add(new ByteRange(134545408, 298778624));
	assertMerge1(v, 298778624, 466747392, "0-466747392");
}
    /** 
	Ad hoc tests of interesting merge cases.
	Test merging several ranges into 1 list.
	Test merge() and toFtpCmdArgument().
     **/
    public void test3() {
	//merge 2 -> 1
	Vector v = new Vector();;

	v.add(new ByteRange(1,3));
	v.add(new ByteRange(4,6));	    
	assertMerge(v,"1-6");

	//merge 3 -> 1, 2, or 3
	v = new Vector();
	v.add(new ByteRange(1,3));
	v.add(new ByteRange(5,19));
	v.add(new ByteRange(4,6));
	assertMerge(v, "1-19");

	v = new Vector();
	v.add(new ByteRange(1,3));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(4,6));
	assertMerge(v, "1-6,9-19");
	      
	v = new Vector();
	v.add(new ByteRange(1,3));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(6,12));
	assertMerge(v, "1-3,6-19");
		    
	v = new Vector();
	v.add(new ByteRange(1,3));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(6,12));
	assertMerge(v, "1-3,6-19");			     
				
	v = new Vector();
	v.add(new ByteRange(1,3));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(0,2));
	assertMerge(v, "0-3,9-19");

	v = new Vector();
	v.add(new ByteRange(1,3));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(0,12));
	assertMerge(v, "0-19");

	//large number first
	v = new Vector();
	v.add(new ByteRange(50,64));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(6,12));
	assertMerge(v, "6-19,50-64");			     

	v = new Vector();
	v.add(new ByteRange(50,64));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(6,7));
	assertMerge(v, "6-7,9-19,50-64");			     

	//2 identical
	v = new Vector();
	v.add(new ByteRange(6,7));
	v.add(new ByteRange(9,19));
	v.add(new ByteRange(6,7));
	assertMerge(v, "6-7,9-19");			     
		
	v = new Vector();
	v.add(new ByteRange(30,40));
	v.add(new ByteRange(6,7));
	v.add(new ByteRange(30,40));
	v.add(new ByteRange(6,7));
	assertMerge(v, "6-7,30-40");			     

	//1 superset
	v = new Vector();
	v.add(new ByteRange(30,40));
	v.add(new ByteRange(6,7));
	v.add(new ByteRange(35,50));
	v.add(new ByteRange(3,100));
	assertMerge(v, "3-100");			     

	v = new Vector();
	v.add(new ByteRange(3,100));
	v.add(new ByteRange(6,7));
	v.add(new ByteRange(35,50));
	v.add(new ByteRange(30,40));
	assertMerge(v, "3-100");			     

	//singletons
	v = new Vector();
	v.add(new ByteRange(3,3));
	v.add(new ByteRange(6,7));
	v.add(new ByteRange(1,1));
	v.add(new ByteRange(8,8));
	assertMerge(v, "1-1,3-3,6-8");	  			     

	v = new Vector();
	v.add(new ByteRange(3,3));
	v.add(new ByteRange(4,4));
	v.add(new ByteRange(1,1));
	v.add(new ByteRange(2,2));
	assertMerge(v, "1-4");	  			     
    }

    /**
       Create ByteRangeList from vector v,
       merge with new ByteRange(from, to),
       assure it renders toFtpCmdArgument() as expectedResult
       and that original vector and range did not change.
       Test merge(Vector), merge(ByteRange), toFtpCmdArgument() and
     **/
    private void assertMerge1(Vector v,
			      int from,
			      int to,
			      String expectedResult) {

	ByteRangeList list = new ByteRangeList();
	list.merge(v);
	ByteRange newRange = new ByteRange(from, to);

	String vBefore = list.toFtpCmdArgument();
	String rBefore = newRange.toString();
	logger.info("merging range: " + vBefore + " + " + rBefore);

	// test merge

	list.merge(newRange);
	String actualResult = list.toFtpCmdArgument();
	logger.debug("  -> " + actualResult);
	assertTrue(expectedResult.equals(actualResult));
	logger.debug("ok, merged as expected.");

	// original vector and range did not change?

	ByteRangeList list2 = new ByteRangeList();
	list2.merge(v);
	String vAfter = list2.toFtpCmdArgument();
	String rAfter = newRange.toString();
	
	assertTrue(vBefore.equals(vAfter));
	assertTrue(rBefore.equals(rAfter));
	logger.debug("ok, original objects intact");
    };

    /**
       Merge vector into a new ByteRangeList.
       Test merge(ByteRange), merge(Vector), toFtpCmdArgument().
     **/
    private void assertMerge(Vector v, String result) {
	
	logger.info("merging vector of ranges: " + result);

	ByteRangeList list1 = new ByteRangeList();
	for (int i=0; i<v.size(); i++) {
	    list1.merge((ByteRange)v.elementAt(i));
	}
	logger.debug("    -> " + list1.toFtpCmdArgument());
	assertTrue(list1.toFtpCmdArgument().equals(result));

 	logger.debug("merging one by one again..");
	ByteRangeList list3 = new ByteRangeList();
	for (int i=0; i<v.size(); i++) {
	    list3.merge((ByteRange)v.elementAt(i));
	}
	logger.debug(" .. -> " + list3.toFtpCmdArgument());
	assertTrue(list3.toFtpCmdArgument().equals(result));


	logger.debug("merging vector at once");
	ByteRangeList list2 = new ByteRangeList();
	list2.merge(v);
	logger.debug(" .. -> " + list2.toFtpCmdArgument());
	assertTrue(list2.toFtpCmdArgument().equals(result));    

    }
}

