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
package org.globus.util.tests;

import org.globus.util.Util;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

public class UtilTest extends TestCase {

    private static final String uStr1 = "(exe = mis)";
    private static final String qStr1 = "\"(exe = mis)\"";
    
    private static final String uStr2 = "(exe = \"mis\")";
    private static final String qStr2 = "\"(exe = \\\"mis\\\")\"";

    private static final String uStr3 = "(exe = \"mis\"\\test)";
    private static final String qStr3 = "\"(exe = \\\"mis\\\"\\\\test)\"";
    
    public UtilTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(UtilTest.class);
    }

    public void testQuote1() {
	String tStr1 = Util.quote(uStr1);
	System.out.println(uStr1 + " : " + tStr1);
	assertEquals("t1", qStr1, tStr1);

	String tStr2 = Util.quote(uStr2);
        System.out.println(uStr2 + " : " + tStr2);
        assertEquals("t2", qStr2, tStr2);

        String tStr3 = Util.quote(uStr3);
        System.out.println(uStr3 + " : " + tStr3);
        assertEquals("t3", qStr3, tStr3);
    }

    public void testUnQuote1() {
        try {
            String tStr0 = Util.unquote(uStr1);
            System.out.println(uStr1 + " : " + tStr0);
            assertEquals("t0", uStr1, tStr0);
        } catch(Exception e) {
            fail("Unquote failed.");
        }

	try {
	    String tStr1 = Util.unquote(qStr1);
	    System.out.println(qStr1 + " : " + tStr1);
	    assertEquals("t1", uStr1, tStr1);
	} catch(Exception e) {
	    fail("Unquote failed.");
	}

        try {
            String tStr2 = Util.unquote(qStr2);
            System.out.println(qStr2 + " : " + tStr2);
            assertEquals("t2", uStr2, tStr2);
        } catch(Exception e) {
            fail("Unquote failed.");
        }

        try {
            String tStr3 = Util.unquote(qStr3);
            System.out.println(qStr3 + " : " + tStr3);
            assertEquals("t3", uStr3, tStr3);
        } catch(Exception e) {
            fail("Unquote failed.");
        }	
    }
}

