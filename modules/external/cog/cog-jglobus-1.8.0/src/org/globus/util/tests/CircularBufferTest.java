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

import org.globus.util.CircularBuffer;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

public class CircularBufferTest extends TestCase {

    private CircularBuffer buffer;

    public  CircularBufferTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(CircularBufferTest.class);
    }

    protected void setUp() throws Exception {
	buffer = new CircularBuffer(5);
    }

    public void testInterruptBoth() throws Exception {

	assertTrue(buffer.put("a"));
	assertTrue(buffer.put("b"));
	buffer.interruptBoth();
	assertTrue(!buffer.put("c"));
	assertTrue(!buffer.put("d"));

	assertEquals(null, buffer.get());
	assertEquals(null, buffer.get());
    }

    public void testPutFull() throws Exception {

	assertTrue(buffer.put("a"));
	assertTrue(buffer.put("b"));
	assertTrue(buffer.put("c"));
	assertTrue(buffer.put("d"));
	assertTrue(buffer.put("e"));

	Thread t = (new Thread() {
		public void run() {
		    buffer.closePut();
		    buffer.interruptPut();
		}
	    });
	t.start();

	assertTrue(!buffer.put("f"));
	assertTrue(!buffer.put("g"));

	assertEquals("a", buffer.get());
	assertEquals("b", buffer.get());
	assertEquals("c", buffer.get());
	assertEquals("d", buffer.get());
	assertEquals("e", buffer.get());
	assertEquals(null, buffer.get());
    }

}
