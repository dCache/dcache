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
package org.globus.gsi.gssapi.net.test;

import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

import org.globus.gsi.gssapi.net.GssOutputStream;

import org.ietf.jgss.GSSContext;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class GssOutputStreamTest extends TestCase {

    public GssOutputStreamTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GssOutputStreamTest.class);
    }
    
    public void test1() throws Exception {
	ByteArrayOutputStream out
	    = new ByteArrayOutputStream();

	TestGssOutputStream t = new TestGssOutputStream(out, 5);

	t.write('A');
	t.write('B');
	
	assertEquals(2, t.getIndex());

	t.write('C');
	t.write('D');
	t.write('E');

	assertEquals(5, t.getIndex());

	t.write('F');

	assertEquals(1, t.getIndex());

	assertEquals("ABCDE", new String(out.toByteArray()));
    }


    public void test2() throws Exception {
	ByteArrayOutputStream out
	    = new ByteArrayOutputStream();

	TestGssOutputStream t = new TestGssOutputStream(out, 5);

	byte [] m1 = new byte[] {'A', 'B'};
	t.write(m1);
	
	assertEquals(2, t.getIndex());

	byte [] m2 = new byte[] {'C', 'D', 'E'};
	t.write(m2);

	assertEquals(5, t.getIndex());

	t.write('F');

	assertEquals(1, t.getIndex());

	assertEquals("ABCDE", new String(out.toByteArray()));
    }
    
    public void test3() throws Exception {
	ByteArrayOutputStream out
	    = new ByteArrayOutputStream();

	TestGssOutputStream t = new TestGssOutputStream(out, 5);

	byte [] m1 = new byte[] {'A', 'B', 'C', 'D', 'E', 'F', 'G'};
	t.write(m1);
	
	assertEquals(2, t.getIndex());

	assertEquals("ABCDE", new String(out.toByteArray()));
    }

    public void test4() throws Exception {
	ByteArrayOutputStream out
	    = new ByteArrayOutputStream();
	
	TestGssOutputStream t = new TestGssOutputStream(out, 5);
	
	byte [] m1 = new byte[] {'A', 'B', 'C', 'D', 'E', 
				 'F', 'G', 'H', 'I', 'J',
				 'K', 'L', 'M'};
	t.write(m1);
	
	assertEquals(3, t.getIndex());

	assertEquals("ABCDEFGHIJ", new String(out.toByteArray()));
    }

    public void test5() throws Exception {
	ByteArrayOutputStream out
	    = new ByteArrayOutputStream();
	
	TestGssOutputStream t = new TestGssOutputStream(out, 5);
	
	byte [] m1 = new byte[] {'A', 'B', 'C', 'D', 'E', 
				 'F', 'G', 'H', 'I', 'J',
				 'K', 'L', 'M', 'N', 'O'};
	t.write(m1);
	
	assertEquals(5, t.getIndex());

	assertEquals("ABCDEFGHIJ", new String(out.toByteArray()));

	t.write('B');
	
	assertEquals(1, t.getIndex());
	assertEquals("ABCDEFGHIJKLMNO", new String(out.toByteArray()));
    }

    class TestGssOutputStream extends GssOutputStream {

	public TestGssOutputStream(OutputStream out, int size) {
	    super(out, null, size);
	}

	public int getIndex() {
	    return index;
	}

	public void flush()
	    throws IOException {
	    out.write(buff, 0, index);
	    index = 0;
	}

    }

}
