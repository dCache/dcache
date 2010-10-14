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

import java.io.InputStream;
import java.io.EOFException;

import org.globus.ftp.Buffer;
import org.globus.ftp.InputStreamDataSink;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InputStreamDataSinkTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(InputStreamDataSinkTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(InputStreamDataSinkTest.class);
    }
    
    public InputStreamDataSinkTest(String name) {
	super(name);
    }

    // data sink is closed
    public void testCloseDataSink() throws Exception {

	InputStreamDataSink sr = new InputStreamDataSink();

	sr.write(new Buffer(new byte[] {'a'}, 1));
	sr.write(new Buffer(new byte[] {'b'}, 1));
	sr.write(new Buffer(new byte[] {'c'}, 1));

	sr.close();

	try {
	    sr.write(new Buffer(null, 4));
	    fail("did not throw exception");
	} catch (EOFException e) {
	}

	InputStream in = sr.getInputStream();
	
	assertEquals('a', in.read());
	assertEquals('b', in.read());
	assertEquals('c', in.read());
	assertEquals(-1, in.read());
    }

    public void testCloseDataSinkAndStream() throws Exception {

	InputStreamDataSink sr = new InputStreamDataSink();

	Thread1 t = new Thread1(sr);
	t.start();

	// give the thread a chance to run
	Thread.sleep(2000);

	sr.close();

	assertTrue(t.getException() == null);

	int n = 5;

	assertEquals(n, t.getCounter());

	InputStream in = sr.getInputStream();
	
	for (int i=0;i<n;i++) {
	    assertEquals(i, in.read());
	}

	in.close();

	t.join(1000*60);

	assertTrue(t.getException() != null);
	assertTrue(t.getException() instanceof EOFException);
	assertEquals(-1, in.read());
    }

    class Thread1 extends Thread {
	
	private Exception exception;
	private InputStreamDataSink sr;
	private int count = 0;

	public Thread1(InputStreamDataSink sr) {
	    this.sr = sr;
	}

	public int getCounter() {
	    return count;
	}
	
	public Exception getException() {
	    return exception;
	}

	public void run() {
	    try {
		for (;;) {
		    sr.write(new Buffer(new byte[] {(byte)count}, 1));
		    count++;
		}
	    } catch (Exception e) {
		exception = e;
	    }
	}
    }

    // input stream is blocked in read while data sink close is called
    public void testCloseDataSourceStream() throws Exception {

	InputStreamDataSink sr = new InputStreamDataSink();

	sr.write(new Buffer(new byte[] {2}, 1));

	InputStream in = sr.getInputStream();

	Thread2 t = new Thread2(in);
	t.start();

	// give the thread a chance to run
	Thread.sleep(2000);

	sr.close();

	t.join(1000*60);

	assertTrue(t.getException1() == null);
	assertTrue(t.getException2() == null);
	assertEquals(2, t.getRead1());
	assertEquals(-1, t.getRead2());
	assertEquals(-1, in.read());
    }

    class Thread2 extends Thread {
	
	private Exception exception1, exception2;
	private InputStream sr;
	private int read1, read2;

	public Thread2(InputStream sr) {
	    this.sr = sr;
	}

	public Exception getException1() {
	    return exception1;
	}

	public Exception getException2() {
	    return exception2;
	}

	public int getRead1() {
	    return read1;
	}

	public int getRead2() {
	    return read2;
	}

	public void run() {
	    try {
		read1 = sr.read();
	    } catch (Exception e) {
		exception1 = e;
		return;
	    }
	    try {
		read2 = sr.read();
	    } catch (Exception e) {
		exception2 = e;
	    }
	}
    }

    // input stream is closed 
    public void testCloseStream() throws Exception {

	InputStreamDataSink sr = new InputStreamDataSink();

	sr.write(new Buffer(new byte[] {1}, 1));
	sr.write(new Buffer(new byte[] {2}, 1));

	InputStream in = sr.getInputStream();

	assertEquals(1, in.read());
	assertEquals(2, in.read());

	in.close();

	assertEquals(-1, in.read());
	assertEquals(-1, in.read(new byte[10]));

	try {
	    sr.write(new Buffer(new byte[] {5}, 1));
	    fail("Did not throw right exception");
	} catch (EOFException e) {
	}
    }

    public void testCloseStreamThead2() throws Exception {

	InputStreamDataSink sr = new InputStreamDataSink();

	sr.write(new Buffer(new byte[] {1}, 1));

	InputStream in = sr.getInputStream();

	Thread4 t = new Thread4(in);
	t.start();

	Thread.sleep(2000);

	in.close();

	try {
	    sr.write(new Buffer(new byte[] {5}, 1));
	    fail("Did not throw right exception");
	} catch (EOFException e) {
	}

	assertTrue(t.getException() == null);
	assertEquals(1, t.getCount());
    }
    
    class Thread4 extends Thread {
	 
	private Exception exception;
	private InputStream sr;
	private int count;

	public Thread4(InputStream sr) {
	    this.sr = sr;
	}

	public Exception getException() {
	    return exception;
	}

	public int getCount() {
	    return count;
	}
	
	public void run() {
	    try {
		while ( sr.read() != -1 ) {
		    count++;
		}
	    } catch (Exception e) {
		exception = e;
	    }
	}
    }
    
}
