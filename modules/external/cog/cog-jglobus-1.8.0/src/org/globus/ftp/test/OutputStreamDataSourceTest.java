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

import java.io.OutputStream;
import java.io.EOFException;

import org.globus.ftp.Buffer;
import org.globus.ftp.OutputStreamDataSource;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OutputStreamDataSourceTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(OutputStreamDataSourceTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(OutputStreamDataSourceTest.class);
    }

    public OutputStreamDataSourceTest(String name) {
	super(name);
    }

    // data source is closed
    public void testCloseDataSourceSingle() throws Exception {

	OutputStreamDataSource sr = new OutputStreamDataSource(512);

	OutputStream out = sr.getOutputStream();
	out.write(1);
	out.flush();
	out.write(2);
	out.flush();
	out.write(3);
	out.flush();

	Buffer buf;

	buf = sr.read();
	assertTrue(buf != null);

	buf = sr.read();
	assertTrue(buf != null);

	sr.close();

	buf = sr.read();

	assertTrue(buf == null);
    }

    // data source is blocked in read while data source close is called
    public void testCloseDataSourceMulti() throws Exception {

	OutputStreamDataSource sr = new OutputStreamDataSource(512);

	Thread1 t = new Thread1(sr);
	t.start();

	// give the thread a chance to run
	Thread.sleep(2000);

	sr.close();

	assertEquals(null, t.getBuffer());
	assertEquals(null, t.getException());
	assertEquals(null, sr.read());
    }

    class Thread1 extends Thread {
	
	private Buffer buf;
	private Exception exception;
	private OutputStreamDataSource sr;

	public Thread1(OutputStreamDataSource sr) {
	    this.sr = sr;
	}

	public Buffer getBuffer() {
	    return buf;
	}
	
	public Exception getException() {
	    return exception;
	}

	public void run() {
	    try {
		buf = sr.read();
	    } catch (Exception e) {
		exception = e;
	    }
	}
    }


    // output stream is blocked in flush while data source close is called
    public void testCloseDataSourceStream() throws Exception {

	OutputStreamDataSource sr = new OutputStreamDataSource(512);
	
	OutputStream out = sr.getOutputStream();

	Thread2 t = new Thread2(out);
	t.start();

	// give the thread a chance to run
	Thread.sleep(2000);

	sr.close();

	assertTrue(t.getException1() == null);
	assertTrue(t.getException2() != null);
	assertTrue(t.getException2() instanceof EOFException);
	assertTrue(sr.read() == null);
    }

    class Thread2 extends Thread {
	
	private Exception exception1, exception2;
	private OutputStream sr;

	public Thread2(OutputStream sr) {
	    this.sr = sr;
	}

	public Exception getException1() {
	    return exception1;
	}

	public Exception getException2() {
	    return exception2;
	}

	public void run() {
	    try {
		sr.write(1);
		sr.flush();
		sr.write(2);
		sr.flush();
		sr.write(3);
		sr.flush();
		sr.write(4);
		sr.flush();
		sr.write(5);
		sr.flush();
		sr.write(6);
	    } catch (Exception e) {
		exception1 = e;
		return;
	    }
	    try {
		sr.flush();
	    } catch (Exception e) {
		exception2 = e;
	    }
	}
    }

    // output stream is closed while Data Source is calling read()
    // until it returns null
    public void testCloseStream() throws Exception {

	OutputStreamDataSource sr = new OutputStreamDataSource(512);
	
	OutputStream out = sr.getOutputStream();

	out.write(1);
	out.flush();
	out.write(2);
	out.flush();
	out.write(3);
	out.flush();
	out.close();

	Buffer buf = null;

	buf = sr.read();
	assertTrue(buf != null);

	buf = sr.read();
	assertTrue(buf != null);

	buf = sr.read();
	assertTrue(buf != null);

	buf = sr.read();
	assertTrue(buf == null);
    }

    public void testCloseStreamThead() throws Exception {

	OutputStreamDataSource sr = new OutputStreamDataSource(512);
	
	Thread3 t = new Thread3(sr);
	t.start();

	OutputStream out = sr.getOutputStream();

	out.write(1);
	out.flush();
	out.write(2);
	out.flush();
	out.write(3);
	out.flush();
	out.close();

	t.join(1000*60);

	assertTrue(t.getException() == null);
	assertEquals(3, t.getCount());
    }

    class Thread3 extends Thread {
	 
	private Exception exception;
	private OutputStreamDataSource sr;
	private int count;

	public Thread3(OutputStreamDataSource sr) {
	    this.sr = sr;
	}

	public Exception getException() {
	    return exception;
	}

	public int getCount() {
	    return count;
	}

	public void run() {
	    Buffer buf = null;
	    try {
		while( (buf = sr.read()) != null) {
		    count++;
		}
	    } catch (Exception e) {
		exception = e;
	    }
	}
    }

}
