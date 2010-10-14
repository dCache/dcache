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

import org.globus.ftp.Session;
import org.globus.ftp.FTPClient;
import org.globus.ftp.DataSink;
import org.globus.ftp.HostPort;
import org.globus.ftp.Buffer;
import org.globus.ftp.FileInfo;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Vector;
import java.io.ByteArrayOutputStream;

public class FTPClientListTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(FTPClientListTest.class.getName());

    public FTPClientListTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(FTPClientListTest.class);
    }

    public void testListPassive() throws Exception {
        testList(true);
    }
    
    public void testListActive() throws Exception {
        testList(false);
    }
    
    private void testList(boolean passive) throws Exception {
	FTPClient src = 
            new FTPClient(TestEnv.serverFHost, TestEnv.serverFPort);
	src.authorize(TestEnv.serverFUser, TestEnv.serverFPassword);
	src.setType(Session.TYPE_ASCII);
	src.changeDir(TestEnv.serverFDir);
        src.setPassiveMode(passive);

        boolean foundit = false;
	Vector v = src.list();
	logger.debug("list received");
	while (! v.isEmpty()) {
	    FileInfo f = (FileInfo)v.remove(0); 
	    logger.info(f.toString());
            if (f.getName().equals(TestEnv.serverFFile)) {
                foundit = true;
            }
	}

	src.close();

        assertTrue("expected file not in the list", foundit);
    }

    
    public void test2() throws Exception {
	logger.info("test two consective list, using both list functions");

	FTPClient src = new FTPClient(TestEnv.serverFHost, TestEnv.serverFPort);
	src.authorize(TestEnv.serverFUser, TestEnv.serverFPassword);

	String output1 = null;
	String output2 = null;

	// using list()

	src.changeDir(TestEnv.serverFDir);
	Vector v = src.list();
	logger.debug("list received");
	StringBuffer output1Buffer = new StringBuffer();
	while (! v.isEmpty()) {
	    FileInfo f = (FileInfo)v.remove(0); 
	    output1Buffer.append(f.toString()).append("\n");

	}
	output1 = output1Buffer.toString();



	// using list(String,String, DataSink)


	HostPort hp2 = src.setPassive();
	src.setLocalActive();

	final ByteArrayOutputStream received2= new ByteArrayOutputStream(1000);

	// unnamed DataSink subclass will write data channel content
	// to "received" stream.

	 src.list("*", "-d", new DataSink(){
		public void write(Buffer buffer) 
		    throws IOException{
		    logger.debug("received " + buffer.getLength() +
				 " bytes of directory listing");
		    received2.write(buffer.getBuffer(),
				   0,
				   buffer.getLength());
		}
		public void close() 
		    throws IOException{};
	     });

	// transfer done. Data is in received2 stream.

	 output2 = received2.toString();
	 logger.debug(output2);

	 src.close();
    }    
}
