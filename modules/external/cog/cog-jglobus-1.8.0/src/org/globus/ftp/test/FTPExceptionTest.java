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

import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.FTPReplyParseException;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test FTPException
 **/
public class FTPExceptionTest extends TestCase{

    private static Log logger = LogFactory.getLog(FTPExceptionTest.class.getName());

    public FTPExceptionTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite ( ) {
	return new TestSuite(FTPExceptionTest.class);
    }

    /**
       This is only testing if no errors are thrown  during standard
       operations on exceptions.
    **/
    public void testFTPException() {
		  
	java.io.IOException ioe = new java.io.IOException("Some weird i/o thing.");

	FTPException e1 = new FTPException(4);
	e1.setCode(0);
	e1.getRootCause();
	e1.setRootCause(ioe);
	e1.toString();
	e1.getMessage();

	FTPException e2 = new FTPException(0, "This is additional message.");
	e2.setCode(0);
	e2.getRootCause();
	e2.setRootCause(ioe);
	e2.toString();
	e2.getMessage();
       
	FTPException e3 = new FTPReplyParseException(1, "This is additional message.");
	//e3.setCode(0);
	e3.getRootCause();
	e3.setRootCause(ioe);
	e3.toString();
	e3.getMessage();

	/*System.out.println("---to string---");
	System.out.println(e.toString());
	System.out.println("--- message ---");
	System.out.println(e.getMessage());
	System.out.println("--- stack trace ---");
	e.printStackTrace();
	*/
    }
}
