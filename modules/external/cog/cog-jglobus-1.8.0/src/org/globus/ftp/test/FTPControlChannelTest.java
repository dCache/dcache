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

import org.globus.ftp.vanilla.Command;
import org.globus.ftp.vanilla.Reply;
import org.globus.ftp.vanilla.FTPControlChannel;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test FTPControlChannel
 **/
public class FTPControlChannelTest extends TestCase{

    private static Log logger = 
	LogFactory.getLog(FTPControlChannelTest.class.getName());

    public FTPControlChannelTest(String name) {
	super(name);
	logger.debug(TestEnv.show());
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run(suite());
    }

    public static Test suite ( ) {
	return new TestSuite(FTPControlChannelTest.class);
    }

    public void testAuth() throws Exception{
	logger.info("USER/PASS");
	try {

	    testAuth(TestEnv.serverFHost,
		     TestEnv.serverFPort,
		     TestEnv.serverFUser,
		     TestEnv.serverFPassword);
	    //"ftp.globus.org", 21, "anonymous", "globus@globus.org"

	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	}
    }


    /**
       Because data channel is not present, the server should
       answer here "425 Can't build data connection: Connection refused."
     **/
    public void testRetr() throws Exception{
        logger.info("RETR");
	try {

	    testRetr(TestEnv.serverFHost,
		     TestEnv.serverFPort,
		     TestEnv.serverFDir,
		     "/",
		     TestEnv.serverFFile,
		     TestEnv.serverFUser,
		     TestEnv.serverFPassword);
	    //testRetr("ftp.globus.org", 21, "anonymous", "globus@globus.org");
	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	}
    }

    private void testAuth(String host, 
			  int port,
			  String user,
			  String password) 
	throws Exception {

	    FTPControlChannel pi = new FTPControlChannel(host, port);
	    try {
		pi.open();
	    } catch (Exception e) {
		fail("Could not connect to server at " + host + ":" + port);
	    }
	    pi.write(new Command("USER", user));
	    Reply reply = pi.read();
	    
	    if (Reply.isPositiveIntermediate(reply)) { 
		pi.write(new Command("PASS", password));
		reply = pi.read();
	    }
	    
            pi.close();

	    if (! Reply.isPositiveCompletion(reply)) {
		fail("in attempt to log in, received unexpected reply from server: " + reply);
	    }
    } //testAuth

    private void testRetr(String host, 
			  int port,
			  String dir,
			  String separator,
			  String file,
			  String user,
			  String password) 
	throws Exception {

	    FTPControlChannel pi = new FTPControlChannel(host, port);
	    try {
		pi.open();
	    } catch (Exception e) {
		fail("Could not connect to server at " + host + ":" + port);
	    }
	    pi.write(new Command("USER", user));
	    Reply reply = pi.read();
	    
	    if (Reply.isPositiveIntermediate(reply)) { 
		pi.write(new Command("PASS", password));
		reply = pi.read();
	    }
	    
	    if (! Reply.isPositiveCompletion(reply)) {
		fail("in attempt to log in, received unexpected reply from server: " + reply);
	    }

	    pi.write(new Command("RETR", dir + separator +file));

	    logger.debug("received: " + pi.read().toString());

	    // this can still fail is connection gets closed.
	    /*
	    logger.debug("tester: start reading reply...");
	    int b;
	    java.io.InputStream is = pi.getInputStream();	    
	    while ( (b = is.read()) != '\n' ) {
		System.out.println("next byte ->" + (char)b +"<- code ["
				 + b + "]");
	    };
	    */

            pi.close();

    } //testRetr

    // not possible to test it: 
    // list needs data channel
    private void testList(String host, 
			  int port,
			  String user,
			  String password) 
	throws Exception {

	    FTPControlChannel pi = new FTPControlChannel(host, port);
	    try {
		pi.open();
	    } catch (Exception e) {
		fail("Could not connect to server at " + host + ":" + port);
	    }
	    pi.write(new Command("USER", user));
	    Reply reply = pi.read();
	    
	    if (Reply.isPositiveIntermediate(reply)) { 
		pi.write(new Command("PASS", password));
		reply = pi.read();
	    }
	    
	    if (! Reply.isPositiveCompletion(reply)) {
		fail("in attempt to log in, received unexpected reply from server: " + reply);
	    }


	    pi.write(new Command("LIST"));
	    logger.debug("tester received: " + pi.read());
	    pi.write(new Command("PWD"));
	    logger.debug("tester received: " + pi.read());
	    pi.write(new Command("SYST"));
	    logger.debug("tester received: " + pi.read());

            pi.close();
    } //testList


} //FTPControlChannelTest
