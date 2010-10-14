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
import org.globus.ftp.StreamModeRestartMarker;
import org.globus.ftp.HostPort;
import org.globus.ftp.exception.ServerException;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.io.IOException;

public class FTPClientTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(FTPClientTest.class.getName());

    public FTPClientTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(FTPClientTest.class);
    }

    private boolean skipTest(String property, String msg) {
	if (property == null) {
	    if (TestEnv.failUnset) {
		fail(msg);
	    }
	    logger.info("Test skipped: " + msg);
	    return true;
	} else {
	    return false;
	}
    }

    public void testSize() throws Exception {
	logger.info("getSize()");
	FTPClient src = new FTPClient(TestEnv.serverFHost, 
				      TestEnv.serverFPort);
	src.authorize(TestEnv.serverFUser, TestEnv.serverFPassword);

	src.changeDir(TestEnv.serverFDir);
	assertEquals(true, src.exists(TestEnv.serverFFile));
	src.setType(Session.TYPE_IMAGE);

	long size = -1;

        size = src.getSize(TestEnv.serverFFile);
	assertEquals(TestEnv.serverFFileSize, size);

        size = src.size(TestEnv.serverFFile);
	assertEquals(TestEnv.serverFFileSize, size);

	Date d1 = src.getLastModified(TestEnv.serverFFile);
        Date d2 = src.lastModified(TestEnv.serverFFile);

        assertEquals(d1.getTime(), d2.getTime());

        // TODO: need to verify the date agaist something..

	src.close();
    }
    
    public void testDir() throws Exception {
	logger.info("makeDir()");

	if (skipTest(TestEnv.serverGHost, "serverGHost undefined")) {
	    return;
	}
	
	FTPClient dest = new FTPClient(TestEnv.serverGHost,
				       TestEnv.serverGPort);
	dest.authorize(TestEnv.serverGUser, TestEnv.serverGPassword);

	String tmpDir = "abcdef";
	String baseDir = dest.getCurrentDir();
	dest.makeDir(tmpDir);
	dest.changeDir(tmpDir);
	assertEquals(baseDir + "/" + tmpDir, dest.getCurrentDir());
	dest.goUpDir();
	assertEquals(baseDir, dest.getCurrentDir());
	dest.deleteDir(tmpDir);
	try {
	    dest.changeDir(tmpDir);
	    fail("directory should have been removed");
	} catch (Exception e) {
	}

	dest.close();
    }

    /* do not run: the server might not support FEAT

    public void testFeature() throws Exception {
	logger.info("getFeatureList()");
	FeatureList fl = src.getFeatureList();
	assertEquals(true, fl.contains("DcaU"));
	assertEquals(false, fl.contains("MIS"));
    }
    */

    
    public void testModes() throws Exception {
	logger.info("setActive()/setPassive()");

	FTPClient src = new FTPClient(TestEnv.serverFHost, 
				      TestEnv.serverFPort);
	src.authorize(TestEnv.serverFUser, TestEnv.serverFPassword);

	HostPort hp = null;
	hp = new HostPort("140.221.11.99", 8888);
	src.setActive(hp);
	hp = src.setPassive();
	logger.debug(hp.getHost() + " " + hp.getPort());

	src.close();
    }
    

    /* do not run: the server might not support OPTS

    public void testOptions() throws Exception {
	logger.info("retrieveOptions()");
	Options opts = new RetrieveOptions(1, 2, 3);
	src.setOptions(opts);
    }
    */
    public void testRestartMarker() throws Exception {
	logger.info("setRestartMarker()");

	FTPClient src = new FTPClient(TestEnv.serverFHost, 
				      TestEnv.serverFPort);
	src.authorize(TestEnv.serverFUser, TestEnv.serverFPassword);
	
	StreamModeRestartMarker rm = new StreamModeRestartMarker(12345);
	src.setRestartMarker(rm);

	src.close();
    }

    /** try third party transfer.
       no exception should be thrown.
    **/
    public void test3Party() throws Exception {
	logger.info("3 party");

	if (skipTest(TestEnv.serverGHost, "serverGHost undefined")) {
	    return;
	}

	test3Party(
		   TestEnv.serverFHost,
		   TestEnv.serverFPort,
		   TestEnv.serverFUser,
		   TestEnv.serverFPassword,
		   TestEnv.serverFDir + "/" + TestEnv.serverFFile,
		   
		   TestEnv.serverGHost,
		   TestEnv.serverGPort,
		   TestEnv.serverGUser,
		   TestEnv.serverGPassword,
		   TestEnv.serverGDir + "/" + TestEnv.serverGFile
		   );
    }

    /** Try transferring file to and from bad port on existing server.
       IOException should be thrown.
    **/
    public void test3Party_noSuchPort() throws Exception{
	logger.info("3 party with bad port");

	if (TestEnv.serverANoSuchPort == TestEnv.UNDEFINED) {
	    logger.info("Omitting the test: test3Party_noSuchPort");
	    logger.info("because some necessary properties are not defined.");
	    return;
	}
	logger.debug("transfer FROM non existent port");
	boolean caughtOK = false;
	try {
	    	    
	    test3Party(
		       TestEnv.serverAHost,
		       TestEnv.serverANoSuchPort,
		       TestEnv.serverFUser,
		       TestEnv.serverFPassword,
		       TestEnv.serverFDir + "/" + TestEnv.serverFFile,
		       
		       TestEnv.serverGHost,
		       TestEnv.serverGPort,
		       TestEnv.serverGUser,
		       TestEnv.serverGPassword,
		       TestEnv.serverGDir + "/" + TestEnv.serverGFile
		       );

	} catch (Exception e) {
	    if (e instanceof IOException) {
		logger.debug(e.toString());
		caughtOK = true;
		logger.debug("Test passed: IOException properly thrown.");
	    } else {
		logger.error("", e);
		fail(e.toString());
	    }
	} finally {
	    if (!caughtOK) {
		fail("Attempted to contact non existent server, but the expected exception has not been thrown.");
	    }
	}
	
	logger.debug("transfer TO non existent port");
	caughtOK = false;
	try {
	    	    
	    test3Party(
		       TestEnv.serverFHost,
		       TestEnv.serverFPort,
		       TestEnv.serverFUser,
		       TestEnv.serverFPassword,
		       TestEnv.serverFDir + "/" + TestEnv.serverFFile,
		       
		       TestEnv.serverAHost,
		       TestEnv.serverANoSuchPort,
		       TestEnv.serverGUser,
		       TestEnv.serverGPassword,
		       TestEnv.serverGDir + "/" + TestEnv.serverGFile
		       );

	} catch (Exception e) {
	    if (e instanceof IOException) {
		logger.debug(e.toString());
		caughtOK = true;
		logger.debug("Test passed: IOException properly thrown.");
	    } else {
		logger.error("", e);
		fail(e.toString());
	    }
	} finally {
	    if (!caughtOK) {
		fail("Attempted to contact non existent server, but the expected exception has not been thrown.");
	    }
	}
    }

    /** Try transferring file to and from non existent server.
       IOException should be thrown.
    **/
    public void test3Party_noSuchServer1() throws Exception {
	logger.info("3 party with bad server");
	logger.debug("transfer FROM non existent server");
	
	try {
	    test3Party(
		       TestEnv.noSuchServer,
		       TestEnv.serverFPort,
		       TestEnv.serverFUser,
		       TestEnv.serverFPassword,
		       TestEnv.serverFDir + "/" + TestEnv.serverFFile,
		       
		       TestEnv.serverGHost,
		       TestEnv.serverGPort,
		       TestEnv.serverGUser,
		       TestEnv.serverGPassword,
		       TestEnv.serverGDir + "/" + TestEnv.serverGFile
		       );

	} catch (IOException e) {
	    logger.debug("Test passed: IOException properly thrown.", e);
	}
    }

    /** Try transferring file to and from non existent server.
	IOException should be thrown.
    **/
    public void test3Party_noSuchServer2() throws Exception {
	logger.info("3 party with bad server");
	logger.debug("transfer TO non existent server");

	try {
	    test3Party(
		       TestEnv.serverFHost,
		       TestEnv.serverFPort,
		       TestEnv.serverFUser,
		       TestEnv.serverFPassword,
		       TestEnv.serverFDir + "/" + TestEnv.serverFFile,
		       
		       TestEnv.noSuchServer,
		       21,
		       TestEnv.serverGUser,
		       TestEnv.serverGPassword,
		       TestEnv.serverGDir + "/" + TestEnv.serverGFile
		       );

	} catch (IOException e) {
	    logger.debug("Test passed: IOException properly thrown.", e);
	}
    }

    /** try transferring non existent file;
	 ServerException should be thrown
    **/
    
    public void test3Party_noSuchSrcFile() throws Exception {
	logger.info("3 party with bad src file");

	if (skipTest(TestEnv.serverGHost, "serverGHost undefined")) {
	    return;
	}

	try {
	    test3Party(
		       TestEnv.serverFHost,
		       TestEnv.serverFPort,
		       TestEnv.serverFUser,
		       TestEnv.serverFPassword,
		       TestEnv.serverFDir + "/" + TestEnv.serverFNoSuchFile,
		       
		       TestEnv.serverGHost,
		       TestEnv.serverGPort,
		       TestEnv.serverGUser,
		       TestEnv.serverGPassword,
		       TestEnv.serverGDir + "/" + TestEnv.serverGFile
		       );

	} catch (ServerException e) {
	    logger.debug("Test passed: ServerException properly thrown.",
			 e);
	}
    }

    /** try transferring file to non existent directory;
	ServerException should be thrown.
    **/

    public void test3Party_noSuchDestDir() throws Exception {
       logger.info("3 party with bad dest dir");

       if (skipTest(TestEnv.serverGHost, "serverGHost undefined")) {
	   return;
       }
       
       try {
	    test3Party(
		       TestEnv.serverFHost,
		       TestEnv.serverFPort,
		       TestEnv.serverFUser,
		       TestEnv.serverFPassword,
		       TestEnv.serverFDir + "/" + TestEnv.serverFFile,
		       
		       TestEnv.serverGHost,
		       TestEnv.serverGPort,
		       TestEnv.serverGUser,
		       TestEnv.serverGPassword,
		       TestEnv.serverGNoSuchDir + "/" + TestEnv.serverGFile
		       );

       } catch (ServerException e) {
	   logger.debug("Test passed: ServerException properly thrown.", 
			e);
       }
    }

    /**
       This method implements the actual transfer.
     **/
    private void test3Party(String host1, 
			    int port1,
			    String user1,
			    String password1,
			    String sourceFile,
			    String host2,
			    int port2,
			    String user2,
			    String password2,
			    String destFile)
	throws Exception {
	FTPClient client1 = new FTPClient(host1, port1);
	FTPClient client2 = new FTPClient(host2, port2);
	test3Party_setParams(client1, user1, password1);
	test3Party_setParams(client2, user2, password2);
	client1.transfer(sourceFile, client2, destFile, false, null);
	client1.close();
	client2.close();
    }
    
    private void test3Party_setParams(FTPClient client, 
				      String user,
				      String password) 
	throws Exception{
	client.authorize(user, password);
	// secure server: client.setProtectionBufferSize(16384);
	client.setType(Session.TYPE_IMAGE);
	client.setMode(Session.MODE_STREAM);
    }

} 


