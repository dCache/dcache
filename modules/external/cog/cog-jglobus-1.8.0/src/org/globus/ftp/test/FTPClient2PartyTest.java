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
import org.globus.ftp.DataSource;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSinkStream;
import org.globus.ftp.DataSourceStream;
import org.globus.ftp.exception.ServerException;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileInputStream;

/**
   Test FTPClient.get() and put()
 **/
public class FTPClient2PartyTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(FTPClient2PartyTest.class.getName());

    protected FTPClient src = null;
    // note that this can be always null, because
    // user is not obliged to provide FTP destination server
    protected FTPClient dest = null;

    public FTPClient2PartyTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(FTPClient2PartyTest.class);
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

    public void testGet() throws Exception {
	logger.info("get");

	testGet(TestEnv.serverFHost,
		TestEnv.serverFPort,
		TestEnv.serverFUser,
		TestEnv.serverFPassword,
		TestEnv.serverFDir,
		TestEnv.serverFFile,
		TestEnv.localDestDir);
    }

    public void testPut() throws Exception {
	logger.info("put");

	if (skipTest(TestEnv.serverGHost, "serverGHost undefined")) {
	    return;
	}

	testPut(TestEnv.serverGHost,
		TestEnv.serverGPort,
		TestEnv.serverGUser,
		TestEnv.serverGPassword,
		TestEnv.serverGDir,
		TestEnv.localSrcDir,
		TestEnv.localSrcFile);
    }

    /** 
	Try transferring file to and from bad port on existing server.
	IOException should be thrown.
    **/
    public void testGetNoSuchPort() throws Exception{

	if (TestEnv.serverANoSuchPort == TestEnv.UNDEFINED) {
	    logger.info("Omitting the test: test3Party_noSuchPort");
	    logger.info("because some necessary properties are not defined.");
	    return;
	}
	logger.info("get from non existent port");
	boolean caughtOK = false;
	try {

	    testGet(TestEnv.serverFHost,
		    TestEnv.serverFNoSuchPort,
		    TestEnv.serverFUser,
		    TestEnv.serverFPassword,
		    TestEnv.serverFDir,
		    TestEnv.serverFFile,
		    TestEnv.localDestDir);

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
	
	logger.info("put to non existent port");
	caughtOK = false;
	try {

	    testPut(TestEnv.serverAHost,
		    TestEnv.serverANoSuchPort,
		    TestEnv.serverGUser,
		    TestEnv.serverGPassword,
		    TestEnv.serverGDir,
		    TestEnv.localSrcDir,
		    TestEnv.localSrcFile);
	    
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

    /** 
	Try transferring file to and from non existent server.
	IOException should be thrown.
    **/
    public void testGetNoSuchServer() throws Exception{
	logger.info("get from non existent server");
	boolean caughtOK = false;
	try {

	    testGet(TestEnv.noSuchServer,
		    TestEnv.serverFPort,
		    TestEnv.serverFUser,
		    TestEnv.serverFPassword,
		    TestEnv.serverFDir,
		    TestEnv.serverFFile,
		    TestEnv.localDestDir);

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
	
	logger.info("put to non existent server");
	caughtOK = false;
	try {
	    	    
	    testPut(TestEnv.noSuchServer,
		    TestEnv.serverFPort,
		    TestEnv.serverFUser,
		    TestEnv.serverFPassword,
		    TestEnv.serverFDir,
		    TestEnv.serverFFile,
		    TestEnv.localDestDir);

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

    /** 
	try transferring non existent file;
	ServerException should be thrown
    **/
    
    public void testGetNoSuchSrcFile() throws Exception{
	logger.info("get with bad src file");
	
	try {
	    testGet(TestEnv.serverFHost,
		    TestEnv.serverFPort,
		    TestEnv.serverFUser,
		    TestEnv.serverFPassword,
		    TestEnv.serverFDir,
		    TestEnv.serverFNoSuchFile,
		    TestEnv.localDestDir);
	} catch (ServerException e) {
	    logger.debug("Test passed: ServerException properly thrown.", 
			 e);
	}
    }

    /** try transferring file to non existent directory;
	ServerException should be thrown.
    **/

    public void testPutNoSuchDestDir() throws Exception{
	logger.info("put with bad dest dir");

	if (skipTest(TestEnv.serverGHost, "serverGHost undefined")) {
	    return;
	}

	try {
	    testPut(TestEnv.serverGHost,
		    TestEnv.serverGPort,
		    TestEnv.serverGUser,
		    TestEnv.serverGPassword,
		    TestEnv.serverGNoSuchDir,
		    TestEnv.localSrcDir,
		    TestEnv.localSrcFile);
	} catch (ServerException e) {
	    logger.debug("Test passed: ServerException properly thrown.", e);
	}
    }

    private void testGet(String host,
			 int port,
			 String user,
			 String password,
			 String remoteDir,
			 String remoteFile,
			 String localDir)
	throws Exception{

	logger.info("active, image, stream");
	testGet(host, port, user, password, remoteDir  + "/" + remoteFile, 
		localDir,
		Session.SERVER_ACTIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	logger.info("active, ascii, stream");
	testGet(host, port, user, password, remoteDir + "/" + remoteFile, 
		localDir,		
		Session.SERVER_ACTIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);
	logger.info("pasive, image, stream");
	testGet(host, port, user, password, remoteDir  + "/" + remoteFile, 
		localDir,
		Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	logger.info("pasive, ascii, stream");
	testGet(host, port, user, password, remoteDir + "/" + remoteFile,
		localDir, 
		Session.SERVER_PASSIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);
    }

    private void testGet(String host, 
			 int port,
			 String user,
			 String password,
			 String fullRemoteFile,
			 String localDestDir,

			 int localServerMode,
			 int transferType,
			 int transferMode)
	throws Exception {
	String smode = (localServerMode == Session.SERVER_PASSIVE)?
	    "pasv" : "actv";
	String tmode = (transferMode == Session.MODE_STREAM) ?
	    "stream" : "eblok";
	String ttype = (transferType == Session.TYPE_ASCII) ?
	    "ascii" : "image";
 	String fullLocalFile = 
	    localDestDir + "/test.get." 
	    + smode + "." + tmode +"." + ttype +"."
	    + System.currentTimeMillis();
	logger.debug("will write to: " + fullLocalFile);
	FTPClient client = new FTPClient(host, port);
	testGet_setParams(client, 
			  user, 
			  password,
			  localServerMode,
			  transferType,
			  transferMode);
	DataSink sink = new DataSinkStream(new FileOutputStream(fullLocalFile));
	client.get(fullRemoteFile,
		   sink,
		   null);
	client.close();
    }
    
    protected void testGet_setParams(FTPClient client, 
				      String user,
				      String password,
				   int localServerMode,
				   int transferType,
				   int transferMode) 
	throws Exception{
	client.authorize(user, password);
	// secure server: client.setProtectionBufferSize(16384);
	client.setType(transferType);
	client.setMode(transferMode);
	if (localServerMode == Session.SERVER_ACTIVE) {
	    client.setPassive();
	    client.setLocalActive();
	} else {
	    client.setLocalPassive();
	    client.setActive();
	}
    }


    private void testPut(String host,
			 int port,
			 String user,
			 String password,
			 String remoteDir,
			 String localDir,
			 String localFile)
	throws Exception{

	logger.info("active, image, stream");
	testPut(host, port, user, password, remoteDir, localFile, 
		localDir,
		Session.SERVER_ACTIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	logger.info("active, ascii, stream");
	testPut(host, port, user, password, remoteDir, localFile, 
		localDir,		
		Session.SERVER_ACTIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);
	logger.info("pasive, image, stream");
	testPut(host, port, user, password, remoteDir, localFile, 
		localDir,
		Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	logger.info("pasive, ascii, stream");
	testPut(host, port, user, password, remoteDir, localFile,
		localDir, 
		Session.SERVER_PASSIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);
    }



    private void testPut(String host, 
			 int port,
			 String user,
			 String password,
			 String remoteDestDir,
			 String localFile,
			 String localDir,

			 int localServerMode,
			 int transferType,
			 int transferMode)
	throws Exception {
	FTPClient client = new FTPClient(host, port);
	testPut_setParams(client, 
			  user, 
			  password,
			  localServerMode,
			  transferType,
			  transferMode);
	String fullLocalFile = localDir + "/" + localFile;
	String fullRemoteFile = remoteDestDir + "/" + localFile;
	DataSource source = new DataSourceStream(new FileInputStream(fullLocalFile));
	client.put(fullRemoteFile,
		   source,
		   null);
	client.close();
    }
    
    protected void testPut_setParams(FTPClient client, 
				   String user,
				   String password,
				   int localServerMode,
				   int transferType,
				   int transferMode) 
	throws Exception{
	client.authorize(user, password);
	// secure server: client.setProtectionBufferSize(16384);
	client.setType(transferType);
	client.setMode(transferMode);
	if (localServerMode == Session.SERVER_ACTIVE) {
	    client.setPassive();
	    client.setLocalActive();
	} else {
	    client.setLocalPassive();
	    client.setActive();
	}
    }


} 


