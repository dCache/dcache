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
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.DataSource;
import org.globus.ftp.FileRandomIO;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.DataSinkStream;
import org.globus.ftp.DataSourceStream;
import org.globus.ftp.DataSink;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.vanilla.FTPServerFacade;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.RandomAccessFile;

import org.ietf.jgss.GSSCredential;

import org.gridforum.jgss.ExtendedGSSManager;

/**
   Test GridFTPClient.get() and put()
 **/
public class GridFTPClient2PartyTest extends TestCase {

    protected static Log logger = 
	LogFactory.getLog(GridFTPClient2PartyTest.class.getName());

    protected GridFTPClient src = null;
    // note that this can be always null, because
    // user is not obliged to provide FTP destination server
    protected GridFTPClient dest = null;

    public GridFTPClient2PartyTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(GridFTPClient2PartyTest.class);
    }

    public void testGet() {
	try{
	    // edit here to run multiple tests of same config
	    for(int i = 0; i<1; i++) {
		logger.info("testing get");
		testGet(TestEnv.serverAHost,
			TestEnv.serverAPort,
			TestEnv.serverASubject,
			TestEnv.serverADir,
			TestEnv.serverASmallFile,
			TestEnv.localDestDir);
	    }
	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	} 
	
    }

    public void testPut() {
	if (TestEnv.serverBHost == null) {
	    fail("Test disabled - serverBHost undefined");
	}
        try{
            // edit here to run multiple tests of same config
	    for(int i = 0; i<1; i++) {
		logger.info("testing put");
		testPut(TestEnv.serverBHost,
			TestEnv.serverBPort,
			TestEnv.serverBSubject,
			TestEnv.serverBDir,
			TestEnv.localSrcDir,
			TestEnv.localSrcFile);
	    }
        } catch (Exception e) {
            logger.error("", e);
            fail(e.toString());
        }

    }

    /** 
	Try transferring file to and from bad port on existing server.
	IOException should be thrown.
    **/
    public void testNoSuchPort() throws Exception{

	if (TestEnv.serverANoSuchPort == TestEnv.UNDEFINED) {
	    logger.info("Omitting the test: test3Party_noSuchPort");
	    logger.info("because some necessary properties are not defined.");
	    return;
	}
	logger.info("get from non existent port");
	boolean caughtOK = false;
	try {

	    testGet(TestEnv.serverAHost,
		    TestEnv.serverANoSuchPort,
		    TestEnv.serverASubject,
		    TestEnv.serverADir,
		    TestEnv.serverASmallFile,
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
		fail("Attempted to contact non existent server, " +
		     "but the expected exception has not been thrown.");
	    }
	}
	caughtOK = false;
	try {

	    testPut(TestEnv.serverAHost,
		    TestEnv.serverANoSuchPort,
		    TestEnv.serverASubject,
		    TestEnv.serverBDir,
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
		fail("Attempted to contact non existent server, "
		     + "but the expected exception has not been thrown.");
	    }
	}
    }

    /** Try transferring file to and from non existent server.
	IOException should be thrown.
    **/
    public void testNoSuchServer() throws Exception{
	logger.info("get from non existent server");
	boolean caughtOK = false;
	try {

	    testGet(TestEnv.noSuchServer,
		    TestEnv.serverAPort,
		    TestEnv.serverASubject,
		    TestEnv.serverADir,
		    TestEnv.serverASmallFile,
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
		fail("Attempted to contact non existent server, "
		     + "but the expected exception has not been thrown.");
	    }
	}
	
	logger.info("put to non existent server");
	caughtOK = false;
	try {
	    	    
	    testPut(TestEnv.noSuchServer,
		    TestEnv.serverAPort,
		    TestEnv.serverASubject,
		    TestEnv.serverADir,
		    TestEnv.serverASmallFile,
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

    /** try transferring non existent file;
	 ServerException should be thrown
    **/
    
    public void testGetNoSuchSrcFile() throws Exception{
	logger.info("get with bad src file");
	boolean serverFNoSuchFile_OK = false;
	try {

	    testGet(TestEnv.serverAHost,
		    TestEnv.serverAPort,
		    TestEnv.serverASubject,
		    TestEnv.serverADir,
		    TestEnv.serverANoSuchFile,
		    TestEnv.localDestDir);

	} catch (Exception e) {
	    if (e instanceof ServerException) {
		logger.debug(e.toString());
		serverFNoSuchFile_OK = true;
		logger.debug("Test passed: ServerException properly thrown.");
	    } else {
		logger.error("", e);
		fail(e.toString());
	    }
	} finally {
	    if (!serverFNoSuchFile_OK) {
		fail("Attempted to transfer non existent file, but " +
		     "no exception has been thrown.");
	    }
	}
    }
    

    /** try transferring file to non existent directory;
	ServerException should be thrown.
    **/
    public void testPutNoSuchDestDir() throws Exception{
       logger.info("put with bad dest dir");
	boolean serverGNoSuchDir_OK = false;
	try {

	    testPut(TestEnv.serverBHost,
		    TestEnv.serverBPort,
		    TestEnv.serverBSubject,
		    TestEnv.serverBNoSuchDir,
		    TestEnv.localSrcDir,
		    TestEnv.localSrcFile);

	} catch (Exception e) {
	    if (e instanceof ServerException) {
		logger.debug(e.toString());
		serverGNoSuchDir_OK = true;
		logger.debug("Test passed: ServerException properly thrown.");
	    } else {
		logger.error("", e);
		fail(e.toString());
	    }
	} finally {
	    if (!serverGNoSuchDir_OK) {
		fail("Attempted to transfer to non existent dir, but " +
		     "no exception has been thrown.");
	    }
	}
    }

    protected void testGet(String host,
			   int port,
			   String subject,
			   String remoteDir,
			   String remoteFile,
			   String localDir)
	throws Exception{

	logger.info("with configuration: passive, image, eblock");
	testGet(host, port,  subject,
		remoteDir + "/" + remoteFile, 
		localDir,		
		Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		GridFTPSession.MODE_EBLOCK);

	logger.info("with configuration: passive, image, stream");
	testGet(host, port, subject,
		remoteDir  + "/" + remoteFile, 
		localDir,
		Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);

	logger.info("with configuration: passive, ascii, stream");
	testGet(host, port, subject,
		remoteDir + "/" + remoteFile,
		localDir, 
		Session.SERVER_PASSIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);

    }

    protected void testGet(String host, 
			   int port,
			   String subject,
			   String fullRemoteFile,
			   String localDestDir,
			   
			   int localServerMode,
			   int transferType,
			   int transferMode)
	throws Exception {
	String smode = (localServerMode == Session.SERVER_PASSIVE)?
	    "pasv" : "actv";
	String tmode = (transferMode == Session.MODE_STREAM) ?
	    "stream" : "eblock";
	String ttype = (transferType == Session.TYPE_ASCII) ?
	    "ascii" : "image";

	DataChannelAuthentication dcau = null;
	int prot = -1;

	for (int i = 0; i < 3; i ++) {
	    
	    switch (i) {
	    case 0:
		dcau = DataChannelAuthentication.NONE;
		prot = GridFTPSession.PROTECTION_CLEAR;
		break;
	    case 1:
		dcau = DataChannelAuthentication.SELF;
		prot = GridFTPSession.PROTECTION_CLEAR;
		break;
	    case 2:
		dcau = DataChannelAuthentication.SELF;
		prot = GridFTPSession.PROTECTION_SAFE;
		break;
	    default:
		throw new Exception();
	    }

	    String dcauStr = (dcau == DataChannelAuthentication.NONE) ?
		"nodcau" : "dcau" ;
	    
	    String protStr = (prot == GridFTPSession.PROTECTION_CLEAR) ? 
		"clear" : "safe";

	    logger.info("with configuration: " + dcauStr + ", " + protStr); 
	    
	    String fullLocalFile = 
		localDestDir + 
		"/c2p2.get." + smode + "." + tmode +"." + ttype 
		+ "." + dcauStr + "." + protStr + "." + 
		System.currentTimeMillis();
	    
	    GridFTPClient client = new GridFTPClient(host, port);
	    client.setAuthorization(TestEnv.getAuthorization(subject));

	    get(client, 
		localServerMode,
		transferType,
		transferMode,
		dcau,
		prot,
		fullLocalFile,
		fullRemoteFile);
	    
	    long size = client.getSize(fullRemoteFile);

	    client.close();

	    // if type = ASCII, file size before and after transfer may
	    // differ, otherwise they shouldn't
	    if (transferType != Session.TYPE_ASCII) {
		File f = new File(fullLocalFile);
		assertEquals(fullRemoteFile + " -> " + fullLocalFile,
			     size, f.length());
	    }
	}
    }
    
    /**
       This method performs the actual transfer
     **/
    protected void get(GridFTPClient client, 
		       int localServerMode,
		       int transferType,
		       int transferMode,
		       DataChannelAuthentication dcau,
		       int prot,
		       String fullLocalFile,
		       String fullRemoteFile) 
	throws Exception{
	client.authenticate(getCredential()); /* use default cred */
	client.setProtectionBufferSize(16384);
	client.setType(transferType);
	client.setMode(transferMode);
	client.setDataChannelAuthentication(dcau);
 	client.setDataChannelProtection(prot);
	if (localServerMode == Session.SERVER_ACTIVE) {
	    client.setPassive();
	    client.setLocalActive();
	} else {
		client.setLocalPassive(); 
	    client.setActive();
	}

	DataSink sink = null;
	if (transferMode == GridFTPSession.MODE_EBLOCK) {
	    sink = new FileRandomIO(new RandomAccessFile(fullLocalFile, 
							 "rw"));
	} else {
	    sink = new DataSinkStream(new FileOutputStream(fullLocalFile));
	}

	client.get(fullRemoteFile, sink, null);
    }


    protected void testPut(String host,
			   int port,
			   String subject,
			   String remoteDir,
			   String localDir,
			   String localFile)
	throws Exception{

	logger.info("with configuration: active, image, eblock");
	testPut(host, port, subject,
		remoteDir, localFile, 
		localDir,		
		Session.SERVER_ACTIVE, 
		Session.TYPE_IMAGE, 
		GridFTPSession.MODE_EBLOCK);
	logger.info("with configuration: active, image, stream");
	testPut(host, port, subject,
		remoteDir, localFile, 
		localDir,
		Session.SERVER_ACTIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	logger.info("with configuration: active, ascii, stream");
	testPut(host, port, subject,
		remoteDir, localFile, 
		localDir,		
		Session.SERVER_ACTIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);

	/* cannot put with passive
	logger.info("pasive, image, eblock");
	testPut(host, port,  remoteDir, localFile, 
		localDir,
		Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_EBLOCK);
	logger.info("pasive, image, stream");
	testPut(host, port,  remoteDir, localFile, 
		localDir,
		Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	logger.info("pasive, ascii, stream");
	testPut(host, port,  remoteDir, localFile,
		localDir, 
		Session.SERVER_PASSIVE, 
		Session.TYPE_ASCII, 
		Session.MODE_STREAM);
	*/
    }



    protected void testPut(String host, 
			   int port,
			   String subject,
			   String remoteDestDir,
			   String localFile,
			   String localDir,
			   
			   int localServerMode,
			   int transferType,
			   int transferMode)
	throws Exception {
	String smode = (localServerMode == Session.SERVER_PASSIVE)?
	    "pasv" : "actv";
	String tmode = (transferMode == Session.MODE_STREAM) ?
	    "stream" : "eblock";
	String ttype = (transferType == Session.TYPE_ASCII) ?
	    "ascii" : "image";

	DataChannelAuthentication dcau = null;
	int prot = -1;

	for (int i = 0; i < 3; i ++) {
	    
	    switch (i) {
	    case 0:
		dcau = DataChannelAuthentication.NONE;
		prot = GridFTPSession.PROTECTION_CLEAR;
		break;
	    case 1:
		dcau = DataChannelAuthentication.SELF;
		prot = GridFTPSession.PROTECTION_CLEAR;
		break;
	    case 2:
		dcau = DataChannelAuthentication.SELF;
		prot = GridFTPSession.PROTECTION_SAFE;
		break;
	    default:
		throw new Exception();
	    }

	    String dcauStr = (dcau == DataChannelAuthentication.NONE) ?
		"nodcau" : "dcau" ;
	    
	    String protStr = (prot == GridFTPSession.PROTECTION_CLEAR) ? 
		"clear" : "safe";
	    
	    logger.info("with configuration: " + dcauStr + ", " + protStr); 
	    
	    String fullLocalFile = localDir + "/" + localFile;

	    String fullRemoteFile = remoteDestDir + "/c2p2.put." + 
		smode + "." + tmode +"." + ttype 
		+ "." + dcauStr + "." + protStr + "." + System.currentTimeMillis();
	    
	    GridFTPClient client = new GridFTPClient(host, port);
	    client.setAuthorization(TestEnv.getAuthorization(subject));

	    put(client, 
		localServerMode,
		transferType,
		transferMode,
		dcau,
		prot,
		fullLocalFile,
		fullRemoteFile);

	    long size = client.getSize(fullRemoteFile);

	    client.close();

	    // if type = ASCII, file sizes before and after transfer may
	    // differ, otherwise they shouldn't
	    if (transferType != Session.TYPE_ASCII) {
		File f = new File(fullLocalFile);
		assertEquals(fullLocalFile + " -> " + fullRemoteFile, 
			     f.length(), size);
	    }
	}
    }

    /**
       This method performs the actual transfer
     **/
    protected void put(GridFTPClient client, 
		       int localServerMode,
		       int transferType,
		       int transferMode,
		       DataChannelAuthentication dcau,
		       int prot,
		       String fullLocalFile,
		       String fullRemoteFile) 
	throws Exception{
	client.authenticate(getCredential()); /* use default cred */
	client.setProtectionBufferSize(16384);
	client.setType(transferType);
	client.setMode(transferMode);
 	client.setDataChannelAuthentication(dcau);
 	client.setDataChannelProtection(prot);

	if (localServerMode == Session.SERVER_ACTIVE) {
	    client.setPassive();
	    client.setLocalActive();
	} else {
	    if (TestEnv.localServerPort == TestEnv.UNDEFINED) {
		client.setLocalPassive(); 
	    } else {
		client.setLocalPassive(TestEnv.localServerPort, 
				       FTPServerFacade.DEFAULT_QUEUE);
	    }
	    client.setActive();
	}

	logger.debug("sending file " + fullLocalFile);

	DataSource source = null;
	if (transferMode == GridFTPSession.MODE_EBLOCK) {
	    source = new FileRandomIO(new RandomAccessFile(fullLocalFile, 
							   "r"));
	} else {
	    source = new DataSourceStream(new FileInputStream(fullLocalFile));
	}
	
	client.put(fullRemoteFile, source, null);
    }
    
    private GSSCredential getCredential() throws Exception {
	return ExtendedGSSManager.getInstance().createCredential(GSSCredential.INITIATE_AND_ACCEPT);
    }
    
} 
