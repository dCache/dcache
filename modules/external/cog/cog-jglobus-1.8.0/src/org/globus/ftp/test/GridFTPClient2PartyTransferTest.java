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
import org.globus.ftp.vanilla.FTPServerFacade;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.RandomAccessFile;

import org.ietf.jgss.GSSCredential;

import org.gridforum.jgss.ExtendedGSSManager;

/**
   Test GridFTPClient.get() and put()
 **/
public class GridFTPClient2PartyTransferTest extends TestCase {

    protected static Log logger = 
	LogFactory.getLog(GridFTPClient2PartyTransferTest.class.getName());

    public GridFTPClient2PartyTransferTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(GridFTPClient2PartyTransferTest.class);
    }


    public void testGetPassive() throws Exception {
	testGet(Session.SERVER_ACTIVE);
    }

    public void testGetActive() throws Exception {
	testGet(Session.SERVER_PASSIVE);

	// test EBLOCK
	testGet(Session.SERVER_PASSIVE, 
		Session.TYPE_IMAGE, 
		GridFTPSession.MODE_EBLOCK);	
    }

    public void testPutPassive() throws Exception {

	testPut(Session.SERVER_ACTIVE);
	
	// test EBLOCK
	testPut(Session.SERVER_ACTIVE,
		Session.TYPE_IMAGE, 
		GridFTPSession.MODE_EBLOCK);	
    }

    public void testPutActive() throws Exception {
	testPut(Session.SERVER_PASSIVE);
    }
    


    // --------- internal functions -----------------
    
    // test two stream binary and stream ascii
    protected void testGet(int localServerMode) throws Exception {
	testGet(localServerMode,
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);
	
	testGet(localServerMode,
		Session.TYPE_ASCII,
		Session.MODE_STREAM);
    }
    
    // test no dcau, dcau with clear, and dcau with safe
    protected void testGet(int localServerMode,
			   int transferType,
			   int transferMode) throws Exception {
	
	testGet(localServerMode, transferType, transferMode,
		DataChannelAuthentication.NONE, GridFTPSession.PROTECTION_CLEAR);
	
	testGet(localServerMode, transferType, transferMode,
		DataChannelAuthentication.SELF, GridFTPSession.PROTECTION_CLEAR);
	
	testGet(localServerMode, transferType, transferMode,
		DataChannelAuthentication.SELF, GridFTPSession.PROTECTION_SAFE);
    }
	
    protected void testGet(int localServerMode,
			   int transferType,
			   int transferMode,
			   DataChannelAuthentication dcau,
			   int prot) throws Exception {
	
	String smode = (localServerMode == Session.SERVER_PASSIVE)?
	    "pasv" : "actv";
	String tmode = (transferMode == Session.MODE_STREAM) ?
	    "stream" : "eblock";
	String ttype = (transferType == Session.TYPE_ASCII) ?
	    "ascii" : "image";
	String dcauStr = (dcau == DataChannelAuthentication.NONE) ? 
	    "nodcau" : "dcau";
	String protStr = (prot == GridFTPSession.PROTECTION_CLEAR) ?
	    "clear" : "safe";
	    
	logger.info("with configuration: " + dcauStr + ", " + protStr);	    
	    
	String fullRemoteFile = TestEnv.serverADir + "/" + TestEnv.serverASmallFile;
	    
	String fullLocalFile = 
	    TestEnv.localDestDir + 
	    "/c2ptt.get." + smode + "." + tmode +"." + ttype 
	    + "." + dcauStr + "." + protStr + "." + System.currentTimeMillis();
	    
	GridFTPClient client = new GridFTPClient(TestEnv.serverAHost,
						 TestEnv.serverAPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));

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

    // test two stream binary and stream ascii
    protected void testPut(int localServerMode) throws Exception {
	testPut(localServerMode,
		Session.TYPE_IMAGE, 
		Session.MODE_STREAM);

	testPut(localServerMode,
		Session.TYPE_ASCII,
		Session.MODE_STREAM);
    }
    
    // test no dcau, dcau with clear, and dcau with safe
    protected void testPut(int localServerMode,
			   int transferType,
			   int transferMode) throws Exception {
	
	testPut(localServerMode, transferType, transferMode,
		DataChannelAuthentication.NONE, GridFTPSession.PROTECTION_CLEAR);
	
	testPut(localServerMode, transferType, transferMode,
		DataChannelAuthentication.SELF, GridFTPSession.PROTECTION_CLEAR);
	
	testPut(localServerMode, transferType, transferMode,
		DataChannelAuthentication.SELF, GridFTPSession.PROTECTION_SAFE);
    }
    
    protected void testPut(int localServerMode,
			   int transferType,
			   int transferMode,
			   DataChannelAuthentication dcau,
			   int prot) throws Exception {
	
	String smode = (localServerMode == Session.SERVER_PASSIVE)?
	    "pasv" : "actv";
	String tmode = (transferMode == Session.MODE_STREAM) ?
	    "stream" : "eblock";
	String ttype = (transferType == Session.TYPE_ASCII) ?
	    "ascii" : "image";
	String dcauStr = (dcau == DataChannelAuthentication.NONE) ? 
	    "nodcau" : "dcau";
	String protStr = (prot == GridFTPSession.PROTECTION_CLEAR) ?
	    "clear" : "safe";
	    
	logger.info("with configuration: " + dcauStr + ", " + protStr);	    

	String fullLocalFile =  TestEnv.localSrcDir + "/" + TestEnv.localSrcFile;
	
	String fullRemoteFile = 
	    TestEnv.serverBDir +
	    "/c2ptt.put." + smode + "." + tmode +"." + ttype 
	    + "." + dcauStr + "." + protStr + "." + System.currentTimeMillis();

	GridFTPClient client = new GridFTPClient(TestEnv.serverBHost,
						 TestEnv.serverBPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverBSubject));
	
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
		client.setLocalPassive(); 
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
    
    protected GSSCredential getCredential() throws Exception {
	return ExtendedGSSManager.getInstance().createCredential(GSSCredential.INITIATE_AND_ACCEPT);
    }
    
} 
