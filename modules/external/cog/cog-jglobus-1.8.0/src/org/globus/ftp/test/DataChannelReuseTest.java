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

import org.globus.ftp.GridFTPClient;
import org.globus.ftp.RetrieveOptions;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.FileRandomIO;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSource;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.RandomAccessFile;

import org.ietf.jgss.GSSCredential;

public class DataChannelReuseTest extends TestCase {

    private static Log logger =
	LogFactory.getLog(DataChannelReuseTest.class.getName());
    
    // for incoming transfers
    protected String remoteSrcFile1 =
	TestEnv.serverADir + "/" + TestEnv.serverAFile;
    protected String remoteSrcFile2 =
	TestEnv.serverADir + "/" + TestEnv.serverAFile;
    protected String remoteSrcFile3 =
	TestEnv.serverADir + "/" + TestEnv.serverAFile;
    
    protected String localDestFile1 =
	TestEnv.localDestDir + "/" + TestEnv.serverAFile;
    protected String localDestFile2 =
	TestEnv.localDestDir + "/" + TestEnv.serverAFile;
    protected String localDestFile3 =
	TestEnv.localDestDir + "/" + TestEnv.serverAFile;

    // for outgoing transfers
    protected String localSrcFile1 =
	TestEnv.localSrcDir + "/" + TestEnv.localSrcFile;
    protected String localSrcFile2 =
	TestEnv.localSrcDir + "/" + TestEnv.localSrcFile;
    protected String localSrcFile3 =
	TestEnv.localSrcDir + "/" + TestEnv.localSrcFile;
    
    protected String remoteDestFile1 =
	TestEnv.serverBDir + "/" + TestEnv.localSrcFile;
    protected String remoteDestFile2 =
	TestEnv.serverBDir + "/" + "somefile";
    protected String remoteDestFile3 =
	TestEnv.serverBDir + "/" + TestEnv.localSrcFile;
    
    public DataChannelReuseTest(String name) {
	super(name);
    }
    
    public static void main(String[] args) throws Exception {
	junit.textui.TestRunner.run(suite());
    }
    
    public static Test suite() {
	return new TestSuite(DataChannelReuseTest.class);
    }
    
    /*
      reuse data channels
    */
    public void testBasicGet() throws Exception {
	logger.info("basic data channel reuse");
	GridFTPClient client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	
	setParamsModeE(client, null); /* use default cred */
	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	
	DataSink sink1 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.setLocalPassive();
	client.setActive();
	client.get(remoteSrcFile1, sink1, null);
	sink1.close();
	
	DataSink sink2 =
	    new FileRandomIO(new RandomAccessFile(localDestFile2, "rw"));
	client.get(remoteSrcFile2, sink2, null);
	sink2.close();
	
	DataSink sink3 =
	    new FileRandomIO(new RandomAccessFile(localDestFile3, "rw"));
	client.get(remoteSrcFile3, sink3, null);
	sink3.close();
	
	client.close();
    }

    /*
      reuse data channels with put operations
    */
    public void testBasicPut() throws Exception {
	logger.info("basic data channel reuse with put");
	
	GridFTPClient client = new GridFTPClient(TestEnv.serverBHost, TestEnv.serverBPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverBSubject));

	setParamsModeE(client, null); /* use default cred */
	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	
	client.setPassive();
	client.setLocalActive();
	
	// transfer
	DataSource source1 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile1, "r"));
	client.put(remoteDestFile1, source1, null);
	source1.close();
	// check that everything got transferred
	long size = client.getSize(remoteDestFile1);               	    
	File f = new File(localSrcFile1);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	DataSource source2 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile2, "r"));
	client.put(remoteDestFile2, source2, null);
	source2.close();
	size = client.getSize(remoteDestFile2);               	    
	f = new File(localSrcFile2);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);

	DataSource  source3 = 
	    new FileRandomIO(new RandomAccessFile(localSrcFile3, "rw"));
	client.put(remoteDestFile3, source3, null);
	source3.close();
	size = client.getSize(remoteDestFile3);               	    
	f = new File(localSrcFile3);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	
	client.close();
    }

    /*
      changing d.c. protection
    */
    public void testGetProtection() throws Exception {
	logger.info("data channel reuse with changing d.c. protection");

	GridFTPClient client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));

	setParamsModeE(client, null); 

	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	client.setPassiveMode(false);

	DataSink sink1 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.setLocalPassive();
	client.setActive();
	client.get(remoteSrcFile1, sink1, null);
	sink1.close();
	
	client.setDataChannelProtection(GridFTPSession.PROTECTION_SAFE);
	client.setPassiveMode(false);

	DataSink sink2 =
	    new FileRandomIO(new RandomAccessFile(localDestFile2, "rw"));
	client.get(remoteSrcFile2, sink2, null);
	sink2.close();
	
	client.setDataChannelProtection(GridFTPSession.PROTECTION_PRIVATE);
	client.setPassiveMode(false);

	DataSink sink3 =
	    new FileRandomIO(new RandomAccessFile(localDestFile3, "rw"));
	client.get(remoteSrcFile3, sink3, null);
	sink3.close();
	
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	client.setPassiveMode(false);

	// use file1 again
	DataSink sink4 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.setLocalPassive();
	client.setActive();
	client.get(remoteSrcFile1, sink4, null);
	sink4.close();
	
	client.close();
    }

    /*
      changing d.c. protection.
    */
    public void testPutProtection() throws Exception {
	logger.info("data channel reuse with put and protection");

	GridFTPClient client = new GridFTPClient(TestEnv.serverBHost, TestEnv.serverBPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverBSubject));

	setParamsModeE(client, null); 

	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	client.setPassiveMode(true);
	
	// transfer
	DataSource source1 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile1, "r"));
	client.put(remoteDestFile1, source1, null);
	source1.close();
	// check that everything got transferred
	long size = client.getSize(remoteDestFile1);               	    
	File f = new File(localSrcFile1);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	client.setDataChannelProtection(GridFTPSession.PROTECTION_PRIVATE);
	client.setPassiveMode(true);

	DataSource source2 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile2, "r"));
	client.put(remoteDestFile2, source2, null);
	source2.close();
	size = client.getSize(remoteDestFile2);               	    
	f = new File(localSrcFile2);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	client.setPassiveMode(true);

	DataSource  source3 = 
	    new FileRandomIO(new RandomAccessFile(localSrcFile3, "rw"));
	client.put(remoteDestFile3, source3, null);
	source3.close();
	size = client.getSize(remoteDestFile3);               	    
	f = new File(localSrcFile3);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	
	client.close();
    }

    /*
      reuse data channels with changing TCP buffer size
    */
    public void testGetTCPBuffer() throws Exception {
	logger.info("data channel reuse with changing TCP buffer size");

	GridFTPClient client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));

	setParamsModeE(client, null); /* use default cred */
	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	
	client.setLocalPassive();
	client.setActive();
	
	client.setLocalTCPBufferSize(16000);
	
	DataSink sink1 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.get(remoteSrcFile1, sink1, null);
	sink1.close();
	
	client.setLocalTCPBufferSize(1234567);
	
	DataSink sink2 =
	    new FileRandomIO(new RandomAccessFile(localDestFile2, "rw"));
	client.get(remoteSrcFile1, sink2, null);
	sink2.close();
	
	client.setLocalTCPBufferSize(80003);
	
	DataSink sink3 =
	    new FileRandomIO(new RandomAccessFile(localDestFile3, "rw"));
	client.get(remoteSrcFile1, sink3, null);
	sink3.close();
	
	client.setLocalTCPBufferSize(55500);
	
	// using file1 again
	DataSink sink4 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.get(remoteSrcFile1, sink4, null);
	sink4.close();
	
	client.close();
    }

    /*
      reuse data channels with put operations and changing TCP buffer
    */
    public void testPutTCPBuffer() throws Exception {
	logger.info("data channel reuse with put and changing tcp buffer");

	GridFTPClient client = new GridFTPClient(TestEnv.serverBHost, TestEnv.serverBPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverBSubject));

	setParamsModeE(client, null); /* use default cred */
	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	
	client.setPassive();
	client.setLocalActive();
	
	client.setLocalTCPBufferSize(16000);
	
	// transfer
	DataSource source1 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile1, "r"));
	client.put(remoteDestFile1, source1, null);
	source1.close();
	// check that everything got transferred
	long size = client.getSize(remoteDestFile1);               	    
	File f = new File(localSrcFile1);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	client.setLocalTCPBufferSize(1234567);
	
	DataSource source2 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile2, "r"));
	client.put(remoteDestFile2, source2, null);
	source2.close();
	size = client.getSize(remoteDestFile2);               	    
	f = new File(localSrcFile2);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	client.setLocalTCPBufferSize(55500);
	
	DataSource  source3 = 
	    new FileRandomIO(new RandomAccessFile(localSrcFile3, "rw"));
	client.put(remoteDestFile3, source3, null);
	source3.close();
	size = client.getSize(remoteDestFile3);               	    
	f = new File(localSrcFile3);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	
	client.close();
    }

    /*
      reuse data channels, interspersed by setActive and setPassive calls
      (which tear all reused connections)
    */
    public void testGetTearing() throws Exception {
	logger.info("data channel reuse interspersed by setActive/passive");

	GridFTPClient client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	
	setParamsModeE(client, null); /* use default cred */
	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	
	client.setLocalPassive();
	client.setActive();
	
	DataSink sink1 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.get(remoteSrcFile1, sink1, null);
	sink1.close();
	
	DataSink sink2 =
	    new FileRandomIO(new RandomAccessFile(localDestFile2, "rw"));
	client.get(remoteSrcFile2, sink2, null);
	sink2.close();
	
	// tear data connections
	client.setLocalPassive();
	client.setActive();
	
	DataSink sink3 =
	    new FileRandomIO(new RandomAccessFile(localDestFile3, "rw"));
	client.get(remoteSrcFile3, sink3, null);
	sink3.close();
	
	// reusing file1 again
	
	DataSink sink4 =
	    new FileRandomIO(new RandomAccessFile(localDestFile1, "rw"));
	client.get(remoteSrcFile1, sink4, null);
	sink4.close();
	
	client.close();
    }
    
    /*
      reuse data channels with put operations,
      interspersed by setActive and setPassive calls
      (which tear all connections)
    */
    public void testPutTearing() throws Exception {
	logger.info("data channel reuse with put and interspersed setActive/passive");

	GridFTPClient client = new GridFTPClient(TestEnv.serverBHost, TestEnv.serverBPort);
	client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverBSubject));

	setParamsModeE(client, null); /* use default cred */
	client.setOptions(new RetrieveOptions(TestEnv.parallelism));
	client.setDataChannelProtection(GridFTPSession.PROTECTION_CLEAR);
	
	client.setPassive();
	client.setLocalActive();
	
	// transfer
	DataSource source1 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile1, "r"));
	client.put(remoteDestFile1, source1, null);
	source1.close();
	// check that everything got transferred
	long size = client.getSize(remoteDestFile1);               	    
	File f = new File(localSrcFile1);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);

	DataSource source2 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile2, "r"));
	client.put(remoteDestFile2, source2, null);
	source2.close();
	size = client.getSize(remoteDestFile2);               	    
	f = new File(localSrcFile2);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	// tear data connections
	client.setPassive();
	client.setLocalActive();
	
	DataSource  source3 = 
	    new FileRandomIO(new RandomAccessFile(localSrcFile3, "rw"));
	client.put(remoteDestFile3, source3, null);
	source3.close();
	size = client.getSize(remoteDestFile3);               	    
	f = new File(localSrcFile3);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	// transfer file1 again
	DataSource source4 =
	    new FileRandomIO(new RandomAccessFile(localSrcFile1, "r"));
	client.put(remoteDestFile1, source4, null);
	source4.close();
	// check that everything got transferred
	size = client.getSize(remoteDestFile1);               	    
	f = new File(localSrcFile1);
	logger.debug("comparing size: " + size + " <-> " + f.length() );
	assertEquals(f.length(), size);
	
	client.close();
    }
    
    private void setParamsModeE(GridFTPClient client, GSSCredential cred)
	throws Exception {
	client.authenticate(cred);
	client.setProtectionBufferSize(16384);
	client.setType(GridFTPSession.TYPE_IMAGE);
	client.setMode(GridFTPSession.MODE_EBLOCK);
	client.setDataChannelAuthentication(DataChannelAuthentication.SELF);
	client.setDataChannelProtection(GridFTPSession.PROTECTION_SAFE);
    }
    
}
