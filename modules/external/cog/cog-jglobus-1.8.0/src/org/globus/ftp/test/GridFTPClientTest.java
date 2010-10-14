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
import org.globus.ftp.HostPortList;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.StreamModeRestartMarker;
import org.globus.ftp.FeatureList;
import org.globus.ftp.FileInfo;
import org.globus.ftp.Session;
import org.globus.ftp.HostPort;
import org.globus.ftp.Options;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSource;
import org.globus.ftp.Buffer;
import org.globus.ftp.vanilla.Reply;
import org.globus.ftp.ChecksumAlgorithm;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.MlsxEntry;
import org.globus.ftp.FileRandomIO;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.util.Vector;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

import org.ietf.jgss.GSSCredential;

public class GridFTPClientTest extends TestCase {
 
    private static Log logger = 
        LogFactory.getLog(GridFTPClientTest.class.getName());

    public GridFTPClientTest(String name) {
        super(name);
    }

    public static void main (String[] args) throws Exception{
        junit.textui.TestRunner.run(suite());   
    }

    public static Test suite ( ) {
        return new TestSuite(GridFTPClientTest.class);
    }

    private GridFTPClient connect() throws Exception {
        GridFTPClient client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
        client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
        client.authenticate(null);
        return client;
    }

    public void testControlChannelProtectionEnc() throws Exception {
        GridFTPClient client = null;
        
        client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
        client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
        client.setControlChannelProtection(GridFTPSession.PROTECTION_PRIVATE);
        client.authenticate(null);
        
        client.getCurrentDir();
        
        client.close();
    }

    public void testControlChannelProtectionSig() throws Exception {
        GridFTPClient client = null;
        
        client = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
        client.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
        client.setControlChannelProtection(GridFTPSession.PROTECTION_SAFE);
        client.authenticate(null);
        
        client.getCurrentDir();
        
        client.close();
    }

    public void testExists() throws Exception {
        GridFTPClient client = connect();
        assertTrue("file", client.exists(TestEnv.serverADir + "/" + TestEnv.serverAFile));
        assertTrue("dir", client.exists(TestEnv.serverADir));
        assertFalse("file2", client.exists("foobar"));
        client.close();
    }

    public void testSize() throws Exception {
        logger.info("getSize()");
        
        GridFTPClient client = connect();
        client.changeDir(TestEnv.serverADir);
        assertEquals(true, client.exists(TestEnv.serverAFile));
        client.setType(Session.TYPE_IMAGE);

        long size = client.getSize(TestEnv.serverAFile);
        assertEquals(TestEnv.serverAFileSize, size);

        Date d = client.getLastModified(TestEnv.serverAFile);

        client.close();
    }
    
    public void testDir() throws Exception {
        logger.info("makeDir()");
        
        GridFTPClient client = connect();
        String tmpDir = "abcdef";
        String baseDir = client.getCurrentDir();
        client.makeDir(tmpDir);
        client.changeDir(tmpDir);
        assertEquals(baseDir + "/" + tmpDir, client.getCurrentDir());
        client.goUpDir();
        assertEquals(baseDir, client.getCurrentDir());
        client.deleteDir(tmpDir);
        try {
            client.changeDir(tmpDir);
            fail("directory should have been removed");
        } catch (Exception e) {
        }
        client.close();
    }

    public void testFeature() throws Exception {
        logger.info("getFeatureList()");
        
        GridFTPClient client = connect();
        FeatureList fl = client.getFeatureList();
        assertEquals(true, fl.contains("DcaU"));
        assertEquals(false, fl.contains("MIS"));

        assertTrue(client.isFeatureSupported("DcaU"));
        assertFalse(client.isFeatureSupported("MIS"));

        client.close();
    }

    public void testQuote() throws Exception {
        GridFTPClient client = connect();
        client.setType(Session.TYPE_IMAGE);
        client.changeDir(TestEnv.serverADir);
        
        Reply r = client.quote("size " + TestEnv.serverAFile);
        
        assertTrue(Reply.isPositiveCompletion(r));
        assertEquals(TestEnv.serverAFileSize, Long.parseLong(r.getMessage()));

        client.close();
    }

    public void testSite() throws Exception {
        GridFTPClient client = connect();
        
        Reply r = client.site("help"); 
        
        assertTrue(Reply.isPositiveCompletion(r));
        assertTrue(r.getMessage().indexOf("PASV") != -1);

        client.close();
    }

   public void testDirRename() throws Exception {
        
        GridFTPClient client = connect();

        String tmpDir = "abcdef";

        client.makeDir(tmpDir);

        String newName = "foo-" + System.currentTimeMillis();

        client.rename(tmpDir, newName);

        client.rename(newName, tmpDir);

        client.deleteDir(tmpDir);

        client.close();
    }

    public void testOptions() throws Exception {
        logger.info("retrieveOptions()");
        
        GridFTPClient client = connect();
        Options opts = new RetrieveOptions(3);
        client.setOptions(opts);
        client.close();
    }

    public void testRestartMarker() throws Exception {
        logger.info("setRestartMarker()");

        GridFTPClient client = connect();
        StreamModeRestartMarker rm = new StreamModeRestartMarker(12345);
        client.setRestartMarker(rm);
        client.close();
    }
    
    public void testAllocate() throws Exception {
        GridFTPClient client = connect();
        
        client.allocate(5);
        
        client.close();
    }

    public void testChecksum() throws Exception {
        GridFTPClient client = connect();
        
        String checksum = 
            client.checksum(ChecksumAlgorithm.MD5, 
                            0, TestEnv.serverAFileSize,
                            TestEnv.serverADir + "/" + TestEnv.serverAFile);

        assertEquals(TestEnv.serverAFileChecksum, checksum.trim());
        
        client.close();
    }
    
    public void testListAscii() throws Exception {
        testList(Session.MODE_STREAM,
                 Session.TYPE_ASCII);
    }

    public void testSetChecksum() throws Exception {
        GridFTPClient client = connect();

        FeatureList fl = client.getFeatureList();
        if( fl.contains("SCKS")) {
           client.setChecksum(ChecksumAlgorithm.MD5, TestEnv.serverAFileChecksum);
        }  else {
           try {
              client.setChecksum(ChecksumAlgorithm.MD5, TestEnv.serverAFileChecksum);
              //assertEquals("SCKS should not be supported by the server","SCKS is supported by the server");
           } catch ( Exception ex ){
              logger.info("SCKS is not supported by the server");
           }
        }

        client.close();
    }

    public void testListEblock() throws Exception {
        testList(GridFTPSession.MODE_EBLOCK,
                 Session.TYPE_IMAGE);
    }

    private void testList(int mode, int type) throws Exception {
        logger.info("show list output using GridFTPClient");
        
        GridFTPClient client = connect();
        client.setType(type);
        client.setMode(mode);
        client.changeDir(TestEnv.serverADir);
        Vector v = client.list(null, null);
        logger.debug("list received");
        while (! v.isEmpty()) {
            FileInfo f = (FileInfo)v.remove(0); 
            logger.info(f.toString());
        }
        client.close();
    }

    public void testNListAscii() throws Exception {
        testNList(Session.MODE_STREAM,
                  Session.TYPE_ASCII);
    }
    
    public void testNListEblock() throws Exception {
        testNList(GridFTPSession.MODE_EBLOCK,
                  Session.TYPE_IMAGE);
    }

    private void testNList(int mode, int type) throws Exception {
        logger.info("show list output using GridFTPClient");
        
        GridFTPClient client = connect();
        client.setType(type);
        client.setMode(mode);
        client.changeDir(TestEnv.serverADir);
        Vector v = client.nlist();
        logger.debug("list received");
        while (! v.isEmpty()) {
            FileInfo f = (FileInfo)v.remove(0); 
            logger.info(f.toString());
        }
        client.close();
    }

    public void testMListAscii() throws Exception {
        testMList(Session.MODE_STREAM,
                  Session.TYPE_ASCII);
    }

    public void testMListEblock() throws Exception {
        testMList(GridFTPSession.MODE_EBLOCK,
                  Session.TYPE_IMAGE);
    }

    private void testMList(int mode, int type) throws Exception {
        logger.info("show list output using GridFTPClient");
        
        GridFTPClient client = connect();
        client.setType(type);
        client.setMode(mode);
        client.changeDir(TestEnv.serverADir);
        Vector v = client.mlsd(null);
        logger.debug("list received");
        while (! v.isEmpty()) {
            MlsxEntry f = (MlsxEntry)v.remove(0); 
            logger.info(f.toString());
        }
        client.close();
    }

    public void testList2() throws Exception {
        logger.info("test two consective list, using both list functions, using GridFTPClient");
        
        GridFTPClient client = connect();

        String output1 = null;
        String output2 = null;

        // using list()

        client.changeDir(TestEnv.serverADir);
        Vector v = client.list(null, null);
        logger.debug("list received");
        StringBuffer output1Buffer = new StringBuffer();
        while (! v.isEmpty()) {
            FileInfo f = (FileInfo)v.remove(0); 
            output1Buffer.append(f.toString()).append("\n");

        }
        output1 = output1Buffer.toString();

        // using list(String,String, DataSink)

        HostPort hp2 = client.setPassive();
        client.setLocalActive();

        final ByteArrayOutputStream received2= new ByteArrayOutputStream(1000);

        // unnamed DataSink subclass will write data channel content
        // to "received" stream.

        client.list(null, null, new DataSink(){
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

         client.close();
    }

    public void testConnectionReset1() throws Exception {
        DataSink sink = (new DataSink() {
                public void write(Buffer buffer) 
                    throws IOException{
                    logger.debug("received " + buffer.getLength() +
                                 " bytes of directory listing");
                }
                public void close() 
                    throws IOException{};
            });

        GridFTPClient client = connect();

        String pwd = client.getCurrentDir();

        client.setPassiveMode(false);
        try {
            client.get(TestEnv.serverANoSuchFile, sink, null);
            fail("did not throw expected exception");
        } catch (Exception e) {
            // should fail
            //e.printStackTrace();
        }
        client.setPassiveMode(true);
        client.put(new File(TestEnv.localSrcDir + "/" + TestEnv.localSrcFile),
                   TestEnv.serverBDir + "/" + TestEnv.serverBFile,
                   false);

        assertEquals(pwd, client.getCurrentDir());

        client.close();
    }
    
    public void testConnectionReset2() throws Exception {
        DataSink sink = (new DataSink() {
                public void write(Buffer buffer) 
                    throws IOException{
                    logger.debug("received " + buffer.getLength() +
                                 " bytes of directory listing");
                }
                public void close() 
                    throws IOException{};
            });

        GridFTPClient client = connect();

        String pwd = client.getCurrentDir();

        client.setPassiveMode(true);
        try {
            client.get(TestEnv.serverANoSuchFile, sink, null);
            fail("did not throw expected exception");
        } catch (Exception e) {
            // should fail
            //e.printStackTrace();
        }
//        client.setPassiveMode(true);
  //      client.nlist();

    //    assertEquals(pwd, client.getCurrentDir());
        
        client.close();
    }

    public void testConnectionReset3() throws Exception {
        DataSink sink = (new DataSink() {
                public void write(Buffer buffer) 
                    throws IOException{
                    logger.debug("received " + buffer.getLength() +
                                 " bytes of directory listing");
                }
                public void close() 
                    throws IOException{};
            });

        GridFTPClient client = connect();

        client.setMode(GridFTPSession.MODE_EBLOCK);          
        client.setType(Session.TYPE_IMAGE);       

        String pwd = client.getCurrentDir();

        client.setOptions(new RetrieveOptions(4));
        client.setPassiveMode(false);
        try {
            client.get(TestEnv.serverANoSuchFile, sink, null);
            fail("did not throw expected exception");
        } catch (Exception e) {
            // should fail
            //e.printStackTrace();
        }

        DataSource source =
            new FileRandomIO(new RandomAccessFile(
                  TestEnv.localSrcDir + "/" + TestEnv.localSrcFile, "r"));
        client.setPassiveMode(true);
        client.put(TestEnv.serverBDir + "/" + TestEnv.serverBFile,
                   source, null);

        assertEquals(pwd, client.getCurrentDir());
        
        client.close();
    }

    // try third party transfer.  no exception should be thrown.
    public void test3PartyModeE() 
        throws Exception {
        
        logger.info("3 party mode E");
        try {
            
            test3PartyModeE(
                            TestEnv.serverAHost,
                            TestEnv.serverAPort,
                            TestEnv.serverASubject,
                            TestEnv.serverADir + "/" + TestEnv.serverAFile,
                            
                            TestEnv.serverBHost,
                            TestEnv.serverBPort,
                            TestEnv.serverBSubject,
                            TestEnv.serverBDir + "/" + TestEnv.serverBFile,
                            
                            null // use default cred
                            );
            
        } catch (Exception e) {
            logger.error("", e);
            fail(e.toString());
        } 
    }


    // try third party transfer. no exception should be thrown.
    public void test3Party() throws Exception{
        logger.info("3 party");

        try {
                    
            test3Party(
                       TestEnv.serverAHost,
                       TestEnv.serverAPort,
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverAFile,
                       
                       TestEnv.serverBHost,
                       TestEnv.serverBPort,
                       TestEnv.serverBSubject,
                       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

                       null // use default cred 
                       );

        } catch (Exception e) {
            logger.error("", e);
            fail(e.toString());
        }
    }
 
    // Try transferring file to and from bad port on existing server.
    // IOException should be thrown.
    public void test3PartyNoSuchPort() throws Exception{
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
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverAFile,
                       
                       TestEnv.serverBHost,
                       TestEnv.serverBPort,
                       TestEnv.serverBSubject,
                       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

                           null // use default cred
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
                       TestEnv.serverAHost,
                       TestEnv.serverAPort,
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverAFile,
                       
                       TestEnv.serverAHost,
                       TestEnv.serverANoSuchPort,
                       TestEnv.serverASubject,
                       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

                       null // use default cred
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

    // Try transferring file to and from non existent server.
    // IOException should be thrown.
    public void test3PartyNoSuchServer() throws Exception{
        logger.info("3 party with bad server");

        logger.debug("transfer FROM non existent server");
        boolean caughtOK = false;
        try {
                    
            test3Party(
                       TestEnv.noSuchServer,
                       TestEnv.serverAPort,
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverAFile,
                       
                       TestEnv.serverBHost,
                       TestEnv.serverBPort,
                       TestEnv.serverBSubject,
                       TestEnv.serverBDir + "/" + TestEnv.serverBFile,
                       
                       null // use default cred 
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
        
        logger.debug("transfer TO non existent server");
        caughtOK = false;
        try {
                    
            test3Party(
                       TestEnv.serverAHost,
                       TestEnv.serverAPort,
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverAFile,
                       
                       TestEnv.noSuchServer,
                       TestEnv.serverBPort,
                       TestEnv.serverBSubject,
                       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

                       null // use default cred 
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

    // try transferring non existent file; ServerException should be thrown
    public void test3PartyNoSuchSrcFile() throws Exception{
        logger.info("3 party with bad src file");
        boolean serverANoSuchFile_OK = false;
        try {

            test3Party(
                       TestEnv.serverAHost,
                       TestEnv.serverAPort,
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverANoSuchFile,
                       
                       TestEnv.serverBHost,
                       TestEnv.serverBPort,
                       TestEnv.serverBSubject,
                       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

                       null // use default cred
                       );

        } catch (Exception e) {
            if (e instanceof ServerException) {
                logger.debug(e.toString());
                serverANoSuchFile_OK = true;
                logger.debug("Test passed: ServerException properly thrown.");
            } else {
                logger.error("", e);
                fail(e.toString());
            }
        } finally {
            if (!serverANoSuchFile_OK) {
                fail("Attempted to transfer non existent file, but no exception has been thrown.");
            }
        }
    }
    

    // try transferring file to non existent directory;
    // ServerException should be thrown.
    public void test3PartyNoSuchDestDir() throws Exception{
        logger.info("3 party with bad dest dir");
        boolean serverBNoSuchDir_OK = false;
        try {
            test3Party(
                       TestEnv.serverAHost,
                       TestEnv.serverAPort,
                       TestEnv.serverASubject,
                       TestEnv.serverADir + "/" + TestEnv.serverAFile,
                       
                       TestEnv.serverBHost,
                       TestEnv.serverBPort,
                       TestEnv.serverBSubject,
                       TestEnv.serverBNoSuchDir + "/" + TestEnv.serverBFile,

                       null // use default cred
                       );

        } catch (Exception e) {
            if (e instanceof ServerException) {
                logger.debug(e.toString());
                serverBNoSuchDir_OK = true;
                logger.debug("Test passed: ServerException properly thrown.");
            } else {
                logger.error("", e);
                fail(e.toString());
            }
        } finally {
            if (!serverBNoSuchDir_OK) {
                fail("Attempted to transfer to non existent dir, but no exception has been thrown.");
            }
        }
    }

    private void test3Party(String host1, 
                            int port1,
                            String subject1,
                            String sourceFile,
                            String host2,
                            int port2,
                            String subject2,
                            String destFile,
                            GSSCredential cred)
        throws Exception {
        GridFTPClient client1 = null;
        GridFTPClient client2 = null;
        try {
            client1 = new GridFTPClient(host1, port1);
            client1.setAuthorization(TestEnv.getAuthorization(subject1));
            client2 = new GridFTPClient(host2, port2);
            client2.setAuthorization(TestEnv.getAuthorization(subject2));

            test3Party_setParams(client1, cred);
            test3Party_setParams(client2, cred);

            client1.transfer(sourceFile, client2, destFile, false, null);
        } finally {
            if (client1 != null) {
                try { 
                    client1.close(true); 
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            if (client2 != null) {
                try { 
                    client2.close(true); 
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }
    }
    
    private void test3Party_setParams(GridFTPClient client, GSSCredential cred)
        throws Exception{
        client.authenticate(cred);
        client.setProtectionBufferSize(16384);
        client.setType(GridFTPSession.TYPE_IMAGE);
        client.setMode(GridFTPSession.MODE_STREAM);
    }


    private void test3PartyModeE(String host1, 
                                 int port1,
                                 String subject1,
                                 String sourceFile,
                                 String host2,
                                 int port2,
                                 String subject2,
                                 String destFile,
                                 GSSCredential cred)
        throws Exception {
        GridFTPClient source = null;
        GridFTPClient dest = null;
        try {
            source = new GridFTPClient(host1, port1);
            source.setAuthorization(TestEnv.getAuthorization(subject1));
            dest = new GridFTPClient(host2, port2);
            dest.setAuthorization(TestEnv.getAuthorization(subject2));
            
            test3PartyModeE_setParams(source, cred);
            test3PartyModeE_setParams(dest, cred);
            source.setOptions(new RetrieveOptions(TestEnv.parallelism));
            //long size = source.getSize(sourceFile);
            
            HostPortList hpl = dest.setStripedPassive();
            source.setStripedActive(hpl);

            source.extendedTransfer(sourceFile, dest, destFile, null);
	} finally {
            if (source != null) {
                try { 
                    source.close(true); 
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            if (dest != null) {
                try { 
                    dest.close(true); 
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }
    }
    
    private void test3PartyModeE_setParams(GridFTPClient client, GSSCredential cred)
        throws Exception{
        client.authenticate(cred);
        client.setProtectionBufferSize(16384);
        client.setType(GridFTPSession.TYPE_IMAGE);
        client.setMode(GridFTPSession.MODE_EBLOCK);
    }

} 


