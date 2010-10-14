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

import java.util.Vector;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.globus.ftp.GridFTPClient;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.HostPort;
import org.globus.ftp.MlsxEntry;
import org.globus.ftp.Session;

/**
 * Test of MlsxEntry class, MLST and MLSD commands
 */
public class MlsxTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(MlsxTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run(suite());
    }
    
    public static Test suite() {
	return new TestSuite(MlsxTest.class);
    }
    
    public MlsxTest(String name) {
	super(name);
    }

	/*
	        size       -- Size in octets
	        modify     -- Last modification time
	        create     -- Creation time
	        type       -- Entry type
	        unique     -- Unique id of file/directory
	        perm       -- File permissions, whether read, write, execute is
	                      allowed for the login id.
	        lang       -- Language of the file name per IANA[12] registry.
	        media-type -- MIME media-type of file contents per IANA registry.
	        charset    -- Character set per IANA registry (if not UTF-8)
	*/

	public void testMlsxEntry() throws Exception {
		logger.info("test creation of MlsxEntry");
		MlsxEntry entry =
			new MlsxEntry("Type=file;Size=1024990;Perm=r; /tmp/cap60.pl198.tar.gz");
		assertEquals(entry.getFileName(), "/tmp/cap60.pl198.tar.gz");
		assertEquals(entry.get(MlsxEntry.TYPE), MlsxEntry.TYPE_FILE);
		assertEquals(entry.get(MlsxEntry.PERM), "r");
		assertEquals(entry.get(MlsxEntry.SIZE), "1024990");
		assertEquals(entry.get(MlsxEntry.CREATE), null);

		entry = new MlsxEntry("Type=dir;Modify=19981107085215;Perm=el; /tmp");
		assertEquals(entry.getFileName(), "/tmp");
		assertEquals(entry.get(MlsxEntry.TYPE), MlsxEntry.TYPE_DIR);
		assertEquals(entry.get(MlsxEntry.MODIFY), "19981107085215");

		entry = new MlsxEntry("Type=pdir;Modify=19990112030508;Perm=el; ..");
		assertEquals(entry.getFileName(), "..");
		assertEquals(entry.get(MlsxEntry.TYPE), MlsxEntry.TYPE_PDIR);

		entry = new MlsxEntry("Type=pdir;Perm=e;Unique=keVO1+d?3; two words");
		assertEquals(entry.getFileName(), "two words");

		entry =
			new MlsxEntry("Type=file;Perm=r;Unique=keVO1+IH4;  leading space");
		assertEquals(entry.getFileName(), " leading space");

		/*
		other possible tests from the specs:

		Type=OS.unix=slink:/foobar;Perm=;Unique=keVO1+4G4; foobar
		Type=OS.unix=chr-13/29;Perm=;Unique=keVO1+5G4; device
		Type=OS.unix=blk-11/108;Perm=;Unique=keVO1+6G4; block
		Type=file;Perm=awr;Unique=keVO1+8G4; writable
		Type=dir;Perm=cpmel;Unique=keVO1+7G4; promiscuous
		Type=dir;Perm=;Unique=keVO1+1t2; no-exec
		Type=file;Perm=r;Unique=keVO1+EG4; two words
		Type=file;Perm=r;Unique=keVO1+IH4;  leading space
		Type=dir;Perm=cpmdelf;Unique=keVO1+!s2; empty
		Type=cdir;Perm=cpmel;Unique=keVO1+7G4; test/incoming
		type=cdir;unique=AQkAAAAAAAABCAAA; /
		type=dir;unique=AQkAAAAAAAABEAAA; bin
		Type=cdir;Modify=19990219073522; /iana/assignments/media-types
		*/

	}

    public void testMlst() throws Exception {
	logger.info("test MLST");
	GridFTPClient src = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	src.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	src.authenticate(null); // use default creds

	src.setType(Session.TYPE_ASCII);
	src.changeDir(TestEnv.serverADir);

	MlsxEntry entry = src.mlst(TestEnv.serverAFile);
	logger.debug(entry.toString());	

        assertEquals(MlsxEntry.TYPE_FILE, entry.get(MlsxEntry.TYPE));
        assertEquals(String.valueOf(TestEnv.serverAFileSize), 
                     entry.get(MlsxEntry.SIZE));
        assertEquals(TestEnv.serverAFile, entry.getFileName());

	src.close();
    }
	
    public void test3() throws Exception {
	logger.info("show mlsd output using GridFTPClient");
	
	GridFTPClient src = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	src.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	src.authenticate(null); // use default creds
	
	src.setType(Session.TYPE_ASCII);
	src.changeDir(TestEnv.serverADir);
	
	Vector v = src.mlsd();
	logger.debug("mlsd received");
	while (!v.isEmpty()) {
	    MlsxEntry f = (MlsxEntry) v.remove(0);
	    logger.info(f.toString());
	}
	
	src.close();
    }

    public void test4() throws Exception {

	logger.info("get mlsd output using GridFTPClient, EBlock, Image");

	GridFTPClient src = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	src.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	src.authenticate(null); // use default creds

	src.setType(Session.TYPE_IMAGE);
	src.setMode(GridFTPSession.MODE_EBLOCK);

	// server sends the listing over data channel.
	// so in EBlock, it must be active
	HostPort hp = src.setLocalPassive();
	src.setActive(hp);

	src.changeDir(TestEnv.serverADir);

	Vector v = src.mlsd();
	logger.debug("mlsd received");
	while (!v.isEmpty()) {
	    MlsxEntry f = (MlsxEntry) v.remove(0);
	    logger.debug(f.toString());
	}
	
	src.close();
    }
    
    public void test5() throws Exception {
	logger.info(
		    "test two consective mlsd, using both mlsd functions, using GridFTPClient");

	GridFTPClient src = new GridFTPClient(TestEnv.serverAHost, TestEnv.serverAPort);
	src.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	src.authenticate(null); // use default creds

	String output1 = null;
	String output2 = null;
	
	// using mlsd()

	Vector v = src.mlsd(TestEnv.serverADir);
	logger.debug("mlsd received");
	StringBuffer output1Buffer = new StringBuffer();
	while (!v.isEmpty()) {
	    MlsxEntry f = (MlsxEntry) v.remove(0);
	    output1Buffer.append(f.toString()).append("\n");
	    
	}
	output1 = output1Buffer.toString();
	logger.debug(output1);

	// using mlsd 2nd time
	
	HostPort hp2 = src.setPassive();
	src.setLocalActive();

	src.changeDir(TestEnv.serverADir);
	v = src.mlsd();
	logger.debug("mlsd received");
	StringBuffer output2Buffer = new StringBuffer();
	while (!v.isEmpty()) {
	    MlsxEntry f = (MlsxEntry) v.remove(0);
	    output2Buffer.append(f.toString()).append("\n");
	    
	}
	output2 = output2Buffer.toString();
	logger.debug(output2);

	src.close();
    }

}
