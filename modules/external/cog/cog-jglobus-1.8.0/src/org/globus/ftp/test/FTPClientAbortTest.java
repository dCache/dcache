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

public class FTPClientAbortTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(FTPClientAbortTest.class.getName());

    public FTPClientAbortTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(FTPClientAbortTest.class);
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

    private void test3Party_setParams(FTPClient client,
                      String user,
                      String password)
    throws Exception{
    client.authorize(user, password);
    // secure server: client.setProtectionBufferSize(16384);
    client.setType(Session.TYPE_IMAGE);
    client.setMode(Session.MODE_STREAM);
    }

    /** try third party transfer.
       no exception should be thrown.
    **/
    public void test_putAbort() throws Exception {
	    logger.info("test_putAbort");

    	final FTPClient client1 = new FTPClient(
            TestEnv.serverFHost,TestEnv.serverFPort);
	    FTPClient client2 = new FTPClient(
            TestEnv.serverGHost, TestEnv.serverGPort);

        test3Party_setParams(
            client1, TestEnv.serverFUser, TestEnv.serverFPassword);
        test3Party_setParams(
            client2, TestEnv.serverGUser, TestEnv.serverGPassword);

        (new Thread() {
            public void run() 
            {
                try
                {
                    Thread.sleep(2000);
                    client1.abort();
                }
                catch(Exception ex)
                {
	                logger.info("Unexpected exception " + ex);
                    assertTrue(false);
                }
            }
        }).start();

        try
        {
	        client1.transfer("/dev/zero", client2, "/dev/null", false, null);
            // we want this one to fail
            assertTrue(false);
        }
        catch(org.globus.ftp.exception.ServerException ex)
        {
        }

	    client2.close();
        try
        {
	        client1.close();
        }
        catch(Exception ex2)
        {
        }
    }
    
} 


