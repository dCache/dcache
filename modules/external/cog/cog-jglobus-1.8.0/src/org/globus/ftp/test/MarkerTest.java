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
import org.globus.ftp.HostPortList;
import org.globus.ftp.RetrieveOptions;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.PerfMarker;
import org.globus.ftp.GridFTPRestartMarker;
import org.globus.ftp.ByteRangeList;
import org.globus.ftp.Marker;
import org.globus.ftp.MarkerListener;
import org.globus.ftp.exception.PerfMarkerException;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.ietf.jgss.GSSCredential;

/**
   Test receiving more E markers.

   This test is only displaying the received markers
   and assuring that no errors occur.
   It cannot be more in-depth because with current server
   there can be no assumption of the frequency, nor content,
   of the received markers.
**/
public class MarkerTest extends TestCase {


    class MarkerListenerImpl implements MarkerListener{

	public ByteRangeList list;
	MarkerTest enclosing;
	
	public MarkerListenerImpl() {
	    list = new ByteRangeList();
	    enclosing = MarkerTest.this;
	}
	
	public void markerArrived(Marker m) {
	    if (m instanceof GridFTPRestartMarker) {
		restartMarkerArrived((GridFTPRestartMarker) m);
	    } else if (m instanceof PerfMarker) {
		perfMarkerArrived((PerfMarker) m);
	    } else {
		enclosing.fail("Received unsupported marker type");
	    }
	};
	
	private void restartMarkerArrived(GridFTPRestartMarker marker) {
	    logger.info("--> restart marker arrived:");
	    list.merge(marker.toVector());
	    logger.info("Current transfer state: " + list.toFtpCmdArgument());
	}
	
	private void perfMarkerArrived(PerfMarker marker) {
	    logger.info("--> perf marker arrived");
	    // time stamp
	    logger.info("Timestamp = " + marker.getTimeStamp());

	    // stripe index
	    if (marker.hasStripeIndex()) {
		try {
		    logger.info("Stripe index =" + marker.getStripeIndex());
		} catch (PerfMarkerException e) {
		    enclosing.fail(e.toString());
		}
	    }else {
		logger.info("Stripe index: not present");
	    }
	    
	    // stripe bytes transferred
	    if (marker.hasStripeBytesTransferred()) {
		try {
		    logger.info("Stripe bytes transferred = "
				 + marker.getStripeBytesTransferred());
		} catch (PerfMarkerException e) {
		    enclosing.fail(e.toString());
		}
	    }else {
		logger.info("Stripe Bytes Transferred: not present");
	    }
	    
	    // total stripe count
	    if (marker.hasTotalStripeCount()) {
		try {
		    logger.info("Total stripe count = " 
				 + marker.getTotalStripeCount());
		} catch (PerfMarkerException e) {
		    enclosing.fail(e.toString());
		}
	    }else {
		logger.info("Total stripe count: not present");
	    }
	}//PerfMarkerArrived   
    }//class MarkerListenerImpl
    

    private static Log logger = 
	LogFactory.getLog(MarkerTest.class.getName());

    public MarkerTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run(suite());	
    }

    public static Test suite ( ) {
	return new TestSuite(MarkerTest.class);
    }
    
    public void testModeEMarkers() 
	   throws Exception {

	MarkerListenerImpl listener = new MarkerListenerImpl();
	
        GridFTPClient source = null;
        GridFTPClient dest = null;
	try {
	    source = new GridFTPClient(TestEnv.serverAHost, 
                                       TestEnv.serverAPort);
	    source.setAuthorization(TestEnv.getAuthorization(TestEnv.serverASubject));
	    String sourceFile = TestEnv.serverADir + "/" + TestEnv.serverALargeFile;

	    dest = new GridFTPClient(TestEnv.serverBHost,
                                     TestEnv.serverBPort);
	    dest.setAuthorization(TestEnv.getAuthorization(TestEnv.serverBSubject));
	    String destFile = TestEnv.serverBDir + "/" + TestEnv.serverBFile;
	    
	    setParams(source, null);
	    setParams(dest, null);

	    source.setOptions(new RetrieveOptions(TestEnv.parallelism));

	    HostPortList hpl = dest.setStripedPassive();
	    source.setStripedActive(hpl);

	    source.extendedTransfer(sourceFile, dest, destFile, listener);
       
	    logger.info("--> most recent byte range list: " 
			+ listener.list.toFtpCmdArgument());

	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	} finally {
            if (source != null) {
                try { 
                    source.close(); 
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            if (dest != null) {
                try { 
                    dest.close(); 
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }
    }
    
    private void setParams(GridFTPClient client, GSSCredential cred)
	throws Exception{
	client.authenticate(cred);
	client.setProtectionBufferSize(16384);
	client.setType(GridFTPSession.TYPE_IMAGE);
	client.setMode(GridFTPSession.MODE_EBLOCK);
    }

} 

