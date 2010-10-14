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
package org.globus.gram.tests;

import java.io.File;
import java.io.ByteArrayOutputStream;

import org.globus.gram.GramJob;
import org.globus.gram.Gram;
import org.globus.gram.GramException;
import org.globus.gram.GramJobListener;
import org.globus.gram.WaitingForCommitException;
import org.globus.io.gass.server.GassServer;
import org.globus.util.TestUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class GramTest extends TestCase {

    private static final int TIMEOUT = 1000*60*2;

    private static Log logger = 
	LogFactory.getLog(GramTest.class.getName());

    private static final String CONFIG = 
	"org/globus/gram/tests/test.properties";
    
    private static TestUtil util;

    static {
	try {
	    util = new TestUtil(CONFIG);
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
    }

    public GramTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
	Gram.deactivateAllCallbackHandlers();
    }

    public static Test suite() {
	return new TestSuite(GramTest.class);
    }

    public void testPing() throws Exception {
        Gram.ping(util.get("job.long.contact"));
    }

    public void testActiveJobs() throws Exception {

        GramJob job1 = new GramJob(util.get("job.long"));
	job1.request(util.get("job.long.contact"));

        GramJob job2 = new GramJob(util.get("job.long"));
	job2.request(util.get("job.long.contact"));

        assertEquals(2, Gram.getActiveJobs());

        int i = 0;
	while ( Gram.getActiveJobs() != 0 ) {
            Thread.sleep(2000); 
            i++;
            if (i == 40) {
                fail("getActiveJob() did not reported 0 jobs");
            }
	}
    }

    public void testJobStatusPoll() throws Exception {

	GramJob job = new
	    GramJob(util.get("job.long"));
	
	logger.debug("submitting job in batch mode...");
	job.request(util.get("job.long.contact"), true);
	logger.debug("job submitted: " + job.getIDAsString());

	String status = null;
	do {

	    try {
		Thread.sleep(5000);
	    } catch(Exception e) {}

	    logger.debug("querying status on job...");
	    try {
		Gram.jobStatus(job);
	    } catch(GramException e) {
		if (e.getErrorCode() == GramException.ERROR_CONTACTING_JOB_MANAGER) {
		    if (status == null) {
			fail("Error contacting job manager - could not get job status");
		    } else {
			logger.debug("error contacting job manager - assuming job is finished.");
			break;
		    }
		} else {
		    fail("Failed to get job status: " + e.getMessage());
		}
	    }
	  
	    status = job.getStatusAsString();
	    logger.debug("status: " + status);
	    
	} while (!status.equals("DONE"));
	
    }

    public void testBind() throws Exception {
	
	GramJob job = new
	    GramJob(util.get("job.long"));
	
	logger.debug("submitting job in batch mode...");
	job.request(util.get("job.long.contact"), true);
	logger.debug("job submitted: " + job.getIDAsString());

	DoneStatusListener listener = new DoneStatusListener();

	job.addListener(listener);
	
	job.bind();

	if (!listener.waitFor(TIMEOUT)) {
	    fail("Did not get DONE notification");
	}
    }

    public void testCancel() throws Exception {
	
	GramJob job = new
	    GramJob(util.get("job.long"));

	FailedStatusListener listener = new FailedStatusListener();
	
	job.addListener(listener);
	
	logger.debug("submitting job in interactive mode...");
	job.request(util.get("job.long.contact"));
	logger.debug("job submitted: " + job.getIDAsString());

	Thread.sleep(5000);

	job.cancel();

	if (!listener.waitFor(TIMEOUT)) {
	    fail("Did not get FAILED notification");
	}
    }

    public void testUnbind() throws Exception {
	
	GramJob job = new
	    GramJob(util.get("job.long"));

	ActiveStatusListener listener = new ActiveStatusListener();
	
	job.addListener(listener);
	
	logger.debug("submitting job in interactive mode...");
	job.request(util.get("job.long.contact"));
	logger.debug("job submitted: " + job.getIDAsString());
	
	if (!listener.waitFor(TIMEOUT)) {
	    fail("Did not get ACTIVE notification");
	}

	job.unbind();
	listener.reset();
	
	Thread.sleep(2000);

	job.cancel();

	if (listener.getNotified()) {
	    fail("Unconnected listener received unexpected notification.");
	}
    }

    public void testBadParameter() throws Exception {
	GramJob job = new GramJob("&(argument=12)");
	try {
	    job.request(util.get("job.long.contact"));
	} catch (GramException e) {
	    if (e.getErrorCode() != GramException.PARAMETER_NOT_SUPPORTED) {
		e.printStackTrace();
		fail("Unexpected error returned: " + e.getMessage());
	    }
	}
    }

    public void testBadExecutable() throws Exception {
	GramJob job = new GramJob("&(executable=/bin/thisexecdoesnotexist)");

	FailedStatusListener listener = new FailedStatusListener();
	job.addListener(listener);

	try {
	    job.request(util.get("job.long.contact"));
	} catch (GramException e) {
	    if (e.getErrorCode() != GramException.EXECUTABLE_NOT_FOUND) {
		e.printStackTrace();
		fail("Unexpected error returned: " + e.getMessage());
	    }
	    logger.debug("Error returned on request()");
	    return;
	}
	
	if (!listener.waitFor(TIMEOUT)) {
	    fail("Did not get FAILED notification");
	}
	
	if (job.getError() != GramException.EXECUTABLE_NOT_FOUND) {
	    fail("Unexpected error returned: " + job.getError());
	}
    }

    public void testRedirect() throws Exception {
	
	DoneStatusListener listener = new DoneStatusListener();

	GassServer server = null;
	try {
	    server = new GassServer();
	    String url = server.getURL();

	    ByteArrayOutputStream stdout = 
		new ByteArrayOutputStream();

	    StringBuffer rsl = new StringBuffer();
	    rsl.append("&(executable=")
		.append(util.get("stdin.exe"))
		.append(")");
	    rsl.append("(rsl_substitution=(GLOBUSRUN_GASS_URL ")
		.append(url)
		.append("))");
	    rsl.append("(stdin=$(GLOBUSRUN_GASS_URL)/")
		.append(util.get("stdin.file"))
		.append(")");
	    rsl.append("(stdout=$(GLOBUSRUN_GASS_URL)/dev/stdout-rgs)");

	    server.registerJobOutputStream("out-rgs", stdout);

	    System.out.println(rsl);

	    GramJob job = new GramJob(rsl.toString());
	    job.addListener(listener);
	    job.request(util.get("job.long.contact"));

	    if (!listener.waitFor(TIMEOUT)) {
		fail("Did not get DONE notification");
	    }

	    File f = new File(util.get("stdin.file"));
	    byte[] stdoutData = stdout.toByteArray();

	    assertEquals("stdout size", f.length(), stdoutData.length);

	} finally {
	    if (server != null) {
		server.shutdown();
	    }
	}
    }

    class DoneStatusListener implements GramJobListener {
	boolean notified = false;
	public synchronized void statusChanged(GramJob job) {
	    if (job.getStatus() == GramJob.STATUS_DONE) {
		notified = true;
		notify();
	    }
	}
	public synchronized boolean waitFor(int timeout) 
	    throws Exception {
	    wait(timeout);
	    return notified;
	}
    }

    class FailedStatusListener implements GramJobListener {
	boolean notified = false;
	public synchronized void statusChanged(GramJob job) {
	    if (job.getStatus() == GramJob.STATUS_FAILED) {
		notified = true;
		notify();
	    }
	}
	public synchronized boolean waitFor(int timeout) 
	    throws Exception {
	    wait(timeout);
	    return notified;
	}
    }

    class ActiveStatusListener implements GramJobListener {
	boolean notified = false;
	public synchronized void statusChanged(GramJob job) {
	    if (job.getStatus() == GramJob.STATUS_ACTIVE) {
		notified = true;
		notify();
	    }
	}
	public synchronized boolean waitFor(int timeout) 
	    throws Exception {
	    wait(timeout);
	    return notified;
	}
	public void reset() {
	    notified = false;
	}
	public boolean getNotified() {
	    return notified;
	}
    }
    
    // These are 1.5 gram tests
    
    public void testTwoPhaseSignalCancel() throws Exception {
        twoPhaseSubmit(false);
    }

    public void testTwoPhaseCancel() throws Exception {
        twoPhaseSubmit(true);
    }

    private void twoPhaseSubmit(boolean cancelCall) throws Exception {
	
        GramJob job = new GramJob(util.get("job.long") + "(twoPhase=yes)");

        try {
            job.request(util.get("job.long.contact"));

            fail("Did not throw expected exception");
	} catch(WaitingForCommitException e) {
	    logger.debug("Two phase commit: sending COMMIT_REQUEST signal");

            job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
	}
	
        logger.debug("job submited: " + job.getIDAsString());

        Thread.sleep(5000);

	// this is little weird... cancel() and signal_cancel() should
        // behave in the same exact way but they do not

        if (cancelCall) {
            logger.debug("Canceling job... (cancel call)");

            job.cancel();

            // XXX: this should be common to both ways
            logger.debug("Two phase commit: sending COMMIT_END signal");
            
            job.signal(GramJob.SIGNAL_COMMIT_END);
        } else {
            logger.debug("Canceling job... (cancel signal)");

            job.signal(GramJob.SIGNAL_CANCEL, " ");
	}
    }
    
    public void testTwoPhaseExtend() throws Exception {
        
        GramJob job = new GramJob(util.get("job.long") + "(twoPhase=yes)");
        
        try {
            job.request(util.get("job.long.contact"));
	} catch(WaitingForCommitException e) {
            logger.debug("Two phase commit: sending COMMIT_EXTEND signal");
            
            job.signal(GramJob.SIGNAL_COMMIT_EXTEND, "30");
	}
	
        logger.debug("job submited: " + job.getIDAsString());
        
        Thread.sleep(75000);
        
	if (job.getStatus() == job.STATUS_FAILED) {
	    fail("Timeout expired!");
	}
    }

}
