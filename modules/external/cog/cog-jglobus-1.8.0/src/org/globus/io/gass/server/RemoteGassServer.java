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
package org.globus.io.gass.server;

import org.globus.gram.GramJob;
import org.globus.gram.GramJobListener;
import org.globus.util.GlobusURL;
import org.globus.io.gass.client.GassException;
import org.globus.util.deactivator.Deactivator;

import org.ietf.jgss.GSSCredential;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class allows for starting gass server remotely. The gass
 * server is started via the globus gatekeeper.
 *
 */
public class RemoteGassServer {

    private static Log logger =
        LogFactory.getLog(RemoteGassServer.class.getName());

    public static final int LINE_BUFFER_ENABLE  = 256;
    public static final int TILDE_EXPAND_ENABLE = 512;
    public static final int USER_EXPAND_ENABLE  = 1024;
    
    private int port           = 0;
    private int options        = 0;
    private boolean secure     = true;
    private GSSCredential cred = null;

    private String url            = null;
    private GramJob job           = null;
  
    private boolean compatibilityMode = false;

    private OutputListener stderrListener, stdoutListener;
    private GassServerListener gassJobListener;

    /**
     * Starts Gass Server with default user credentials.
     * Port of the server will be dynamically assigned.
     */
    public RemoteGassServer() {
	this(true, 0);
    }
  
    /**
     * Starts Gass Server on given port and mode. Default
     * credentials will be used to start the server.
     *
     * @param secure
     *        if true starts server in secure mode, otherwise unsecure.
     * @param port
     *        port of the server, if 0 it will be dynamically assigned.
     */
    public RemoteGassServer(boolean secure, int port) {
	this(null, secure, port);
    }

    /**
     * Starts Gass Server on given port and mode. The supplied
     * credentials will be used to start the server.
     *
     * @param cred
     *        credentials to use to start the server.
     * @param secure
     *        if true starts server in secure mode, otherwise unsecure.
     * @param port
     *        port of the server, if 0 it will be dynamically assigned.
     */
    public RemoteGassServer(GSSCredential cred, boolean secure, int port) {
	this.cred = cred;
	this.secure = secure;
	this.port = port;
    
	options = USER_EXPAND_ENABLE |
	    TILDE_EXPAND_ENABLE |
	    LINE_BUFFER_ENABLE |
	    GassServer.READ_ENABLE |
	    GassServer.WRITE_ENABLE;
    }
  
    /**
     * Returns url of this server.
     *
     * @return url of this server
     */
    public String getURL() {
	return url;
    }

    /**
     * Sets the options of the gass server such
     * as enabling client shutdown, etc.
     *
     * @param options server options
     */
    public void setOptions(int options) {
	this.options = options;
    }

    /**
     * Returns current options of the server.
     *
     * @return options of the server. O if not
     *         none set.
     */
    public int getOptions() {
	return options;
    }

    /**
     * Sets the compatibility mode to work with the old
     * globus 1.1.x installations.
     *
     * @param compatibility set to true if working with
     *                      the old globus 1.1.x installation.
     *
     */
    public void setCompatibilityMode(boolean compatibility) {
	this.compatibilityMode = compatibility;
    }

    /**
     * Starts the gass server on the remote machine.
     *
     * @param     rmc resource manager contact of the remote machine.
     * @exception GassException if any error occurs during 
     *            remote startup.
     */
    public void start(String rmc) 
	throws GassException {
	
	if (rmc == null) {
	    throw new IllegalArgumentException("Resource manager contact not specified");
	}

	GassServer gassServer = null;
	String error          = null;

	 try {
	     
	     gassServer     = new GassServer(this.cred, 0);
	     String gassURL = gassServer.getURL();
	     String rsl     = getRSL(gassURL);
	   
	     logger.debug("RSL: " + rsl);
	   
	     stderrListener = new OutputListener();
	     stdoutListener = new OutputListener();
	   
	     gassServer.registerJobOutputStream("err-rgs", new JobOutputStream(stderrListener));
	     gassServer.registerJobOutputStream("out-rgs", new JobOutputStream(stdoutListener));
	     
	     job = new GramJob(this.cred, rsl);
	     
	     gassJobListener = new GassServerListener();
	     
	     job.addListener(gassJobListener);
	     job.request(rmc);

	     int status = gassJobListener.waitFor(1000*60*2);

	     if (status == GramJob.STATUS_ACTIVE) {
		 
		 while(true) {
		     if (stderrListener.hasData()) {
			 // got some error
			 error = stderrListener.getOutput();
			 break;
		     } else if (stdoutListener.hasData()) {
			 // this could be the url
			 String fl = stdoutListener.getOutput();
			 if (fl.startsWith("https://") ||
			     fl.startsWith("http://")) {
			     // extract url
			     url = fl.trim();
			     break;
			 } else {
			     // something is wrong with stdout
			     error = "Unable to extract gass url : " + fl;
			     break;
			 }
		     } else {
			 // wait for stdout/err
			 logger.debug("waiting for stdout/err");
			 sleep(500);
		     }
		 }

	     } else if (status == GramJob.STATUS_FAILED ||
			status == GramJob.STATUS_DONE) {
		 int errorCode = gassJobListener.getError();
		 if (stderrListener.hasData()) {
		     error = stderrListener.getOutput();
		 } else if (errorCode != 0) {
		     error = "Remote gass server stopped with error : " + 
			 errorCode;
		 } else {
		     error = "Remote gass server stopped and returned no error";
		 }
	     } else {
		 error = "Unexpected state or received no notification";
	     }
	     
	 } catch(Exception e) {
	     throw new GassException( e.getMessage() );
	 } finally {
	     if (gassServer != null) {
		 gassServer.shutdown();
	     }
	 }
	 
	 if (error != null) {
	     throw new GassException(error);
	 }
    }

    /**
     * Shutdowns remotely running gass server.
     *
     * @return true if server was successfully killed, false
     *         otherwise.
     */
    public boolean shutdown() {
	
	if (url != null) {
	    logger.debug("Trying to shutdown gass server directly...");
	    try {
		GlobusURL u = new GlobusURL(url);
		GassServer.shutdown(this.cred, u);
	    } catch(Exception e) {
		logger.debug("gass server shutdown failed", e);
	    }

	    try {
		gassJobListener.reset();
		int status = gassJobListener.waitFor(1000*60);
		if (status == GramJob.STATUS_FAILED ||
		    status == GramJob.STATUS_DONE) {
		    // shutdown successful
		    reset();
		    return true;
		}
	    } catch(InterruptedException e) {
		logger.debug("", e);
	    }
	}
	
	// otherwise just cancel the job.
    
	if (job == null) return true;
	
	logger.debug("Canceling gass server job.");

	try {
	    job.cancel();
	    reset();
	    return true;
	} catch(Exception e) {
	    return false;
	}
	
    }
  
    private void reset() {
	job = null;
	url = null;
    }
    
    private String getRSL(String gassURL) {
	StringBuffer buf = new StringBuffer();
	
	if (compatibilityMode) {
	    buf.append("&(executable=$(GLOBUS_TOOLS_PREFIX)/bin/globus-gass-server)");
	} else {
	    buf.append("&(executable=$(GLOBUS_LOCATION)/bin/globus-gass-server)");
	    buf.append("(environment=(LD_LIBRARY_PATH $(GLOBUS_LOCATION)/lib))");
	}
	
	buf.append("(rsl_substitution=(GLOBUSRUN_GASS_URL " + gassURL + "))");
	buf.append("(stderr=$(GLOBUSRUN_GASS_URL)/dev/stderr-rgs)");
	buf.append("(stdout=$(GLOBUSRUN_GASS_URL)/dev/stdout-rgs)");
	
	setRSLArguments(buf);
	
	return buf.toString();
    }
    
    private void setRSLArguments(StringBuffer buf) {
	buf.append("(arguments=\"-c\"");
	
	if (port != 0) {
	    buf.append(" \"-p\" \"" + port + "\"");
	}
	
	if (!secure) {
	    buf.append(" \"-i\"");
	}
	
	if ((options & LINE_BUFFER_ENABLE) != 0) {
	    buf.append(" \"-l\"");
	}
	
	if ((options & TILDE_EXPAND_ENABLE) != 0) {
	    buf.append(" \"-t\"");
	}
	
	if ((options & USER_EXPAND_ENABLE) != 0) {
	    buf.append(" \"-u\"");
	}
	
	if ((options & GassServer.READ_ENABLE) != 0) {
	    buf.append(" \"-r\"");
	}
	
	if ((options & GassServer.WRITE_ENABLE) != 0) {
	    buf.append(" \"-w\"");
	}
	
	buf.append(")");
    }
    
    private void sleep(int msec) {
	try {
	    Thread.sleep(msec);
	} catch(Exception e) {
	}
    }
    
    
    // ---------- main ----------------
    
    public static void main(String [] args) {

	RemoteGassServer s = null;

	int port       = 0;
	boolean secure = true;
	String host    = null;
	
	for (int i = 0; i < args.length; i++) {
	    if (args[i].equals("-h")) {
		host = args[++i];
	    } else if (args[i].equals("-p")) {
		port = Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-i")) {
		secure = false;
	    } else {
		System.err.println("Unknown command: " + args[i]);
		System.exit(1);
	    }
	}
	
	try {
	    s = new RemoteGassServer(secure, port);
	    
	    s.setOptions( USER_EXPAND_ENABLE | 
			  TILDE_EXPAND_ENABLE | 
			  LINE_BUFFER_ENABLE | 
			  GassServer.READ_ENABLE | 
			  GassServer.WRITE_ENABLE );
	    
	    s.start(host);
	    
	    System.out.println("Remote gass server url: " + s.getURL());
	    
	    Thread.sleep(10000);
	    
	    System.out.println("Shutting down...");
	    
	} catch(Exception e) {
	    e.printStackTrace();
	} finally {
	    if (s != null) s.shutdown();
	}
	
	System.out.println("Done");
	Deactivator.deactivateAll();
    }
    
}

class GassServerListener implements GramJobListener {
    
    private static Log logger =
        LogFactory.getLog(RemoteGassServer.class.getName());

    private int status = -1;
    private int error = 0;
    
    public int getError() {
	return this.error;
    }
    
    public static boolean isStartState(int status) {
	return (status == GramJob.STATUS_ACTIVE ||
		status == GramJob.STATUS_FAILED ||
		status == GramJob.STATUS_DONE);
    }
    
    public synchronized void reset() {
	this.error = 0;
	this.status = -1;
    }
    
    public synchronized int waitFor(int timeout) 
	throws InterruptedException {
	for(;;) {
	    if (isStartState(status)) {
		break;
	    }
	    wait(timeout);
	    if (status == -1) {
		break;
	    }
	}
	return status;
    }
    
    public synchronized void statusChanged(GramJob job) {
	int st = job.getStatus();
	logger.debug("Gass job status: " + st);
	if (status == -1 && isStartState(st)) {
	    status = st;
	    error = job.getError();
	    notify();
	}
    }
}

class OutputListener implements JobOutputListener {
    
    private StringBuffer outputBuf = null;
    
    public void outputChanged(String output) {
	if (outputBuf == null) {
	    outputBuf = new StringBuffer();
	}
	outputBuf.append(output);
    }
  
    public void outputClosed() {
    }
  
    public String getOutput() {
	return (outputBuf == null) ? null : outputBuf.toString();
    }
  
    public boolean hasData() {
	return (outputBuf != null);
    }
    
}
