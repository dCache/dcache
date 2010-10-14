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
package org.globus.gatekeeper.jobmanager;

import org.globus.gatekeeper.jobmanager.internal.Pipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

import org.globus.gram.internal.GRAMConstants;
import org.globus.rsl.RslAttributes;

// FIXME: at any point an exception is throww in request()
// make sure to close the open sockets/resources

public class ForkJobManager extends AbstractJobManager {
    
    private static final String [] FORK_ENV_VARIABLES = { "HOME", 
							  "LOGNAME", 
							  "TZ", 
							  "LANG" };
    private Process _process;
    private PListener _processListener;

    private InputStream stdIn;
    private OutputStream stdOut;
    private OutputStream stdErr;

    private boolean canceled;

    public ForkJobManager() {
	allowStdioUrls = true;
	canceled = false;
    }

    public void cancel() 
	throws JobManagerException {
        if (_jobLogger.isInfoEnabled()) {
            _jobLogger.info("cancel called: " + (_process != null) );
        }
        if (_process != null) {
	    canceled = true;
            _process.destroy();
        }
    }

    public Map getEnvironment() {
	Map map = super.getEnvironment();
        String value;
        for (int i=0;i<FORK_ENV_VARIABLES.length;i++) {
            value = _symbolTable.getProperty(FORK_ENV_VARIABLES[i]);
            if (value != null) {
		map.put(FORK_ENV_VARIABLES[i], value);
            }
        }
	return map;
    }

    public void request(JobRequest request) 
	throws JobManagerException {
	
	RslAttributes rsl = request.getRsl();
	int i;
	String [] cmd = null;

	List list = rsl.getMulti("arguments");
	if (list != null) {
	    cmd = new String [ list.size() + 1 ];
	    cmd[0] = request.getExecutable();
	    Iterator iter = list.iterator();
	    i=1;
	    while( iter.hasNext() ) {
		cmd[i++] = (String)iter.next();
	    }
	} else {
	    cmd = new String [] {request.getExecutable()};
	}

        String file = request.getStdin();
        if (file != null) {
            try {
                stdIn = new FileInputStream(file);
            } catch(IOException e) {
                throw new JobManagerException(JobManagerException.STDIN_NOT_FOUND);
            }
        }
	
	file = rsl.getSingle("output");
	boolean stream = (file != null && file.equalsIgnoreCase("stream"));

	if (_jobLogger.isInfoEnabled()) {
	    _jobLogger.info("Stdio/err streaming: " + stream);
	}

	stdOut = openOut(request.getStdout(), stream,
			JobManagerException.ERROR_OPENING_STDOUT);
	stdErr = openOut(request.getStderr(), stream,
			 JobManagerException.ERROR_OPENING_STDERR);
	
	String [] env = getEnvArray(rsl.getMap("environment"));
	
	if (_jobLogger.isDebugEnabled()) {
	    for (i=0;i<cmd.length;i++) {
		_jobLogger.debug("CMD LINE: " + cmd[i]);
	    }
	    for (i=0;i<env.length;i++) {
		_jobLogger.debug("ENV LINE: " + env[i]);
	    }
	}

	if (request.isDryRun()) {
	    throw new JobManagerException(JobManagerException.DRYRUN);
	}

	try {
	    _process = Runtime.getRuntime().exec(cmd, 
						 env,
						 request.getDirectory());
	} catch(Exception e) {
	    throw new JobManagerException(JobManagerException.ERROR_FORKING_EXECUTABLE, e);
	}

	if (stdIn != null) {
	    redirect(_process.getOutputStream(), stdIn);
	}
	
	if (stdOut != null) {
	    redirect(stdOut, _process.getInputStream());
	}
	
	if (stdErr != null) {
	    redirect(stdErr, _process.getErrorStream());
	}
	
        setStatus(GRAMConstants.STATUS_ACTIVE);

	_processListener = new PListener();
	_processListener.start();
    }

    private OutputStream openOut(String file, boolean stream, int err) 
	throws JobManagerException {
	if (file == null) return null;
	if (_jobLogger.isInfoEnabled()) {
	    _jobLogger.info("Opening stdout/err file: " + file);
	}
        OutputStream out = null;
	if (file.indexOf("://") != -1) {
	    if (stream) {
		out = openUrl(file, err);
	    } else {
		try {
		    out = new FileOutputStream(redirectThruFile(file, err));
		} catch(IOException e) {
		    throw new JobManagerException(err,
						  "Failed to redirect: " + file,
						  e);
		}
	    }
	} else {
	    try {
		out = new FileOutputStream(file, appendStdout);
	    } catch(IOException e) {
		throw new JobManagerException(err,
					      "Failed to open (local): " + file,
					      e);
	    }
	}
	return out;
    }

    private void redirect(OutputStream out, InputStream in) 
	throws JobManagerException {
	Pipe p = new Pipe();
	p.setLogger(_jobLogger);
	p.setInputStream(in);
	p.setOutputStream(out);
	p.start();
    }

    protected void dispose() {
	_jobLogger.info("[fork] Cleaning up...");
	if (stdIn != null) {
	    try { stdIn.close(); } catch(Exception e) {}
	}
	if (stdOut != null) {
	    try { stdOut.close(); } catch(Exception e) {}
	}
	if (stdErr != null) {
	    try { stdErr.close(); } catch(Exception e) {}
	}
	super.dispose();
    }

    /**
     * Is called when the process is finished executing. Handles any necessary
     * waiting or shut-down of server onces all sub-process are done.
     */
    private void processDone(int exitcode) {
        _failureCode = exitcode;
        if (_jobLogger.isInfoEnabled()) {
            _jobLogger.info("Process done, exit code: " + exitcode);
        }
	
	/* currently GRAM ignores job error code */
	
	if (canceled) {
	    setStatus(GRAMConstants.STATUS_FAILED);
	} else {
	    setStatus(GRAMConstants.STATUS_DONE);
	}
    }
    
    class PListener extends Thread {

	/**
	 * waits for the process to finish, then signals the jobmanager.
	 */
	public void run() {
	    try {
		int exitcode = _process.waitFor();
		processDone(exitcode);
	    } catch(Exception e) {
		_jobLogger.error("Unexpected process error", e);
	    }
	}
    }
    
}
