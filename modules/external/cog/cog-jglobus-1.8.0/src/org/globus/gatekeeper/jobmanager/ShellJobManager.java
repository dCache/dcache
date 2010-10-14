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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

import org.globus.gram.internal.GRAMConstants;
import org.globus.rsl.RslAttributes;

// TODO: validate first if can open and execute the different scripts
// TODO: each time an JobManagerException is thrown job state is set to FAILED?

// FIXME: how does cancel relate to killing an update thread.
// FIXME: stderr of the script is not catched!

public class ShellJobManager extends AbstractJobManager {
    
    private static final String SCRIPT_JOB_ID  = "GRAM_SCRIPT_JOB_ID:";
    private static final String SCRIPT_SUCCESS = "GRAM_SCRIPT_SUCCESS:";
    private static final String SCRIPT_ERROR   = "GRAM_SCRIPT_ERROR:";

    private static final int STATUS_POLL_FREQUENCY = 5 * 1000; // five seconds

    private StatusThread _statusThread = null;

    private String _type;
    private String _libExecDir;

    // to execute the scripts in
    private File _directory;

    // this file will be generated
    private File argFile = null;
    
    public ShellJobManager() {
	allowStdioUrls = false;
    }

    public void setLibExecDirectory(String libExecDirectory) {
	_libExecDir = libExecDirectory;
    }

    public void setType(String type) {
	_type = type;
    }
    
    private void runScript(String scriptType) 
	throws JobManagerException {
	runScript(scriptType, null);
    }
    
    private void runScript(String scriptType, String arg)
	throws JobManagerException {
	if (_type == null) {
	    throw new JobManagerException(JobManagerException.ERROR_OPENING_JOBMANAGER_SCRIPT, 
					  "JobManager type was not set");
	}
	if (_libExecDir == null) {
	    throw new JobManagerException(JobManagerException.ERROR_OPENING_JOBMANAGER_SCRIPT,
					  "JobManager libexec directory was not set");
	}
	if (scriptType == null) {
	    throw new JobManagerException(JobManagerException.ERROR_OPENING_JOBMANAGER_SCRIPT,
                                          "JobManager script name was not set");
	}
	String [] cmd = new String[ (arg == null) ? 2 : 3 ];
	cmd[0] = _libExecDir + "/globus-script-" + _type + "-" + scriptType;
	cmd[1] = argFile.getAbsolutePath();
	if (arg != null) {
	    cmd[2] = arg;
	}
	runScript(cmd);
    }
    
    private void runScript(String [] cmd) 
	throws JobManagerException {
	String line = null;
	try {
	    Process process = Runtime.getRuntime().exec(cmd,
							getEnvArray(),
							_directory);
	    InputStream out = process.getInputStream();
	    BufferedReader reader = new BufferedReader(new InputStreamReader(out));
	    while( (line = reader.readLine()) != null ) {
		if (_jobLogger.isDebugEnabled()) {
		    _jobLogger.debug("SCRIPT OUTPUT: " + line);
		}
		if (line.startsWith(SCRIPT_JOB_ID)) {
		    // ignored for now
		} else if (line.startsWith(SCRIPT_SUCCESS)) { // status code
		    String value = line.substring(SCRIPT_SUCCESS.length()).trim();
		    try {
			setStatus(Integer.parseInt(value));
		    } catch(Exception e) {
			throw new JobManagerException(JobManagerException.INVALID_SCRIPT_STATUS, e);
		    }
		    return;
		} else if (line.startsWith(SCRIPT_ERROR)) { // error code
		    String value = line.substring(SCRIPT_ERROR.length()).trim();
		    int fc = 0;
		    try {
			fc = Integer.parseInt(value);
                    } catch(Exception e) {
			throw new JobManagerException(JobManagerException.INVALID_SCRIPT_STATUS, e);
		    }
		    throw new JobManagerException(fc);
		}
	    }
	    throw new JobManagerException(JobManagerException.INVALID_SCRIPT_REPLY);
	} catch(Exception e) {
	    throw new JobManagerException(JobManagerException.ERROR_OPENING_JOBMANAGER_SCRIPT, e);
	}
    }

    /**
     * Cancels a job.
     */
    public void cancel() 
	throws JobManagerException {
	
	runScript("rm");
	
	// status might be set twice but it should be sent once
	setStatus(GRAMConstants.STATUS_FAILED);
	
	// setting status to FAILED will
	// delete the argument file and stop
	// the status thread.
    }

    public void signal(int signal, String args)
        throws JobManagerException {
	
	PrintWriter writer = null;
	File signalFile = null;
	
        try {
            signalFile = File.createTempFile("grami_signal", ".tmp");
            fileList.add(signalFile);
            writer = new PrintWriter(new FileOutputStream(signalFile));
        } catch(IOException e) {
            throw new JobManagerException(JobManagerException.ARG_FILE_CREATION_FAILED);
        }
	
	writer.println( write("grami_signal_arg", args) );
	writer.println( write("grami_signal", signal) );
        writer.close();
	
        if (writer.checkError()) {
            throw new JobManagerException(JobManagerException.ARG_FILE_CREATION_FAILED);
        }

	try {
	    runScript("signal", signalFile.getAbsolutePath());
	} finally {
	    if (signal == GRAMConstants.SIGNAL_CANCEL) {
		setStatus(GRAMConstants.STATUS_FAILED);
	    }
	    signalFile.delete();
	}
    }
    
    public void request(JobRequest request) 
	throws JobManagerException {

	PrintWriter writer = null;

	try {
	    argFile = File.createTempFile("grami", ".tmp");
	    fileList.add(argFile);
	    writer = new PrintWriter(new FileOutputStream(argFile));
	} catch(IOException e) {
	    throw new JobManagerException(JobManagerException.ARG_FILE_CREATION_FAILED);
	}

	writeRequest(writer, request);
	writer.close();
	if (writer.checkError()) {
	    throw new JobManagerException(JobManagerException.ARG_FILE_CREATION_FAILED);
	}

        if (request.isDryRun()) {
            throw new JobManagerException(JobManagerException.DRYRUN);
        }
	
	_directory = request.getDirectory();

	// this will set the state
	runScript("submit");

	// start the update thread only when successful
	
	if (_status == GRAMConstants.STATUS_ACTIVE ||
	    _status == GRAMConstants.STATUS_PENDING) {
	    _statusThread = new StatusThread();
	    _statusThread.start();
	}
    }

    private void writeRequest(PrintWriter writer, JobRequest request) {
	RslAttributes rsl = request.getRsl();

	writer.println( write("grami_logfile", rsl.getSingle("scriptLogFile") ) );
	writer.println( write("grami_directory", request.getDirectory().getAbsolutePath() ) );
	writer.println( write("grami_program", request.getExecutable() ) );
	writer.println( write("grami_args", rsl.getMulti("arguments") ) );
	writer.println( write("grami_env", rsl.getMap("environment") ) );
	writer.println( write("grami_count", request.getCount() ) );

	writer.println( write("grami_stdin", request.getStdin() ) );
	writer.println( write("grami_stdout", request.getStdout(), "/dev/null" ) );
	writer.println( write("grami_stderr", request.getStderr(), "/dev/null" ) );

	writer.println( write("grami_max_wall_time", request.getMaxWallTime() ) );
	writer.println( write("grami_max_cpu_time", request.getMaxCpuTime() ) );

	// start time not implemented
	writer.println( write("grami_start_time", (String)null) );

	writer.println( write("grami_min_memory", request.getMinMemory() ) );
	writer.println( write("grami_max_memory", request.getMaxMemory() ) );
	writer.println( write("grami_host_count", request.getHostCount() ) );
	writer.println( write("grami_job_type", request.getJobType() ) );

	// FIXME: not implemented yet

	writer.println( write("grami_queue", (String)null ) );
	writer.println( write("grami_project", (String)null) );
	writer.println( write("grami_reservation_handle", (String)null) );
	
	// FIXME: condord stuff is missing
    }
    
    private String write(String attrib, int value) {
	return write(attrib, String.valueOf(value), null);
    }

    private String write(String attrib, long value) {
        return write(attrib, String.valueOf(value), null);
    }
    
    private String write(String attrib, String value) {
	return write(attrib, value, null);
    }

    private String write(String attrib, String value, String defValue) {
	StringBuffer buf = new StringBuffer(attrib);
	buf.append("='");
	value = (value == null) ? defValue : value;
	if (value != null) {
	    buf.append(encode(value));
	}
	buf.append("'");
	return buf.toString();
    }
    
    private String write(String attrib, List values) {
	StringBuffer buf = new StringBuffer(attrib);
        buf.append("='");
	if (values != null) {
	    Iterator iter = values.iterator();
	    while(iter.hasNext()) {
		buf.append("\"");
		buf.append( encode(((String)iter.next())) );
		buf.append("\" ");
	    }
	}
        buf.append("'");
        return buf.toString();
    }

    private String write(String attrib, Map values) {
        StringBuffer buf = new StringBuffer(attrib);
        buf.append("='");
        if (values != null) {
	    Iterator iter = values.keySet().iterator();
	    String name;
            while(iter.hasNext()) {
		name = (String)iter.next();
		buf.append("\"");
		buf.append(name);
		buf.append("\" \"");
		buf.append(values.get(name));
		buf.append("\" ");
            }
        }
        buf.append("'");
        return buf.toString();
    }

    private StringBuffer encode(String value) {
	int size = value.length();
	StringBuffer buf = new StringBuffer(size);
	char ch;
	for (int i=0;i<size;i++) {
	    ch = value.charAt(i);
	    if (ch == '"') {
		buf.append("\\\"");
	    } else if (ch == '$') {
		buf.append("'\"\\\\$\"'");
	    } else if (ch == '\\') {
		buf.append("\\\\\\\\");
	    } else if (ch == '\'') {
		buf.append("'\"'\"'");
	    } else {
		buf.append(ch);
	    }
	}
	return buf;
    }

    public void dispose() {
	_jobLogger.info("[shell] Cleaning up...");
	if (_statusThread != null) {
            _statusThread.stop();
	}
	super.dispose();
	_statusThread = null;
    }

    class StatusThread implements Runnable {

	private boolean _stop = false;
	private int _frequency = STATUS_POLL_FREQUENCY;
	
	public void start() {
	    Thread d = new Thread(this);
	    d.start();
	}
	
	public void stop() {
	    _stop = true;
	}
	
	public void run() {
	    _jobLogger.debug("[Shell status thread] running");
	    
	    _stop = false;
	    try {
		while(!_stop) {
		    try { Thread.sleep(_frequency); } catch(Exception e) {}
		    if (_stop) break;
		    runScript("poll");
		}
	    } catch(JobManagerException e) {
		// FIXME: don't know what to do 
		// in case of an error
		// e.printStackTrace();
	    }
	    
	    _jobLogger.debug("[Shell status thread] done");
	}
	
    }
    
}
