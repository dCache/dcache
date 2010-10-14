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

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Hashtable;
import java.util.Iterator;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;

import org.globus.util.GlobusURL;
import org.globus.util.Util;
import org.globus.gram.internal.GRAMConstants;
import org.globus.rsl.RSLParser;
import org.globus.rsl.RslNode;
import org.globus.rsl.RslEvaluationException;
import org.globus.rsl.RslAttributes;
import org.globus.gsi.gssapi.auth.SelfAuthorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.io.urlcopy.UrlCopyException;
import org.globus.io.streams.HTTPOutputStream;
import org.globus.io.streams.GassOutputStream;
import org.globus.io.streams.FTPOutputStream;
import org.globus.io.streams.GridFTPOutputStream;
import org.globus.util.Tail;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.FileAppender;

import org.gridforum.jgss.ExtendedGSSCredential;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import org.apache.commons.logging.impl.Log4JLogger;


// FIXME: does not handle file:// urls

/**
 * AbstractJobManager is a base class from which all specific job managers 
 * should inherit from. It provides all the basic functionality required
 * for a job manager.
 */
public abstract class AbstractJobManager implements JobManager {

    private static final String LOG_PATTERN = "%-5p: %m%n";
    
    private static final String [] ENV_VARIABLES = { "X509_CERT_DIR",
						     "X509_USER_PROXY",
						     "GLOBUS_GRAM_JOB_CONTACT",
						     "GLOBUS_DEPLOY_PATH",
						     "GLOBUS_INSTALL_PATH" };
    
    // will open stdout files in append mode - append should be assumed
    // becuase the external scripts do that
    protected boolean appendStdout = true;
    
    protected List fileList;
    protected GSSCredential _credential = null;
    protected int _status = 0;
    protected int _failureCode = 0;
    protected Hashtable _callbackUrl;
    protected boolean allowStdioUrls = false;
    protected Tail _outputFollower;
    protected String _id;
    protected Properties _symbolTable;
    
    /** called only when job is done or failed and
     * all after other listeners were notified of the state */
    protected JobDoneListener _jobDoneListener; 
    
    protected Logger _jobLogger;
    
    public AbstractJobManager() {
	_id = String.valueOf(hashCode());
	
	_symbolTable = new Properties();
	_callbackUrl = new Hashtable(); // FIXME: lazy instatiate it
	fileList = new LinkedList();

	initSymbolTable();
	initJobLogger();
    }

    protected void initJobLogger() {
	_jobLogger = Logger.getLogger( getClass().getName() + "." + _id );
    }

    public void setLogFile() {
	File f = new File( System.getProperty("user.home"),
			   "jgram_job_mgr_" + _id + ".log");
	setLogFile(f.getAbsolutePath());
    }

    public void setLogFile(String file) {
        FileAppender ap = new FileAppender();
        ap.setFile(file);
	
        ap.setName("JobManager Log");
        ap.setLayout(new PatternLayout(LOG_PATTERN));
        ap.activateOptions();
	
        _jobLogger.addAppender(ap);
    }

    public void setLogger(Logger logger) {
	_jobLogger = logger;
    }
    
    public void setCredentials(GSSCredential credentials) {
	_credential = credentials;
    }

    public GSSCredential getCredentials() {
	return _credential;
    }

    public String getID() {
	return _id;
    }

    public void setID(String id) {
	_id = id;
    }

    protected void initSymbolTable() {
	_symbolTable.put("HOME",
			 System.getProperty("user.home"));
        _symbolTable.put("LOGNAME", 
			 System.getProperty("user.name"));
	_symbolTable.put("GLOBUS_HOST_OSNAME", 
			 System.getProperty("os.name"));
	_symbolTable.put("GLOBUS_HOST_OSVERSION",
			 System.getProperty("os.version"));
	
	setGlobusProperties(_symbolTable);
    }
    
    public static void setGlobusProperties(Map map) {
	String key = null;
	Properties props = System.getProperties();
	Enumeration e = props.keys();
	while(e.hasMoreElements()) {
	    key = (String)e.nextElement();
	    if (key.regionMatches(true, 0, "GLOBUS", 0, 6)) {
		map.put(key, props.getProperty(key));
	    }
	}
    }
    
    public Properties getSymbolTable() {
	return _symbolTable;
    }

    public Map getEnvironment() {
	Map map = new HashMap();
	String value;
	for (int i=0;i<ENV_VARIABLES.length;i++) {
	    value = _symbolTable.getProperty(ENV_VARIABLES[i]);
	    if (value != null) {
		map.put(ENV_VARIABLES[i], value);
	    }
	}
	return map;
    }
    
    public String[] getEnvArray() {
	return getEnvArray(null);
    }
    
    public String[] getEnvArray(Map map) {

	String name = null;
        Iterator iter = null;

	Map env = getEnvironment();

	if (map != null) {
            iter = map.keySet().iterator();
            while(iter.hasNext()) {
                name = (String)iter.next();
		// merge passed env with internal env
		// and do not overwrite the variables
		if (env.get(name) == null) {
		    env.put(name, map.get(name));
		}
            }
	}
	
	int i = 0;
	String [] envArray = new String[env.size()];
	iter = env.keySet().iterator();
	while(iter.hasNext()) {
	    name = (String)iter.next();
	    envArray[i++] = name + "=" + env.get(name);
	}
	
	return envArray;
    }
    
    // this is GSI specific - will not work with Kerberos
    protected void saveDelegatedCredentials() 
	throws JobManagerException {

	if (_credential == null) {
	    throw new JobManagerException(JobManagerException.USER_PROXY_NOT_FOUND);
	}
	
	if (!(_credential instanceof ExtendedGSSCredential)) {
	    throw new JobManagerException(JobManagerException.ERROR_OPENING_CACHE_USER_PROXY);
	}

	File proxyFile = null;

        try {
            proxyFile = File.createTempFile("x509up_", ".tmp");
        } catch(IOException e) {
            throw new JobManagerException(JobManagerException.ERROR_OPENING_CACHE_USER_PROXY);
        }
	
	fileList.add(proxyFile);

	FileOutputStream out = null;
	
	try {
	    out = new FileOutputStream(proxyFile);
	    // set read only permissions
	    Util.setOwnerAccessOnly(proxyFile.getAbsolutePath());
	    // write the contents
	    byte [] data = 
		((ExtendedGSSCredential)_credential).export(ExtendedGSSCredential.IMPEXP_OPAQUE);
	    out.write(data);
	} catch (Exception e) {
	    throw new JobManagerException(JobManagerException.ERROR_OPENING_CACHE_USER_PROXY, e);
	} finally {
	    if (out != null) {
		try { out.close(); } catch (Exception e) {}
	    }
	}
	
	// FIXME: this enables to rsl evaluate again user proxy location
	_symbolTable.put("X509_USER_PROXY", proxyFile.getAbsolutePath());
    }

    public void request(String rslRequest) 
	throws JobManagerException {
	
	/*
	 * In case of an error the dispose()
	 * function is called automatically 
	 */
	
	if (_jobLogger.isInfoEnabled()) {
	    _jobLogger.info("Job request: " + rslRequest);
	}
	
	RslNode rslTree = null;
	
	try {
            rslTree = RSLParser.parse( Util.unquote(rslRequest) );
	} catch(Exception e) {
	    throw new JobManagerException(JobManagerException.BAD_RSL, e);
        }

	// init req
        JobRequest jobReq = new JobRequest();
	
	try {
	    rslTree = (RslNode)rslTree.evaluate(getSymbolTable());
	} catch (RslEvaluationException e) {
	    throw new JobManagerException(JobManagerException.RSL_EVALUATION_FAILED, e);
	}
	
	String tmpArg = null;
	
        RslAttributes rsl = new RslAttributes(rslTree);

	// custom mod to add dynamic debugging
        tmpArg = rsl.getSingle("debug");
        if (tmpArg != null && tmpArg.equalsIgnoreCase("yes")) {
            setLogFile();
        }

        if (_jobLogger.isInfoEnabled()) {
            _jobLogger.info("Final rsl: " + rslTree);
        }

	jobReq.setRslAttributes(rsl);

	int tmpInt;
	long tmpLong;

	try {

            // *** check directory ***

            tmpArg = rsl.getSingle("directory");
            if (tmpArg == null) {
                tmpArg = System.getProperty("user.home");
            }

            File dir = new File(tmpArg);
            if (!dir.exists() || dir.isFile()) {
                throw new JobManagerException(JobManagerException.BAD_DIRECTORY);
            }

            jobReq.setDirectory(dir);

	    // *** check executable ***
	    
	    tmpArg = rsl.getSingle("executable");
	    
	    if (tmpArg == null) {
		throw new JobManagerException(JobManagerException.EXECUTABLE_UNDEFINED);
	    }
	    
	    if (tmpArg.indexOf("://") != -1) {
		tmpArg = stageExecutable(tmpArg);
	    }
	    
	    File exec = null;

	    if (isAbsolutePath(tmpArg)) {
		exec = new File(tmpArg);
	    } else {
		exec = new File(dir, tmpArg);
	    }
	    if (!exec.exists()) {
		throw new JobManagerException(JobManagerException.EXECUTABLE_NOT_FOUND);
	    }
	    
	    // set executable
	    jobReq.setExecutable(exec.getAbsolutePath());
	    
	    // FIXME: should these checks be part of the JobRequest class?
	    
	    // *** count ***
	    
	    tmpInt = getAsInt(rsl.getSingle("count"), 1,
			      JobManagerException.INVALID_COUNT);
	    if (tmpInt < 1) {
		throw new JobManagerException(JobManagerException.INVALID_COUNT);
	    }
	    jobReq.setCount(tmpInt);
	    
	    // *** min memory ***
	    
	    tmpLong = getAsLong(rsl.getSingle("minMemory"), 0,
				JobManagerException.INVALID_MIN_MEMORY);
	    if (tmpLong < 0) {
		throw new JobManagerException(JobManagerException.INVALID_MIN_MEMORY);
	    }
	    jobReq.setMinMemory(tmpLong);
	    
	    // *** max memory ***
	    
	    tmpLong = getAsLong(rsl.getSingle("maxMemory"), 0,
				JobManagerException.INVALID_MAX_MEMORY);
	    if (tmpLong < 0) {
		throw new JobManagerException(JobManagerException.INVALID_MAX_MEMORY);
	    }
	    jobReq.setMaxMemory(tmpLong);
	    
	    // *** host count ***
	    
	    tmpInt = getAsInt(rsl.getSingle("hostCount"), 1,
			      JobManagerException.INVALID_HOST_COUNT);
	    if (tmpInt < 1) {
		throw new JobManagerException(JobManagerException.INVALID_HOST_COUNT);
	    }
	    jobReq.setHostCount(tmpInt);
	    
	    // *** wall time ***
	    
	    tmpInt = getAsInt(rsl.getSingle("maxWallTime"), 0,
			      JobManagerException.INVALID_MAX_WALL_TIME);
	    if (tmpInt < 0) {
		throw new JobManagerException(JobManagerException.INVALID_MAX_WALL_TIME);
	    }
	    jobReq.setMaxWallTime(tmpInt);
	    
	    // *** max cpu time ***
	    
	    tmpInt = getAsInt(rsl.getSingle("maxCpuTime"), 0,
			      JobManagerException.INVALID_MAX_CPU_TIME);
	    if (tmpInt < 0) {
		throw new JobManagerException(JobManagerException.INVALID_MAX_CPU_TIME);
	    }
	    jobReq.setMaxCpuTime(tmpInt);
	    
	    // *** max time ***
	    
	    tmpInt = getAsInt(rsl.getSingle("maxTime"), 0,
			      JobManagerException.INVALID_MAXTIME);
	    if (tmpInt < 0) {
		throw new JobManagerException(JobManagerException.INVALID_MAXTIME);
	    }
	    jobReq.setMaxTime(tmpInt);
	    
	    // *** job type ***
	    
	    tmpArg = rsl.getSingle("jobType");
	    if (tmpArg != null) {
		if (tmpArg.equalsIgnoreCase("mpi")) {
		tmpInt = JobRequest.JOBTYPE_MPI;
		} else if (tmpArg.equalsIgnoreCase("single")) {
		    tmpInt = JobRequest.JOBTYPE_SINGLE;
		} else if (tmpArg.equalsIgnoreCase("multiple")) {
		    tmpInt = JobRequest.JOBTYPE_MULTIPLE;
		} else if (tmpArg.equalsIgnoreCase("condor")) {
		    tmpInt = JobRequest.JOBTYPE_CONDOR;
		} else {
		    throw new JobManagerException(JobManagerException.INVALID_JOBTYPE);
		}
	    } else {
		tmpInt = JobRequest.JOBTYPE_MULTIPLE;
	    }
	    jobReq.setJobType(tmpInt);
	    
	    // *** dry run ***
	    
	    tmpArg = rsl.getSingle("dryrun");
	    if (tmpArg != null && tmpArg.equalsIgnoreCase("yes")) {
		jobReq.setDryRun(true);
	    } else {
		jobReq.setDryRun(false);
	    }
	    
	    // FIXME: other stuff is not set yet ***
	    
	    String inputFile = rsl.getSingle("stdin");
	    if (inputFile != null) {
		if (inputFile.indexOf("://") != -1) {
		    inputFile = stageStdin(inputFile);
		} else {
		    inputFile = getPath(inputFile, dir);
		}
	    }
	    jobReq.setStdin(inputFile);
	    
	    String outFile = getFile(rsl.getSingle("stdout"), 
				     dir,
				     JobManagerException.ERROR_OPENING_STDOUT);
	    jobReq.setStdout(outFile);
	    
	    String errFile = getFile(rsl.getSingle("stderr"), 
				     dir,
				     JobManagerException.ERROR_OPENING_STDERR);
	    
	    jobReq.setStderr(errFile);

	    // save proxy
	    saveDelegatedCredentials();
	    
	    // call the abstract method
	    request(jobReq);

	} catch(JobManagerException e) {
	    _jobLogger.error("Job request failed.", e);
	    dispose();
	    throw e;
	} catch(Exception e) {
	    _jobLogger.error("Unexpected error.", e);
	    dispose();
	    throw new JobManagerException(JobManagerException.UNIMPLEMENTED, e);
	}
	
    }

    private static boolean isAbsolutePath(String path) {
	int length = path.length();
	return ( (length > 1 && path.charAt(0) == '/') ||
		 (length > 2 && path.charAt(1) == ':' && 
		  Character.isLetter(path.charAt(0))) );
    }

    public abstract void request(JobRequest request) throws JobManagerException;

    private int getAsInt(String tmpArg, int defValue, int errorCode) 
	throws JobManagerException {
	if (tmpArg != null) {
	    try {
		return Integer.parseInt(tmpArg);
	    } catch(NumberFormatException e) {
		throw new JobManagerException(errorCode, e);
	    }
	} else {
	    return defValue;
	}
    }

    private long getAsLong(String tmpArg, long defValue, int errorCode) 
	throws JobManagerException {
        if (tmpArg != null) {
            try {
                return Long.parseLong(tmpArg);
            } catch(NumberFormatException e) {
                throw new JobManagerException(errorCode, e);
            }
        } else {
            return defValue;
        }
    }

    private String getFile(String file, File dir, int err) 
	throws JobManagerException {
	if (file == null) return null;
	
	if (file.indexOf("://") != -1) {
	    if (allowStdioUrls) {
		return file;
	    } else {
		return redirectThruFile(file, err);
	    }
	} else {
	    return getPath(file, dir);
	}
    }

    protected String redirectThruFile(String file, int err) 
	throws JobManagerException {
	OutputStream out = openUrl(file, err);
	
	File outFile;
	
	try {
	    outFile = File.createTempFile("output", ".tmp");
	} catch(IOException e) {
	    throw new JobManagerException(err, e);
	}
	
	fileList.add(outFile);
	
	if (_outputFollower == null) {
	    _outputFollower = new Tail();
	    _outputFollower.setLogger(new Log4JLogger(_jobLogger));
	    _outputFollower.start();
	}

	try {
	    _outputFollower.addFile(outFile, out, 0);
	} catch(IOException e) {
	    _jobLogger.error("Unexpected error in io redirection", e);
	}
	
	return outFile.getAbsolutePath();
    }
	
    protected OutputStream openUrl(String file, int err) 
	throws JobManagerException {
	GlobusURL url = null;
	try {
	    url = new GlobusURL(file);
	} catch(Exception e) {
	    throw new JobManagerException(err,
					  "Invalid url: " + file,
					  e);
	}
	try {
	    return openUrl(url);
	} catch(Exception e) {
	    throw new JobManagerException(err,
					  "Failed to open (remote): " + file,
					  e);
	}
    }

    protected String getPath(String localFile, File dir) {
	if (localFile.length() == 0) return localFile;
	if (isAbsolutePath(localFile)) {
	    return localFile;
	} else {
	    return (new File(dir, localFile)).getAbsolutePath();
	}
    }

    protected OutputStream openUrl(GlobusURL url)
        throws Exception {
        String protocol = url.getProtocol();
        if (protocol.equalsIgnoreCase("https")) {
            return new GassOutputStream(_credential,
					SelfAuthorization.getInstance(),
                                        url.getHost(),
                                        url.getPort(),
                                        url.getPath(),
                                        -1,
                                        appendStdout);
        } else if (protocol.equalsIgnoreCase("http")) {
            return new HTTPOutputStream(url.getHost(),
                                        url.getPort(),
                                        url.getPath(),
                                        -1,
                                        appendStdout);
        } else if (protocol.equalsIgnoreCase("gsiftp") ||
		   protocol.equalsIgnoreCase("gridftp")) {
            return new GridFTPOutputStream(_credential,
					   HostAuthorization.getInstance(),
					   url.getHost(),
					   url.getPort(),
					   url.getPath(),
					   appendStdout,
					   true);
        } else if (protocol.equalsIgnoreCase("ftp")) {
            return new FTPOutputStream(url.getHost(),
                                       url.getPort(),
                                       url.getUser(),
                                       url.getPwd(),
                                       url.getPath(),
                                       appendStdout);
        } else {
            throw new Exception("Protocol not supported: " + protocol);
	}
    }

    public void signal(int signal, String args)
	throws JobManagerException {
	if (_jobLogger.isInfoEnabled()) {
	    _jobLogger.info("signal called: " + signal + " " + args);
	}
	switch(signal) {
	case GRAMConstants.SIGNAL_CANCEL:
	    cancel(); break;
	default:
	    throw new JobManagerException(JobManagerException.UNKNOWN_SIGNAL_TYPE);
	}
    }

    public void addJobStatusListener(JobStatusListener jobStatusListener) {
        if (_jobLogger.isInfoEnabled()) {
            _jobLogger.info("addStatusListener: " + jobStatusListener);
        }
	if (jobStatusListener == null) {
	    throw new IllegalArgumentException("job status listener cannot be null");
	}
	if (jobStatusListener instanceof JobDoneListener) {
	    _jobDoneListener = (JobDoneListener)jobStatusListener;
	} else {
	    _callbackUrl.put(jobStatusListener.getID(),
			     jobStatusListener);
	}
	// can throw JobManagerException.ERROR_INSERTING_CLIENT_CONTACT)
    }
    
    public void removeJobStatusListener(JobStatusListener jobStatusListener) 
	throws JobManagerException {
        if (_jobLogger.isInfoEnabled()) {
            _jobLogger.info("removeStatusListener: " + jobStatusListener);
        }
	if (jobStatusListener == null) {
            throw new IllegalArgumentException("job status listener cannot be null");
        }
	removeJobStatusListenerByID(jobStatusListener.getID());
    }
    
    public void removeJobStatusListenerByID(String jobStatusListenerID) 
	throws JobManagerException {
        if (_jobLogger.isInfoEnabled()) {
            _jobLogger.info("removeStatusListenerID: " + jobStatusListenerID);
        }
	if (jobStatusListenerID == null) {
	    throw new IllegalArgumentException("job status listener id cannot be null");
	}
	JobStatusListener list = (JobStatusListener)_callbackUrl.remove(jobStatusListenerID);
	if (list == null) {
	    throw new JobManagerException(JobManagerException.CLIENT_CONTACT_NOT_FOUND);	    
	}
	list.dispose();
    }
    
    /**
     * Provides the status of the job.
     * @return the status of the job.
     */
    public int getStatus(){
	return _status;
    }
    
    /**
     * Provides the failure code or exit code of the job.
     * @return the failure code or exit code of the job.
     */
    public int getFailureCode(){
	return _failureCode;
    }

    /**
     * changes the old status to the new status and calls any required status
     * updates which are registered.
     * @param status the new status of the process.
     */
    public synchronized void setStatus(int status){
	if (_status == status) return;
	if (isValidStatus(status)) {
	    _status = status;
	    
	    if (_jobLogger.isInfoEnabled()) {
		_jobLogger.info("Setting job status to: " + _status);
	    }
	
	    /* send DONE or FAILED event only after
	     * the whole output is sent to the client if any */
	    if (status == GRAMConstants.STATUS_DONE ||
		status == GRAMConstants.STATUS_FAILED) {
		
		if (_outputFollower != null) {
		    _outputFollower.stop();
		    try {
			_outputFollower.join();
		    } catch (Exception e) {
			_jobLogger.error("Unexpected error", e);
		    }
		}
		
		fireStatusUpdate();
		
		dispose();
		
	    } else {

		fireStatusUpdate();
		
	    }
	}
    }

    public void fireStatusUpdate() {
	Enumeration e = _callbackUrl.elements();
	JobStatusListener listener;
	while(e.hasMoreElements()) {
	    listener = (JobStatusListener)e.nextElement();
	    listener.statusChanged(this);
	}
    }

    protected void dispose() {
	_jobLogger.info("[base jm] Cleaning up...");
	Iterator iter = fileList.iterator();
	while(iter.hasNext()) {
	    File f = (File)iter.next();
	    if (_jobLogger.isDebugEnabled()) {
		_jobLogger.debug("Deleting tmp file: " + f);
	    }
	    f.delete();
	}
	
	if (_callbackUrl != null) {
	    Enumeration e = _callbackUrl.elements();
	    JobStatusListener listener;
	    while(e.hasMoreElements()) {
		listener = (JobStatusListener)e.nextElement();
		listener.dispose();
	    }
	}
	
	if (_jobDoneListener != null) {
	    _jobDoneListener.dispose();
	}

	if (_outputFollower != null) {
	    _outputFollower.stop();
	}
    }
    
    /**
     * checks to whether or not status is valid
     * @return true if status is valid, otherwise false.
     */
    private boolean isValidStatus(int status){
	return  status == GRAMConstants.STATUS_ACTIVE  ||
	    status == GRAMConstants.STATUS_DONE    ||
	    status == GRAMConstants.STATUS_FAILED  ||
	    status == GRAMConstants.STATUS_PENDING ||
	    status == GRAMConstants.STATUS_SUSPENDED;
    }

    public String stageExecutable(String url) 
	throws JobManagerException {
	String file = null;
	try {
	    file = stageFile(url);
	} catch(Exception e) {
	    throw new JobManagerException(JobManagerException.ERROR_STAGING_EXECUTABLE, e);
	}
	Util.setFilePermissions(file, 700);
	return file;
    }
    
    public String stageStdin(String url)
        throws JobManagerException {
        try {
            return stageFile(url);
        } catch(Exception e) {
            throw new JobManagerException(JobManagerException.ERROR_STAGING_STDIN, e);
        }
    }
    
    private String getExtension(GlobusURL url) {
	String file = url.getPath();
	char ch;
	for (int i=file.length()-1;i>0;i--) {
	    ch = file.charAt(i);
	    if (ch == '/' || ch == '\\') {
		break;
	    } else if (ch == '.') {
		return file.substring(i);
	    }
	}
	return ".tmp";
    }

    protected String stageFile(String url) 
	throws MalformedURLException, 
	       IOException, 
	       UrlCopyException {
	// it is a url
	GlobusURL remoteUrl;
	GlobusURL localUrl;
	
	remoteUrl = new GlobusURL(url);
	
	File localFile = null;
	
	localFile = File.createTempFile("staged", getExtension(remoteUrl));
	
	fileList.add(localFile);
	
	localUrl = new GlobusURL(localFile.toURL());
	
	UrlCopy c = new UrlCopy();
	c.setCredentials(_credential);
	c.setSourceUrl(remoteUrl);
	c.setDestinationUrl(localUrl);
	c.copy();
	
	return localFile.getAbsolutePath();
    }
    
}
