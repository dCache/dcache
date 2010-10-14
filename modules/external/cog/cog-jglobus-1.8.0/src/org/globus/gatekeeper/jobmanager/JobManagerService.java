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

import java.net.MalformedURLException;

import org.globus.util.GlobusURL;
import org.globus.gatekeeper.jobmanager.internal.JobManagerProtocol;
import org.globus.gatekeeper.jobmanager.internal.JobRequestParser;
import org.globus.gatekeeper.jobmanager.internal.JobManagerServer;
import org.globus.gatekeeper.jobmanager.internal.StatusUpdater;
import org.globus.gatekeeper.Service;
import org.globus.gatekeeper.ServiceRequest;
import org.globus.gatekeeper.ServiceException;

import org.ietf.jgss.GSSCredential;

import org.apache.log4j.Logger;

public class JobManagerService implements Service {
    
    protected JobManagerServer _jmServer;
    protected JobManagerProtocol _protocol;

    protected AbstractJobManager _jobManager;

    protected String _handle;
    protected GSSCredential _credentials;

    protected Logger _logger;
    
    public JobManagerService(AbstractJobManager jobManager) {
	if (jobManager == null) {
	    throw new IllegalArgumentException("Job Manager reference cannot be null");
	}
	_protocol = JobManagerProtocol.getInstance("");
	_jobManager = jobManager;

	_logger = Logger.getLogger( getClass().getName() + "." + hashCode() );
	_jobManager.setLogger(_logger);
    }

    public String getRequestSuccessMessage() {
	return _protocol.getRequestReply(0, getHandle());
    }

    public String getRequestFailMessage(Exception e) {
	return _protocol.getErrorMessage(e);
    }

    public void setCredentials(GSSCredential credentials) {
	_credentials = credentials;
	_jobManager.setCredentials(credentials);
    }
    
    public String getHandle(){
        return _handle;
    }

    public void setArguments(String [] args) 
	throws ServiceException {
    }
    
    public void request(ServiceRequest request) 
	throws ServiceException {
	
	JobRequestParser req = null;

	_jobManager.addJobStatusListener(new ShutdownListener());

	try {
	    
	    req = _protocol.handleJobRequest(request);
	    req.parse();
	    
	    // get callback url and register it in case

	    String callbackUrl = req.getCallbackURL();
	    if (callbackUrl == null ||
		callbackUrl.equals("") ||
		callbackUrl.equals("\"\"")) {
		// no callback url
	    } else {
		register(callbackUrl, req.getJobStateMask());
	    }
	    
	    initJobManagerServer();

	} catch(ServiceException e) {
            shutdown();
            throw e;
        } catch(Exception e) {
	    _logger.error("Unexpected error.", e);
            shutdown();
	    throw new JobManagerException();
        }
	
	/* if an error is thrown in this method
	 * dipose will be called automatically */
	_jobManager.request(req.getRSL());
    }

    // -- job manager interface ---
    
    public void cancel() 
	throws JobManagerException {
	_jobManager.cancel();
    }
    
    public void signal(int signal, String args) 
	throws JobManagerException {
	_jobManager.signal(signal, args);
    }
    
    public int getStatus() {
	return _jobManager.getStatus();
    }
    
    public int getFailureCode() {
	return _jobManager.getFailureCode();
    }

    public void register(String url, int statusMask)
        throws JobManagerException {
	JobStatusRecipient rec = null;
	try {
	    rec = new JobStatusRecipient(url, statusMask);
	} catch(Exception e) {
	    throw new JobManagerException(
					  JobManagerException.ERROR_INSERTING_CLIENT_CONTACT, e);
	}
	_jobManager.addJobStatusListener(rec);
    }
    
    public void unregister(String url)
        throws JobManagerException {
	_jobManager.removeJobStatusListenerByID(url);
    }

    private void initJobManagerServer() 
	throws JobManagerException {
	try {

	    // FIXME: does it work with port ranges?!
            _jmServer = new JobManagerServer(_credentials, 0);
            _jmServer.setJobManager(this);
	    
            _handle = _jmServer.getURL() + "/" + _jobManager.getID();

	    _jobManager.getSymbolTable().put("GLOBUS_GRAM_JOB_CONTACT",
					     _handle);
	} catch(Exception e) {
	    throw new JobManagerException(JobManagerException.JM_FAILED_ALLOW_ATTACH, e);
	}
    }
    
    public void shutdown() {
	_logger.info("[jm service] shutting down...");
	if (_jmServer != null) {
	    _jmServer.shutdown();
	}
    }

    class JobStatusRecipient implements JobStatusListener {
	
	private GlobusURL _url;
	private int _statusMask;
	private StatusUpdater _updater;

	public JobStatusRecipient(String url, int statusMask) 
	    throws MalformedURLException {
	    _url = new GlobusURL(url);
	    _statusMask = statusMask;
	}
	
	public GlobusURL getURL() {
	    return _url;
	}

	public String getID() {
	    return _url.getURL();
	}

	public int getStatusMask() {
	    return _statusMask;
	}

	public void dispose() {
	    if (_updater != null) {
		_updater.stop();
	    }
	}
	
	public void statusChanged(JobManager jobManager) {
	    if (_updater == null) {
		_updater = new StatusUpdater();
		_updater.setLogger(_logger);
		_updater.start();
	    }
	    _updater.updateStatus(_credentials, 
				  getURL(),
				  jobManager.getStatus(),
				  jobManager.getFailureCode(),
				  getHandle());
	}

	public String toString() {
	    StringBuffer buf = new StringBuffer();
	    buf.append("URL: ").append(getID());
	    buf.append(" mask: ").append(String.valueOf(_statusMask));
	    return buf.toString();
	}

    }

    class ShutdownListener extends JobDoneListener {
	public void dispose() {
	    shutdown();
	}
    }
    
    
}
