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

import java.util.Properties;

import org.ietf.jgss.GSSCredential;

/**
 * JobManager interface provides the common methods that is used by all extensions
 * of JobManager
 */
public interface JobManager {

    public void setCredentials(GSSCredential credentials);
    
    public GSSCredential getCredentials();

    public String getID();

    public void setID(String id);

    public Properties getSymbolTable();

    public void request(String rsl) 
	throws JobManagerException;
    
    /**
     * Retrieves the current state of the process.
     * @return the current status value
     */
    public int getStatus();
    
    /**
     * Retrieves the failure code of the running process.
     * By default it is equal to zero
     * @return the current failure code
     */
    public int getFailureCode();
    
    /**
     * Cancels the job.
     */
    public void cancel() 
	throws JobManagerException;

    /**
     * Sends a signal to the JobManager.
     */
    public void signal(int signal, String argument) 
	throws JobManagerException;
    
    
    public void addJobStatusListener(JobStatusListener listener)
	throws JobManagerException;
    
    public void removeJobStatusListener(JobStatusListener listenter)
	throws JobManagerException;
    
    public void removeJobStatusListenerByID(String id)
	throws JobManagerException;
    
}
