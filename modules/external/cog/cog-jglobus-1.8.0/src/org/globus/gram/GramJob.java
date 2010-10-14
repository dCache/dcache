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
package org.globus.gram;

import java.util.Vector;
import java.net.MalformedURLException;

import org.globus.util.GlobusURL;
import org.globus.gram.internal.GRAMConstants;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;

/**
 * This class represents a simple gram job. It allows
 * for submitting a job to a gatekeeper, canceling it, 
 * sending a signal command and registering and
 * unregistering from callback. 
 */
public class GramJob implements GRAMConstants {
    
    /* holds job credentials */
    private GSSCredential credential;

    private String rsl;
    private GlobusURL id;
    protected int status;
    protected int error;
    private Vector listeners;

    /**
     * Creates a gram job with specified rsl with
     * default user credentials.
     * 
     * @param rsl resource specification string
     */
    public GramJob(String rsl) {
	this.rsl = rsl;
	this.credential = null;
	this.id = null;
	this.status = 0;
    }

    /**
     * Creates a gram job with specified rsl and
     * user credentials.
     *
     * @param cred user credentials
     * @param rsl   resource specification string
     */
    public GramJob(GSSCredential cred, String rsl) {
	this.rsl = rsl;
	this.credential = cred;
	this.id = null;
	this.status = 0;
    }

    /** 
     * Add a listener to the GramJob. The listener will be notified whenever
     * the status of the GramJob changes.
     * 
     * @param listener The object that wishes to receive status updates.
     * @see org.globus.gram.GramJobListener
     */
    public void addListener(GramJobListener listener) {
	if (listeners == null) listeners = new Vector();
	listeners.addElement(listener);
    }
    
    /** 
     * Remove a listener from the GramJob. The listener will no longer be
     * notified of status changes for the GramJob.
     * 
     * @param listener The object that wishes to stop receiving status updates.
     * @see org.globus.gram.GramJobListener
     */
    public void removeListener(GramJobListener listener) {
	if (listeners == null) return;
	listeners.removeElement(listener);
    }
    
    /**
     * Gets the rsl of this job.
     *
     * @return resource specification string
     */
    public String getRSL() {
	return rsl;
    }

    /**
     * Gets the credentials of this job.
     * 
     * @return job credentials. If null none were set.
     */
    public GSSCredential getCredentials() {
	return this.credential;
    }
    
    /**
     * Sets credentials of the job
     *
     * @param  credential user credentials
     * @throws IllegalArgumentException if credentials are already set
     */
    public void setCredentials(GSSCredential credential) {
	if (this.credential != null) {
	    throw new IllegalArgumentException("Credentials already set");
	} else {
	    this.credential = credential;
	}
    }

    /**
     * Sets the job handle. It is automatically
     * set after the job successfuly has been
     * successfuly started on a gatekeeper.
     *
     * @param  jobUrl job handle in form of url
     * @throws MalformedURLException if the job
     *         handle is invalid
     */
    public void setID(String jobUrl) 
	throws MalformedURLException {
	this.id = new GlobusURL(jobUrl);
    }
    
    /**
     * Gets the job handle of this job.
     *
     * @return job handle
     */
    public GlobusURL getID() {
	return id;
    }
    
    /**
     * Gets the job handle of this job and 
     * returns it as a string representaion.
     *
     * @return job handle as string
     */
    public String getIDAsString() {
	if (id == null) return null;
	return id.getURL();
    }
    
    /**
     * Gets the current status of this job.
     *
     * @return current job status
     */
    public int getStatus() {
	return status;
    }
    
    /**
     * Sets the status of the job. 
     * User should not call this function.
     * 
     * @param status status of the job
     */
    protected void setStatus(int status) {
	if (this.status == status) return;
	this.status = status;
	
	if (listeners == null) return;
	int size = listeners.size();
	for(int i=0;i<size;i++) {
	    GramJobListener listener = (GramJobListener)listeners.elementAt(i);
	    listener.statusChanged(this);
	}
    }

    /**
     * Submits a job to the specified gatekeeper as an
     * interactive job. Performs limited delegation.
     * 
     * @param contact the resource manager contact.
     * The contact can be specified in number of ways for 1.1.3 gatekeepers:
     * <br>
     * host <br>
     * host:port <br>
     * host:port/service <br>
     * host/service <br>
     * host:/service <br>
     * host::subject <br>
     * host:port:subject <br>
     * host/service:subject <br>
     * host:/service:subject <br>
     * host:port/service:subject <br>
     * For 1.1.2 gatekeepers full contact string must be specifed.
     *
     * @throws GramException 
     *         if error occurs during job submission.
     * @throws GSSException
     *         if user credentials are invalid.
     */
    public void request(String contact) 
	throws GramException, GSSException {
	Gram.request(contact, this, false);
    }
    
    /**
     * Submits a job to the specified gatekeeper either as
     * an interactive or batch job. Performs limited delegation.
     * 
     * @param contact 
     *        the resource manager contact.
     * @param batch 
     *        specifies if the job should be submitted as 
     *        a batch job.
     * @throws GramException 
     *         if error occurs during job submission.
     * @throws GSSException
     *         if user credentials are invalid.
     * @see #request(String) for detailed resource manager
     *       contact specification.
     */
    public void request(String contact, boolean batch) 
	throws GramException, GSSException {
	Gram.request(contact, this, batch);
    }

    /**
     * Submits a job to the specified gatekeeper either as
     * an interactive or batch job. It can perform limited
     * or full delegation.
     *
     * @param contact
     *        the resource manager contact. 
     * @param batch
     *        specifies if the job should be submitted as
     *        a batch job.
     * @param limitedDelegation
     *        true for limited delegation, false for
     *        full delegation.
     * @throws GramException
     *         if error occurs during job submission.
     * @throws GSSException
     *         if user credentials are invalid.
     * @see #request(String) for detailed resource manager 
     *       contact specification.
     */
    public void request(String contact, 
			boolean batch, 
			boolean limitedDelegation)
	throws GramException, GSSException {
	Gram.request(contact, this, batch, limitedDelegation);
    }
    
    /**
     * Cancels a job.
     *
     * @throws GramException 
     *         if error occurs during job cancelation.
     * @throws GSSException
     *         if user credentials are invalid.
     */
    public void cancel() 
	throws GramException, GSSException {
	Gram.cancel(this);
    }
    
    /**
     * Registers a callback listener for this job.
     * (Reconnects to the job)
     *
     * @throws GramException 
     *         if error occurs during job registration.
     * @throws GSSException
     *         if user credentials are invalid.
     */
    public void bind() 
	throws GramException, GSSException {
	Gram.registerListener(this);
    }
    
    /**
     * Unregisters a callback listener for this job.
     * (Disconnects from the job)
     *
     * @throws GramException 
     *         if error occurs during job unregistration.
     * @throws GSSException
     *         if user credentials are invalid.
     */
    public void unbind() 
	throws GramException, GSSException {
	Gram.unregisterListener(this);
    }
    
    /**
     * Sends a signal command to the job.
     *
     * @param signal signal type
     * @param arg    argument of signal
     * @throws GramException 
     *         if error occurs during signalization.
     * @throws GSSException
     *         if user credentials are invalid.
     */
    public int signal(int signal, String arg) 
	throws GramException, GSSException {
	return Gram.jobSignal(this, signal, arg);
    }
    
    /**
     * Sends a signal command to the job.
     *
     * @param signal signal type
     * @throws GramException
     *         if error occurs during signalization.
     * @throws GSSException
     *         if user credentials are invalid.
     */
    public int signal(int signal)
	throws GramException, GSSException {
	return Gram.jobSignal(this, signal, "");
    }
    
    /**
     * Sets the error code of the job.
     * Note: User should not use this method.
     *
     * @param code error code
     */
    protected void setError(int code) {
	this.error = code;
    }
    
    /**
     * Gets the error of the job.
     *
     * @return error number of the job.
     */
    public int getError() {
	return error;
    }
    
    /**
     * Returns string representation of this job.
     *
     * @return string representation of this job. Useful for
     *         debugging.
     */
    public String toString() {
	return "RSL: " + rsl + " id: " + id;
    }
    
    /** 
     * Get the status of the GramJob.
     * 
     * @return string representing the status of the GramJob. This String is 
     *         useful for user-readable output.
     */
    public String getStatusAsString() {
	return getStatusAsString(status);
    }
    
    /** 
     * Convert the status of a GramJob from an integer to a string. This
     * method is not typically called by users.
     * 
     * @return string representing the status of the GramJob passed as an 
     *         argument.
     */
    public static String getStatusAsString(int status) {
	if (status == STATUS_PENDING) {
	    return "PENDING";
	} else if (status == STATUS_ACTIVE) {
	    return "ACTIVE";
	} else if (status == STATUS_DONE) {
	    return "DONE";
	} else if (status == STATUS_FAILED) {
	    return "FAILED";
	} else if (status == STATUS_SUSPENDED) {
	    return "SUSPENDED";
	} else if (status == STATUS_UNSUBMITTED) {
	    return "UNSUBMITTED";
	} else if (status == STATUS_STAGE_IN) {
	    return "STAGE_IN";
	} else if (status == STATUS_STAGE_OUT) {
	    return "STAGE_OUT";
	}
	return "Unknown";
    }
    
}
