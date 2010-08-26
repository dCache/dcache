/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * Request.java
 *
 * Created on September 29, 2006, 3:12 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.request;

import java.sql.SQLException;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMInvalidRequestException;

/**
 *
 * @author timur
 */
public abstract class Request extends Job {
    private String client_host;
    private SRMUser user;
    public Request(SRMUser user,
    Long requestCredentalId,
    JobStorage requestJobsStorage,
    Configuration configuration,
    int max_number_of_retries,
    long max_update_period,
    long lifetime,
    String description,
    String client_host
        ) throws Exception{
        super(lifetime,requestJobsStorage,max_number_of_retries);
        this.credentialId = requestCredentalId;
        this.storage = configuration.getStorage();
        this.configuration = configuration;
        this.max_update_period = max_update_period;
        this.description = description;
        this.client_host = client_host;
        this.user = user;
    }
    
   /**
     * this constructor is used for restoring the previously
     * saved Request from persitance storage
     */
  
    protected Request(
    Long id,
    Long nextJobId,
    JobStorage jobStorage,
    long creationTime,
    long lifetime,
    int stateId,
    String errorMessage,
    SRMUser user,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long credentialId,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    Configuration configuration
    ) {
        super(id,
        nextJobId,
        jobStorage,
        creationTime,
        lifetime,
        stateId,
        errorMessage,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,maxNumberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray);
        this.configuration = configuration;
        this.storage = configuration.getStorage();
        this.credentialId = credentialId;
        this.retryDeltaTime = retryDeltaTime;
        this.should_updateretryDeltaTime = should_updateretryDeltaTime;
        this.description = description;
        this.client_host = client_host;
        this.statusCode = statusCodeString==null
                ?null
                :TStatusCode.fromString(statusCodeString);
        this.user = user;
        say("restored");
    }
    
    
    /** general srm server configuration settings */
    protected Configuration configuration;

    
    
    
    /*
     * public static (class) variables
     */
    protected Long credentialId;


    
    /**
     * this is called every time user gets RequestStatus
     * so the next time user waits longer (up to MAX_RETRY_TIME secs)
     * if nothing has been happening for a while
     * The algoritm of incrising retryDeltaTime is absolutely arbitrary
     */
    protected int i = 0;

    protected long max_update_period = 10*60*60;

    
    
    
    /*
     * public constructors
     */
    protected static RequestsPropertyStorage requestsproperties = null;

    
    /*
     * private instance variables
     */
    protected int retryDeltaTime = 1;

    protected boolean should_updateretryDeltaTime = true;

    /** instance of the AbstractStorageElement implementation,
     * class that gives us an interface to the
     * underlying storage system
     */
    protected AbstractStorageElement storage;
    /**
     * Status code from version 2.2
     * provides a better description of 
     * reasons for failure, etc
     * need this to comply with the spec
     */
    private TStatusCode statusCode;
    
    public RequestCredential getCredential() {
        if(credentialId==null) {
            return null;
        }
        return RequestCredential.getRequestCredential(credentialId);
    }

    
    /**
     * Getter for property credentialId.
     * @return Value of property credentialId.
     */
    public Long getCredentialId() {
        return credentialId;
    }

    public abstract String getMethod();
    
    private String description;
    
    public void addDebugHistoryEvent(String description) {
        if(configuration.isJdbcLogRequestHistoryInDBEnabled()) {
            addHistoryEvent( description);
        }
    }

    
    public static Request getRequest(Long requestId) 
            throws  SRMInvalidRequestException {
        Job job = Job.getJob( requestId);
        if (job == null || !(job instanceof Request)) {
            return null;
        }
        return (Request) job;
    }

    
    
    public static Request getRequest(int requestNum) 
            throws SRMInvalidRequestException {
        return (Request) Job.getJob(new Long((long) requestNum));
    }

    
    /**
     * gets request id as int
     * @return
     * request id
     */
    public int getRequestNum() {
        return (int) (getId().longValue());
    }

    
    public SRMUser getSRMUser() {
        return user;
    }

   
    /**
     * Getter for property retryDeltaTime.
     * @return Value of property retryDeltaTime.
     */
    public int getRetryDeltaTime() {
        return retryDeltaTime;
    }

    
    /**
     * gets srm user who issued the request
     * @return
     * srm user
     */
    public SRMUser getUser() {
    return user;
    }

    
    /**
     * Getter for property should_updateretryDeltaTime.
     * @return Value of property should_updateretryDeltaTime.
     */
    public boolean isShould_updateretryDeltaTime() {
        return should_updateretryDeltaTime;
    }

    
    /**
     * reset retryDeltaTime to 1
     */
    public void resetRetryDeltaTime() {
        retryDeltaTime = 1;
    }

    //public abstract void schedule(Scheduler scheduler) throws InterruptedException, IllegalStateTransition, SQLException;

    
    /**
     * status is not going to change
     * set retry delta time to 1
     */
    public void stopUpdating() {
        retryDeltaTime = 1;
        should_updateretryDeltaTime = false;
    }

    
    protected void updateRetryDeltaTime() {
        if (should_updateretryDeltaTime && i == 0) {
            
            if(retryDeltaTime <100) {
                retryDeltaTime +=3;
            }
            else if(retryDeltaTime <300) {
                retryDeltaTime +=6;
            }
            else {
                retryDeltaTime *= 2;
            }
            if (retryDeltaTime > max_update_period) {
                retryDeltaTime = (int) max_update_period;
            }
        }
        i = (i+1)%5;
    }

    public final String getDescription() {
        return description;
    }

    public final void setDescription(String description) {
        this.description = description;
    }

    public TStatusCode getStatusCode() {
        return statusCode;
    }

    public String getStatusCodeString() {
        return statusCode==null ? null:statusCode.getValue() ;
    }

    public void setStatusCode(TStatusCode statusCode) {
        this.statusCode = statusCode;
    }
    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode==null?null:TStatusCode.fromString(statusCode);
    }

    public String getClient_host() {
        return client_host;
    }
    
    public String getSubmitterId() {
         return Long.toString(user.getId());
    }

    
}
