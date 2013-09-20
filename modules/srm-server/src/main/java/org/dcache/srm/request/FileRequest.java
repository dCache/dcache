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
 * FileRequest.java
 *
 * Created on July 5, 2002, 12:04 PM
 */

package org.dcache.srm.request;


import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.net.URI;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.qos.QOSTicket;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 * A FileRequest is used by ContainerRequest (and its subclasses) to represent
 * the individual files that form this request.  For example if the user issues
 * an srmLs operation then the corresponding LsRequest object will contain zero
 * or more LsFileRequest objects (a subclass of FileRequest), one for each file
 * in the srmLs operation.
 */
public abstract class FileRequest<R extends ContainerRequest> extends Job {
    private final static Logger logger =
            LoggerFactory.getLogger(FileRequest.class);
    //file ContainerRequest is being processed
    // for get and put it means that file turl
    // is not available yet
    //for copy - file is being copied

    @SuppressWarnings("unchecked")
    private final Class<R> containerRequestType = (Class<R>) new TypeToken<R>(getClass()) {}.getRawType();

    //request which contains this fileRequest (which is different from request number)
    protected final long requestId;
    protected Long credentialId;

    //pointer to underlying storage
    private transient AbstractStorageElement storage;
    //srm configuration
    private transient Configuration configuration;
    //error message if error, or just information message

    private transient QOSTicket qosTicket;

    /**
     * Status code from version 2.2
     * provides a better description of
     * reasons for failure, etc
     * need this to comply with the spec
     */
    private TStatusCode statusCode;

   /** Creates new FileRequest */
    protected FileRequest(long requestId,
                          Long requestCredentalId,
                          long lifetime,
                          int maxNumberOfRetries)
    {
        super(lifetime, maxNumberOfRetries);
        this.credentialId = requestCredentalId;
        this.requestId = requestId;
        logger.debug("created");
    }

    /** this constructor is used for restoring the previously
     * saved FileRequest from persitance storage
     */


    protected FileRequest(
    long id,
    Long nextJobId,
    long creationTime,long lifetime,
    int stateId,String errorMessage,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    long requestId,
    Long  requestCredentalId,
    String statusCodeString) {
        super(id,
        nextJobId,
        creationTime,  lifetime,
        stateId, errorMessage,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,maxNumberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray);
        this.credentialId = requestCredentalId;
        this.requestId = requestId;
        this.statusCode = statusCodeString==null
                ?null
                :TStatusCode.fromString(statusCodeString);
        logger.debug("restored");

    }

    public void addDebugHistoryEvent(String description) {
        if(getJobStorage().isJdbcLogRequestHistoryInDBEnabled()) {
            addHistoryEvent( description);
        }
    }

    /**
     * @return the configuration
     */
    public final Configuration getConfiguration() {
        if(configuration == null) {
            configuration = SRM.getSRM().getConfiguration();
        }
        return configuration;
    }

    public RequestCredential getCredential() throws DataAccessException {
         return RequestCredential.getRequestCredential(credentialId);
    }

   /**
    * Getter for property credentialId.
    * @return Value of property credentialId.
    */
    public Long getCredentialId() {
        return credentialId;
    }

    public abstract RequestFileStatus getRequestFileStatus() ;

    public abstract TReturnStatus getReturnStatus();

    @Override
    public boolean equals(Object o) {
        if(o == this) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (int) (getId() ^ getId() >> 32);
    }

    public void setStatus(String status) throws SRMException {
        logger.debug("("+status+")");
        try {
            wlock();
            try {
                if(status.equalsIgnoreCase("Done")) {
                    State state = getState();
                    if( !state.isFinalState()) {
                        if(state == State.READY ||
                           state == State.TRANSFERRING ||
                           state == State.RUNNING) {
                            setState(State.DONE,"set by setStatus to \"Done\"");
                        }
                        else {
                            setState(State.CANCELED,"set by setStatus to \"Done\"");
                        }
                    }
                }
                else if(status.equalsIgnoreCase("Running")) {
                    setState(State.TRANSFERRING,"set by setStatus to \"Running\"");
                }
                else {
                    String error =  "Can't set Status to "+status;
                    logger.error(error);
                    throw new SRMException(error);

                }
            } finally {
                wunlock();
            }
        }
        catch(IllegalStateTransition ist) {
            String error =  "Can't set Status to "+status+
                    " due to Illegal State Transition : " +ist.getMessage();
            logger.error(error);
            throw new SRMException(error);
        }

    }

    @Override
    protected void stateChanged(State oldState)
    {
        try {
            getContainerRequest().fileRequestStateChanged(this);
        } catch (SRMInvalidRequestException ire) {
            logger.error(ire.toString());
        }
    }

    public void abort() throws IllegalStateTransition
    {
        wlock();
        try {
            /* [ SRM 2.2, 5.12.2 ]
             *
             * a) srmAbortFiles aborts all files in the request regardless of the state.
             * g) srmAbortFiles must not change the request level status of the completed
             *    requests. Once a request is completed, the status of the request remains
             *    the same.
             * h) When duplicate abort file request is issued on the same files, SRM_SUCCESS
             *    must be returned to all duplicate abort file requests and no operations on
             *    duplicate abort file requests must performed.
             *
             * Combined we interpret this to mean that for any non-final state, the request
             * is cancelled.
             *
             * Note that the remaining items more constraints for particular types of
             * requests. Some subclasses thus override this method.
             */
            if (!getState().isFinalState()) {
                setState(State.CANCELED, "Request aborted");
            }
        } finally {
            wunlock();
        }
    }

    public SRMUser getUser() throws SRMInvalidRequestException {
        return getContainerRequest().getUser();
    }

    public R getContainerRequest() throws SRMInvalidRequestException  {
        return Job.getJob(requestId, containerRequestType);
    }

    /**
     * Getter for property requestId.
     * @return Value of property requestId.
     */
    public long getRequestId() {
        return requestId;
    }

    public void setQOSTicket(QOSTicket qosTicket) {
        this.qosTicket = qosTicket;
    }

    public QOSTicket getQOSTicket() {
        return qosTicket;
    }

    public abstract boolean isTouchingSurl(URI surl);

   /**
     * @param newLifetime  new lifetime in millis
     *  -1 stands for infinite lifetime
     * @return int lifetime left in millis
     *    -1 stands for infinite lifetime
     */

    public abstract long extendLifetime(long newLifetime) throws SRMException ;

    public TStatusCode getStatusCode() {
        rlock();
        try {
            return statusCode;
        } finally {
            runlock();
        }
    }

     public String getStatusCodeString() {
         rlock();
         try {
            return statusCode==null ? null:statusCode.getValue() ;
         } finally {
             runlock();
         }
    }

    public final void setStateAndStatusCode(
            State state,
            String description,
            TStatusCode statusCode)  throws IllegalStateTransition  {
        wlock();
        try {
            setState(state, description);
            setStatusCode(statusCode);
        } finally {
            wunlock();
        }
    }

    public void setStatusCode(TStatusCode statusCode) {
        wlock();
        try {
            this.statusCode = statusCode;
        } finally {
            wunlock();
        }
    }

    /**
     *
     * @return
     */
     @Override
     public String getSubmitterId() {
         try {
            return Long.toString(getUser().getId());
         } catch (Exception e) {
             //
             // Throwing the exception from this method
             //  will prevent the change of the status of this request
             //  to canceled, done or failed.
             // Therefore we catch the exception and
             // just report the submitter id as unknown
             //
             logger.error(e.toString());
             return "unknown";
         }
     }

    @Override
    public JDC applyJdc()
    {
        JDC current = jdc.apply();
        JDC.appendToSession(String.valueOf(requestId) + ':' + String.valueOf(id));
        return current;
    }

     /**
     * @return the storage
     */
    public final AbstractStorageElement getStorage() {
        if(storage == null) {
            storage = SRM.getSRM().getStorage();
        }
        return storage;
    }

    protected boolean isProtocolSupported(String[] protocols)
        throws SRMException
    {
        for (String supported: getStorage().supportedGetProtocols()) {
            for (String protocol: protocols) {
                if (supported.equals(protocol)) {
                    return true;
                }
            }
        }
        return false;
    }
}
