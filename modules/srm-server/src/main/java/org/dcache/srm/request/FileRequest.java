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

import java.net.URI;

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
import org.dcache.srm.util.Lifetimes;
import org.dcache.srm.v2_2.TReturnStatus;


/**
 * A FileRequest is used by ContainerRequest (and its subclasses) to represent
 * the individual files that form this request.  For example if the user issues
 * an srmLs operation then the corresponding LsRequest object will contain zero
 * or more LsFileRequest objects (a subclass of FileRequest), one for each file
 * in the srmLs operation.
 */
public abstract class FileRequest<R extends ContainerRequest> extends Job {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FileRequest.class);
    //file ContainerRequest is being processed
    // for get and put it means that file turl
    // is not available yet
    //for copy - file is being copied

    @SuppressWarnings("unchecked")
    private final Class<R> containerRequestType = (Class<R>) new TypeToken<R>(getClass()) {}.getRawType();

    //request which contains this fileRequest (which is different from request number)
    private final long requestId;

    //pointer to underlying storage
    private transient AbstractStorageElement storage;

    private transient QOSTicket qosTicket;

    /** Creates new FileRequest */
    protected FileRequest(long requestId,
                          long lifetime)
    {
        super(lifetime);
        this.requestId = requestId;
        LOGGER.debug("created");
    }

    /** this constructor is used for restoring the previously
     * saved FileRequest from persitance storage
     */


    protected FileRequest(
    long id,
    Long nextJobId,
    long creationTime,long lifetime,
    int stateId,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    long requestId,
    String statusCodeString) {
        super(id,
        nextJobId,
        creationTime,  lifetime,
        stateId,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray, statusCodeString);
        this.requestId = requestId;
        LOGGER.debug("restored");

    }

    protected abstract TReturnStatus getReturnStatus();

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return (int) (getId() ^ getId() >> 32);
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        super.processStateChange(newState, description);

        // Notify container *after* this job's state is fully updated.
        try {
            getContainerRequest().fileRequestStateChanged(this);
        } catch (SRMInvalidRequestException ire) {
            LOGGER.error(ire.toString());
        }
    }

    public void abort(String reason) throws IllegalStateTransition, SRMException
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
            if (!getState().isFinal()) {
                setState(State.CANCELED, reason);
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

    protected void setQOSTicket(QOSTicket qosTicket) {
        this.qosTicket = qosTicket;
    }

    protected QOSTicket getQOSTicket() {
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

    protected void reassessLifetime(long fileSize)
    {
        long currentLifetime = getLifetime();

        Configuration config = SRM.getSRM().getConfiguration();
        long newLifetime = Lifetimes.calculateRequestLifetimeWithWorkaround(currentLifetime,
                fileSize, config.getMaximumClientAssumedBandwidth(), config.getGetLifetime());
        try {
            if (newLifetime > currentLifetime) {
                extendLifetime(newLifetime);
            }
        } catch (SRMException e) {
            LOGGER.debug("Unable to adjust lifetime: {}", e.getMessage());
        }
    }

    @Override
    public JDC applyJdc()
    {
        JDC current = jdc.apply();
        JDC.appendToSession(String.valueOf(requestId) + ':' + String.valueOf(getId()));
        return current;
    }

     /**
     * @return the storage
     */
    protected final AbstractStorageElement getStorage() {
        if(storage == null) {
            storage = SRM.getSRM().getStorage();
        }
        return storage;
    }

    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        toString(sb, "", longformat);
    }

    abstract void toString(StringBuilder sb, String padding, boolean longformat);
}
