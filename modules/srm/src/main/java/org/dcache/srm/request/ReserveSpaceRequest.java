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

import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 * File request is an abstract "SRM file request"
 * its concrete subclasses are GetFileRequest,PutFileRequest and CopyFileRequest
 * File request is one in a set of file requests within a request
 * each Request is identified by its requestId
 * and each file request is identified by its fileRequestId within Request
 * File Request contains  a reference to its Request
 *
 *
 * @author  timur
 * @version
 */
public final class ReserveSpaceRequest extends Request {
    private static final Logger logger =
            LoggerFactory.getLogger (ReserveSpaceRequest.class);

    private long sizeInBytes ;
    private final TRetentionPolicy retentionPolicy;
    private final TAccessLatency accessLatency;
    private String spaceToken;
    private long spaceReservationLifetime;


    /** Creates new ReserveSpaceRequest */
    public ReserveSpaceRequest(
            Long  requestCredentalId,
            SRMUser user,
            long lifetime,
            int maxNumberOfRetries,
            long sizeInBytes ,
            long spaceReservationLifetime,
            TRetentionPolicy retentionPolicy,
            TAccessLatency accessLatency,
            String description,
            String clienthost) throws Exception {
              super(user,
              requestCredentalId,
              maxNumberOfRetries,
              0,
              lifetime,
              description,
              clienthost);

        this.sizeInBytes = sizeInBytes ;
        this.retentionPolicy = retentionPolicy;
        this.accessLatency = accessLatency;
        this.spaceReservationLifetime = spaceReservationLifetime;
    }

    /** this constructor is used for restoring the previously
     * saved FileRequest from persitance storage
     */


    public ReserveSpaceRequest(
            long id,
            Long nextJobId,
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
            Long  requestCredentalId,
            long sizeInBytes,
            long spaceReservationLifetime,
            String spaceToken,
            String retentionPolicy,
            String accessLatency,
            String description,
            String clienthost,
            String statusCodeString) {
                super(id,
                nextJobId,
                creationTime,
                lifetime,
                stateId,
                errorMessage,
                user,
                scheduelerId,
                schedulerTimeStamp,
                numberOfRetries,
                maxNumberOfRetries,
                lastStateTransitionTime,
                jobHistoryArray,//VVV
                requestCredentalId,
                0,
                false,
                description,
                clienthost,
                statusCodeString);
        this.sizeInBytes = sizeInBytes;
        this.spaceToken = spaceToken;

        this.retentionPolicy = retentionPolicy == null?null: TRetentionPolicy.fromString(retentionPolicy);
        this.accessLatency = accessLatency == null ?null :TAccessLatency.fromString(accessLatency);
        this.spaceReservationLifetime = spaceReservationLifetime;

        logger.debug("restored");
    }


    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        sb.append(" ReserveSpaceRequest ");
        sb.append(" id =").append(getId());
        sb.append(" created: ").append(getUser());
        sb.append(" spaceToken: ").append(getSpaceToken());
        sb.append(" state=").append(getState());
        if(longformat) {
            sb.append("\ncredential: \"").append(getCredential()).
                append("\"\n");
            sb.append("\nsubmitted: ").
                append(new Date(getCreationTime()));
            sb.append("\nexpires: ").
                append(new Date(
                    getCreationTime() +getLifetime()));
            sb.append("\nstatus code: ").append(getStatusCode());
            sb.append("\nerror message: ").append(getErrorMessage());
            sb.append('\n').append("   lifetime: ").append(getSpaceReservationLifetime());
            sb.append('\n').append("   AccessLatency: ").append(getAccessLatency());
            sb.append('\n').append("   RetentionPolicy: ").append(getRetentionPolicy());
            sb.append('\n').append("History of State Transitions: \n");
            sb.append(getHistory());
        }
    }


    @Override
    protected void stateChanged(State oldState) {
    }


    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure {
        try{
            SrmReserveSpaceCallbacks callbacks = new SrmReserveSpaceCallbacks(this.getId());
            getStorage().srmReserveSpace(
            getUser(),
            sizeInBytes,
            spaceReservationLifetime,
            retentionPolicy == null ? null:retentionPolicy.getValue(),
            accessLatency == null ? null:accessLatency.getValue(),
            getDescription(),
            callbacks
            );
            setState(State.ASYNCWAIT,
                    "waiting Space Reservation completion");
        } catch(IllegalStateTransition e) {
            throw new FatalJobFailure("cannot reserve space: " + e.getMessage());
        }
    }

    public SrmStatusOfReserveSpaceRequestResponse getSrmStatusOfReserveSpaceRequestResponse() {
        rlock();
        try {
            SrmStatusOfReserveSpaceRequestResponse response =
                    new SrmStatusOfReserveSpaceRequestResponse();
            response.setReturnStatus(getTReturnStatus());
            response.setRetentionPolicyInfo(new TRetentionPolicyInfo(retentionPolicy, accessLatency));
            response.setSpaceToken(getSpaceToken());
            response.setSizeOfTotalReservedSpace(new UnsignedLong(sizeInBytes) );
            response.setSizeOfGuaranteedReservedSpace(new UnsignedLong(sizeInBytes));
            response.setLifetimeOfReservedSpace((int)(spaceReservationLifetime/1000L));
            return response;
        } finally {
            runlock();
        }

    }

    public SrmReserveSpaceResponse getSrmReserveSpaceResponse() {
        rlock();
        try {
            SrmReserveSpaceResponse response = new SrmReserveSpaceResponse();
            response.setReturnStatus(getTReturnStatus());
            response.setRetentionPolicyInfo(new TRetentionPolicyInfo(retentionPolicy, accessLatency));
            response.setRequestToken(String.valueOf(getId()));
            response.setSpaceToken(getSpaceToken());
            response.setSizeOfTotalReservedSpace(new UnsignedLong(sizeInBytes) );
            response.setSizeOfGuaranteedReservedSpace(new UnsignedLong(sizeInBytes));
            response.setLifetimeOfReservedSpace((int)(spaceReservationLifetime/1000L));
            return response;
        } finally {
            runlock();
        }
    }

    public final TReturnStatus getTReturnStatus()   {
        TReturnStatus status = new TReturnStatus();
        rlock();
        State state;
        TStatusCode statusCode;
        try {
            status.setExplanation(getErrorMessage());
            state = getState();
            statusCode = getStatusCode() ;
        } finally {
            runlock();
        }
        if(statusCode != null) {
            status.setStatusCode(statusCode);
        }
        else if(state == State.FAILED) {
            status.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        else if(state == State.CANCELED) {
            status.setStatusCode(TStatusCode.SRM_ABORTED);
        }
        else if(state == State.DONE) {
            status.setStatusCode(TStatusCode.SRM_SUCCESS);
        }
        else {
            status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
        }
        return status;
    }

    public static ReserveSpaceRequest getRequest(Long requestId)
            throws SRMInvalidRequestException {
        return Job.getJob( requestId, ReserveSpaceRequest.class);
    }

    private class SrmReserveSpaceCallbacks implements org.dcache.srm.SrmReserveSpaceCallbacks {
        private final long requestJobId;
        public SrmReserveSpaceCallbacks(long requestJobId){
            this.requestJobId = requestJobId;
        }

        public ReserveSpaceRequest getReserveSpacetRequest()
                throws SRMInvalidRequestException {
            return Job.getJob(requestJobId, ReserveSpaceRequest.class);
        }

        @Override
        public void ReserveSpaceFailed(String reason) {

            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
                return;
            }
            try {
                request.setState(State.FAILED,reason);
            } catch(IllegalStateTransition ist) {
                logger.error("Illegal State Transition : " +ist.getMessage());
            }

            logger.error("ReserveSpace error: "+ reason);
        }

        @Override
        public void NoFreeSpace(String  reason) {
            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
                return;
            }

            try {
                request.setStateAndStatusCode(State.FAILED,reason,TStatusCode.SRM_NO_FREE_SPACE);
            } catch(IllegalStateTransition ist) {
                logger.error("Illegal State Transition : " +ist.getMessage());
            }

            logger.error("ReserveSpace failed (NoFreeSpace), no free space : "+reason);
        }

        @Override
        public void ReserveSpaceFailed(Exception e) {
            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
                return;
            }

            try {
                request.setState(State.FAILED,e.toString());
            } catch(IllegalStateTransition ist) {
              logger.error("Illegal State Transition : " +ist.getMessage());
            }

            logger.error("ReserveSpace exception: ",e);
        }

        @Override
        public void SpaceReserved(String spaceReservationToken, long reservedSpaceSize) {
            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
                return;
            }
            request.wlock();
            try {
                State state = request.getState();
                if(!State.isFinalState(state)) {

                    request.setSpaceToken(spaceReservationToken);
                    request.setSizeInBytes(reservedSpaceSize);
                    request.setState(State.DONE,"space reservation succeeded" );
                }
            } catch(IllegalStateTransition ist) {
                logger.error("Illegal State Transition : " +ist.getMessage());
            } finally {
                wunlock();
            }
        }
    }

    public long getSizeInBytes() {
        rlock();
        try {
            return sizeInBytes;
        } finally {
            runlock();
        }
    }

    public void setSizeInBytes(long sizeInBytes) {
        wlock();
        try {
            this.sizeInBytes = sizeInBytes;
        } finally {
            wunlock();
        }
    }

    public TRetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    public TAccessLatency getAccessLatency() {
        return accessLatency;
    }

    public String getSpaceToken() {
        rlock();
        try {
            return spaceToken;
        } finally {
            runlock();
        }
    }

    public void setSpaceToken(String spaceToken) {
        wlock();
        try {
            this.spaceToken = spaceToken;
        } finally {
            wunlock();
        }
    }

    public long getSpaceReservationLifetime() {
        rlock();
        try {
            return spaceReservationLifetime;
        } finally {
            runlock();
        }
    }

    public void setSpaceReservationLifetime(long spaceReservationLifetime) {
        wlock();
        try {
            this.spaceReservationLifetime = spaceReservationLifetime;
        } finally {
            wunlock();
        }
    }

    @Override
    public String getMethod(){
        return "srmReserveSpace";
    }
}
