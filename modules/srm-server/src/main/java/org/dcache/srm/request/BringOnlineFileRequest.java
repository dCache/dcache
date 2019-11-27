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

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileBusyException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TBringOnlineRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 * A BringOnlineFileRequest object represents the users desire that the system
 * provide some level of guarantee of low-latency when initiating subsequent
 * transfer requests against a particular file; i.e., that the file is brought
 * into an "online state".  A BringOnlineRequest object is an aggregation of
 * one or more BringOnlineFileRequest objects, representing the ability (in the
 * SRM protocol) for the user to request multiple files be brought into a
 * low-latency state with a single request.
 */
public final class BringOnlineFileRequest extends FileRequest<BringOnlineRequest> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BringOnlineFileRequest.class);

    // the globus url class created from surl_string
    private final URI surl;
    private String pinId;
    private String fileId;
    private transient FileMetaData fileMetaData;
    private Optional<String> lastPinFailure = Optional.empty();

    /** Creates new FileRequest */
    public BringOnlineFileRequest(long requestId, URI surl, long lifetime)
    {
        super(requestId, lifetime);
        LOGGER.debug("BringOnlineFileRequest, requestId={} fileRequestId = {}", requestId, getId());
        this.surl = surl;
    }

    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */

    public BringOnlineFileRequest(
    long id,
    Long nextJobId,
    long creationTime,
    long lifetime,
    int stateId,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    long requestId,
    String statusCodeString,
    String SURL,
    String fileId,
    String pinId
    ) {
        super(id,
              nextJobId,
              creationTime,
              lifetime,
              stateId,
              scheduelerId,
              schedulerTimeStamp,
              numberOfRetries,
              lastStateTransitionTime,
              jobHistoryArray,
              requestId,
              statusCodeString);

        this.surl = URI.create(SURL);

        if(fileId != null && (!fileId.equalsIgnoreCase("null"))) {
            this.fileId = fileId;
        }

        if(pinId != null && (!pinId.equalsIgnoreCase("null"))) {
            this.pinId = pinId;
        }
    }

    private void setPinId(String pinId) {
        wlock();
        try {
            this.pinId = pinId;
        } finally {
            wunlock();
        }
    }

    public String getPinId() {
        rlock();
        try {
            return pinId;
        } finally {
            runlock();
        }
    }

    private boolean isPinned() {
        rlock();
        try {
            return getPinId() != null;
        } finally {
            runlock();
        }

    }

    public URI getSurl() {
        return surl;
    }


    public String getSurlString() {
        return getSurl().toASCIIString();
    }

    public String getFileId() {
        rlock();
        try {
            return fileId;
        } finally {
            runlock();
        }
    }


    protected TBringOnlineRequestFileStatus getTGetRequestFileStatus()
            throws SRMInvalidRequestException
    {
        TBringOnlineRequestFileStatus fileStatus = new TBringOnlineRequestFileStatus();
        if(fileMetaData != null) {
            fileStatus.setFileSize(new UnsignedLong(fileMetaData.size));
        }

        try {
             fileStatus.setSourceSURL(new org.apache.axis.types.URI(getSurlString()));
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOGGER.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }

        if(this.isPinned()) {

            fileStatus.setRemainingPinTime((int)(getRemainingLifetime()/1000));
        }
        fileStatus.setEstimatedWaitTime(getContainerRequest().getRetryDeltaTime());
        fileStatus.setStatus(getReturnStatus());

        return fileStatus;
    }

    @Override
    public void toString(StringBuilder sb, String padding, boolean longformat) {
        sb.append(padding);
        if (padding.isEmpty()) {
            sb.append("Bring online ");
        }
        sb.append("file id:").append(getId());
        sb.append(" state:").append(getState());
        if(longformat) {
            sb.append('\n');
            sb.append(padding).append("   SURL: ").append(getSurl()).append('\n');
            sb.append(padding).append("   Pinned: ").append(isPinned()).append('\n');
            String thePinId = getPinId();
            if(thePinId != null) {
                sb.append(padding).append("   Pin id: ").append(thePinId).append('\n');
            }
            TStatusCode status = getStatusCode();
            if (status != null) {
                sb.append(padding).append("   Status:").append(status).append('\n');
            }
            sb.append(padding).append("   History:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    @Override
    public final void run() throws SRMException, IllegalStateTransition
    {
        LOGGER.trace("run");
        if (!getState().isFinal() && getPinId() == null) {
            // [ SRM 2.2, 5.4.3] SRM_FILE_BUSY: client requests for a file which there is an
            // active srmPrepareToPut (no srmPutDone is yet called) request for.
            if (SRM.getSRM().isFileBusy(surl)) {
                throw new SRMFileBusyException("The requested SURL is locked by an upload.");
            }

            if (lastPinFailure.isPresent()) {
                addHistoryEvent("Retrying to pin file, previous attempt failed with " + lastPinFailure.get());
                lastPinFailure = Optional.empty();
            } else {
                addHistoryEvent("Pinning file.");
            }
            pinFile(getContainerRequest());
        }
    }


    @Override
    protected void onSrmRestartForActiveJob() throws IllegalStateTransition
    {
        State state = getState();

        if (state != State.INPROGRESS) {
            setState(State.FAILED, "Invalid state (" + state + ") detected " +
                    "after SRM service restart");
        }
    }

    private void pinFile(BringOnlineRequest request)
    {
        long desiredPinLifetime = request.getDesiredOnlineLifetimeInSeconds();
        if (desiredPinLifetime != -1) {
            desiredPinLifetime *= 1000;  // convert to millis
        }
        URI surl = getSurl();
        LOGGER.info("Pinning {}", surl);
        CheckedFuture<AbstractStorageElement.Pin,? extends SRMException> future =
                getStorage().pinFile(
                        request.getUser(),
                        surl,
                        request.getClient_host(),
                        desiredPinLifetime,
                        String.valueOf(getRequestId()),
                        true);
        LOGGER.debug("BringOnlineFileRequest: waiting async notification about pinId...");
        future.addListener(new ThePinCallbacks(getId(), future), MoreExecutors.directExecutor());
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        State oldState = getState();
        LOGGER.debug("State changed from {} to {}", oldState, getState());
        switch (newState) {
        case READY:
            try {
                getContainerRequest().resetRetryDeltaTime();
            } catch (SRMInvalidRequestException ire) {
                LOGGER.error(ire.toString());
            }
            break;
        case CANCELED:
        case FAILED:
            try {
                SRMUser user = getUser();
                String pinId = getPinId();
                String fileId = getFileId();
                AbstractStorageElement storage = getStorage();
                if (fileId != null && pinId != null) {
                    LOGGER.info("State changed to final state, unpinning fileId = {} pinId = {}.", fileId, pinId);
                    CheckedFuture<String, ? extends SRMException> future = storage.unPinFile(null, fileId, pinId);
                    future.addListener(() -> {
                        try {
                            LOGGER.debug("File unpinned (pinId={}).", future.checkedGet());
                        } catch (SRMException e) {
                            LOGGER.error("Unpinning failed: {}", e.getMessage());
                        }

                    }, MoreExecutors.directExecutor());
                } else {
                    unpinBySURLandRequestToken(storage, user, String.valueOf(getRequestId()), getSurl());
                }
            } catch (SRMInternalErrorException | SRMInvalidRequestException ire) {
                LOGGER.error(ire.toString());
            }
            break;
        }

        super.processStateChange(newState, description);
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
    }

    public TReturnStatus release(SRMUser user) throws SRMInternalErrorException
    {
        String fileId;
        String pinId;

        wlock();
        try {
            State state = getState();
            switch (state) {
            case DONE:
                fileId = getFileId();
                pinId = getPinId();
                if (fileId == null || pinId == null) {
                    return new TReturnStatus(TStatusCode.SRM_FAILURE, "SURL is not pinned");
                }
                // Unpinning is done below, outside the lock
                break;
            case CANCELED:
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "SURL has been aborted and cannot be released");
            case FAILED:
                return new TReturnStatus(TStatusCode.SRM_FAILURE, "Pinning failed");
            default:
                setState(State.CANCELED, "Aborted by srmReleaseFile request.");
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "SURL is not yet pinned, pinning aborted");
            }
        } catch (IllegalStateTransition e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        } finally {
            wunlock();
        }

        LOGGER.debug("srmReleaseFile, unpinning fileId={} pinId={}", fileId, pinId);
        CheckedFuture<String, ? extends SRMException> future =
                getStorage().unPinFile(user, fileId, pinId);
        try {
            future.checkedGet(60, TimeUnit.SECONDS);
            setPinId(null);
            saveJob(true);
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } catch (TimeoutException e) {
            throw new SRMInternalErrorException("Operation timed out.");
        } catch (SRMException e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "Failed to unpin SURL: " + e.getMessage());
        }
    }

    @Override
    protected TReturnStatus getReturnStatus()
    {
        String description = latestHistoryEvent();
        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            if (statusCode == TStatusCode.SRM_FILE_PINNED ||
                    statusCode == TStatusCode.SRM_SUCCESS ||
                    statusCode == TStatusCode.SRM_RELEASED) {
                description = null;
            }
            return new TReturnStatus(statusCode, description);
        }

        switch (getState()) {
        case UNSCHEDULED:
        case QUEUED:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        case READY:
        case TRANSFERRING:
            return new TReturnStatus(TStatusCode.SRM_FILE_PINNED, null);
        case DONE:
            if (getPinId() != null) {
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            }  else {
                return new TReturnStatus(TStatusCode.SRM_RELEASED, null);
            }
        case CANCELED:
            return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
        case FAILED:
            return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
        default:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        }
    }

    /**
     *
     * @param newLifetime  new lifetime in millis
     *  -1 stands for infinite lifetime
     * @return int lifetime left in millis
     *    -1 stands for infinite lifetime
     */
    @Override
    public long extendLifetime(long newLifetime) throws SRMException {
        long remainingLifetime = getRemainingLifetime();
        if(remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        long requestLifetime = getContainerRequest().extendLifetimeMillis(newLifetime);
        if(requestLifetime <newLifetime) {
            newLifetime = requestLifetime;
        }
        if(remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }

        newLifetime = extendLifetimeMillis(newLifetime);
        if(remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        if(getPinId() == null) {
            return newLifetime;
        }
        SRMUser user = getUser();
        getStorage().extendPinLifetime(user,getFileId(), getPinId(),newLifetime);
        return newLifetime;
    }

    /**
     * @param fileId the fileId to set
     */
    private void setFileId(String fileId) {
        wlock();
        try {
            this.fileId = fileId;
        } finally {
            wunlock();
        }
    }

    private static class ThePinCallbacks implements Runnable
    {
        private final long fileRequestJobId;
        private final CheckedFuture<AbstractStorageElement.Pin, ? extends SRMException> future;

        public ThePinCallbacks(long fileRequestJobId,
                               CheckedFuture<AbstractStorageElement.Pin, ? extends SRMException> future)
        {
            this.fileRequestJobId = fileRequestJobId;
            this.future = future;
        }

        @Override
        public void run()
        {
            try {
                BringOnlineFileRequest fr = Job.getJob(fileRequestJobId, BringOnlineFileRequest.class);
                fr.wlock();
                try {
                    AbstractStorageElement.Pin pin = getPin();
                    LOGGER.debug("File pinned (pinId={}).", pin.pinId);

                    State state = fr.getState();
                    if (state == State.INPROGRESS) {
                        fr.setFileId(pin.fileMetaData.fileId);
                        fr.fileMetaData = pin.fileMetaData;
                        fr.setPinId(pin.pinId);
                        fr.setState(State.DONE, "File is pinned.");
                    }
                } catch (SRMInternalErrorException e) {
                    if (!fr.getState().isFinal()) {
                        fr.lastPinFailure = Optional.of(e.getMessage());
                        Scheduler.getScheduler(fr.getSchedulerId()).schedule(fr,
                                1, TimeUnit.SECONDS);
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            e.getMessage(),
                            e.getStatusCode());
                } finally {
                    fr.wunlock();
                }
            } catch (SRMInvalidRequestException e) {
                LOGGER.warn(e.getMessage());
            } catch (IllegalStateTransition e) {
                if (!e.getFromState().isFinal()) {
                    LOGGER.error(e.getMessage());
                }
            }
        }

        /* Workaround for a problem with the Java compiler. If inlined it gets confused
         * about which exceptions the statement can throw.
         */
        private AbstractStorageElement.Pin getPin() throws SRMException
        {
            return future.checkedGet();
        }
    }

    public static TReturnStatus unpinBySURLandRequestToken(
            AbstractStorageElement storage, SRMUser user, String requestToken, URI surl)
            throws SRMInternalErrorException
    {
        String fileId;
        try {
            fileId = storage.getFileMetaData(user, surl, true).fileId;
        } catch (SRMInternalErrorException e) {
            throw e;
        } catch (SRMAuthorizationException e) {
            return new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
        } catch (SRMInvalidPathException e) {
            return new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
        } catch (SRMException e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        }

        CheckedFuture<String, ? extends SRMException> future =
                storage.unPinFileBySrmRequestId(user, fileId, requestToken);
        try {
            future.checkedGet(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new SRMInternalErrorException("Operation timed out");
        } catch (SRMException e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "Failed to unpin: " + e.getMessage());
        }
        return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
    }

    public static TReturnStatus unpinBySURL(AbstractStorageElement storage, SRMUser user, URI surl)
            throws SRMInternalErrorException
    {
        String fileId;
        try {
            fileId = storage.getFileMetaData(user, surl, true).fileId;
        } catch (SRMInternalErrorException e) {
            throw e;
        } catch (SRMAuthorizationException e) {
            return new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
        } catch (SRMInvalidPathException e) {
            return new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
        } catch (SRMException e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        }

        CheckedFuture<String, ? extends SRMException> future = storage.unPinFile(user, fileId);
        try {
            future.checkedGet(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new SRMInternalErrorException("Operation timed out");
        } catch (SRMException e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "Failed to unpin: " + e.getMessage());
        }
        return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
    }
}
