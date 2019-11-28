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

package org.dcache.srm.request;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.client.RemoteTurlGetterV2;
import org.dcache.srm.client.RemoteTurlPutterV2;
import org.dcache.srm.client.RequestFailedEvent;
import org.dcache.srm.client.TURLsArrivedEvent;
import org.dcache.srm.client.TURLsGetFailedEvent;
import org.dcache.srm.client.Transport;
import org.dcache.srm.client.TurlGetterPutter;
import org.dcache.srm.qos.QOSPlugin;
import org.dcache.srm.qos.QOSPluginFactory;
import org.dcache.srm.qos.QOSTicket;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus;
import org.dcache.srm.v2_2.SrmCopyResponse;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TCopyRequestFileStatus;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkArgument;


/**
 *
 * @author  timur
 */
public final class CopyRequest extends ContainerRequest<CopyFileRequest>
        implements PropertyChangeListener, DelegatedCredentialAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyRequest.class);
    private static final String SFN_STRING = "?SFN=";

    private final Long credentialId;
    private boolean isSourceSrm;
    private boolean isDestinationSrm;
    private boolean isSourceLocal;
    private boolean isDestinationLocal;

    private final String[] protocols;
    private final TFileStorageType storageType;
    private final TRetentionPolicy targetRetentionPolicy;
    private final TAccessLatency targetAccessLatency;
    private final TOverwriteMode overwriteMode;

    private final transient String targetSpaceToken;
    private final transient Transport clientTransport;

    private final transient Multimap<String,Long> remoteSurlToFileReqIds = HashMultimap.create();
    private transient TurlGetterPutter remoteTurlClient;
    private final transient QOSPlugin qosPlugin;

    private volatile boolean processingDone;

    public CopyRequest(@Nonnull String srmId,
            SRMUser user,
            Long requestCredentialId,
            URI[] sourceUrl,
            URI[] destinationUrl,
            String spaceToken,
            long lifetime,
            long maxUpdatePeriod,
            TFileStorageType storageType,
            TRetentionPolicy targetRetentionPolicy,
            TAccessLatency targetAccessLatency,
            String description,
            String clientHost,
            TOverwriteMode overwriteMode,
            ImmutableMap<String,String> extraInfo)
    {
        super(srmId, user, maxUpdatePeriod, lifetime, description, clientHost,
              id -> {
                  checkArgument(sourceUrl.length == destinationUrl.length,
                                "unequal number of elements in url arrays");
                  ImmutableList.Builder<CopyFileRequest> requests = ImmutableList.builder();
                  for (int i = 0; i < sourceUrl.length; ++i) {
                      requests.add(new CopyFileRequest(id, requestCredentialId, sourceUrl[i], destinationUrl[i],
                                                       spaceToken, lifetime, extraInfo));
                  }
                  return requests.build();
              });

        ArrayList<String> allowedProtocols = new ArrayList<>(4);
        if (getConfiguration().isUseGsiftpForSrmCopy()) {
            allowedProtocols.add("gsiftp");
        }
        if (getConfiguration().isUseHttpForSrmCopy()) {
            allowedProtocols.add("http");
        }
        if (getConfiguration().isUseDcapForSrmCopy()) {
            allowedProtocols.add("dcap");
        }
        if (getConfiguration().isUseFtpForSrmCopy()) {
            allowedProtocols.add("ftp");
        }

        clientTransport = getConfiguration().getClientTransport();
        protocols = allowedProtocols.toArray(new String[allowedProtocols.size()]);
        this.qosPlugin = QOSPluginFactory.createInstance(SRM.getSRM());
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = overwriteMode;
        this.targetSpaceToken = spaceToken;
        this.credentialId = requestCredentialId;
        LOGGER.debug("Request.createCopyRequest : created new request succesfully");
    }

    /**
     * restore constructor
     */
    public CopyRequest(@Nonnull String srmId,
            long id,
            Long nextJobId,
            long creationTime,
            long lifetime,
            int stateId,
            SRMUser user,
            String scheduelerId,
            long schedulerTimeStamp,
            int numberOfRetries,
            long lastStateTransitionTime,
            JobHistory[] jobHistoryArray,
            Long credentialId,
            ImmutableList<CopyFileRequest> fileRequest,
            int retryDeltaTime,
            boolean should_updateretryDeltaTime,
            String description,
            String client_host,
            String statusCodeString,
            TFileStorageType storageType,
            TRetentionPolicy targetRetentionPolicy,
            TAccessLatency targetAccessLatency)
    {
        super(srmId, id, nextJobId, creationTime, lifetime, stateId,
                user, scheduelerId, schedulerTimeStamp, numberOfRetries,
                lastStateTransitionTime, jobHistoryArray,
                fileRequest, retryDeltaTime, should_updateretryDeltaTime,
                description, client_host, statusCodeString);

        ArrayList<String> allowedProtocols = new ArrayList<>(4);

        if (getConfiguration().isUseGsiftpForSrmCopy()) {
            allowedProtocols.add("gsiftp");
        }
        if (getConfiguration().isUseHttpForSrmCopy()) {
            allowedProtocols.add("http");
        }
        if (getConfiguration().isUseDcapForSrmCopy()) {
            allowedProtocols.add("dcap");
        }
        if (getConfiguration().isUseFtpForSrmCopy()) {
            allowedProtocols.add("ftp");
        }

        clientTransport = getConfiguration().getClientTransport();

        protocols = allowedProtocols.toArray(new String[allowedProtocols.size()]);
        this.qosPlugin = QOSPluginFactory.createInstance(SRM.getSRM());
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = null;
        this.credentialId = credentialId;
        targetSpaceToken = null;
     }

    @Override
    public Long getCredentialId()
    {
        return credentialId;
    }

    public void proccessRequest() throws DataAccessException, IOException,
            SRMException, InterruptedException, IllegalStateTransition
    {
        if (getNumOfFileRequest() == 0) {
            try {
                setState(State.FAILED, "Request contains zero file requests.");
                return;
            } catch (IllegalStateTransition ist) {
                LOGGER.error("Illegal State Transition : {}", ist.getMessage());
            }

        }
        LOGGER.debug("Processing request");
        identify();
        getTURLs();
    }

    private void identify() throws IOException, SRMException
    {
        wlock();
        try {
            URI source = getFileRequests().get(0).getSourceSurl();
            String sourceProtocol = source.getScheme();
            String sourceHost = source.getHost();
            int sourcePort = source.getPort();

            URI destination = getFileRequests().get(0).getDestinationSurl();
            String destinationProtocol = destination.getScheme();
            String destinationHost = destination.getHost();
            int destinationPort = destination.getPort();

            for (CopyFileRequest cfr : getFileRequests().subList(1, getNumOfFileRequest())) {
                URI sourceSurl = cfr.getSourceSurl();
                URI destinationSurl = cfr.getDestinationSurl();
                if (!sourceSurl.getScheme().equals(sourceProtocol) ||
                    !sourceSurl.getHost().equals(sourceHost) ||
                    sourceSurl.getPort() != sourcePort) {
                    String err = "Source URL " + sourceSurl + " is inconsistent with first source URL";
                    LOGGER.error(err);
                    throw new IOException(err);
                }

                if (!destinationSurl.getScheme().equals(destinationProtocol) ||
                    !destinationSurl.getHost().equals(destinationHost) ||
                    destinationSurl.getPort() != destinationPort) {
                    String err = "Destination URL " + destinationSurl +
                                 " is inconsistent with first destination URL";
                    LOGGER.error(err);
                    throw new IOException(err);
                }
            }

            isSourceSrm = sourceProtocol.equals("srm");
            isDestinationSrm = destinationProtocol.equals("srm");

            AbstractStorageElement storage = SRM.getSRM().getStorage();
            isSourceLocal = storage.isLocalSurl(source);
            isDestinationLocal = storage.isLocalSurl(destination);

            LOGGER.debug("src (srm={}, local={}), dest (srm={}, local={})",
                      isSourceSrm, isSourceLocal, isDestinationSrm, isDestinationLocal);

            if (!isSourceLocal && !isDestinationLocal) {
                LOGGER.error("Both source ({}) and destination ({}) URLs are remote.", source, destination);
                throw new SRMInvalidRequestException("Both source and destination URLs are remote.");
            }
        } finally {
            wunlock();
        }
    }

    private void makeQosReservation(int fileIndex) throws MalformedURLException,
            SRMException
    {
        try {
            CopyFileRequest cfr = getFileRequests().get(fileIndex);
            RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
            QOSTicket qosTicket = getQosPlugin().createTicket(
                    credential.getCredentialName(),
                    getStorage().getFileMetaData(getUser(), cfr.getSourceSurl(), false).size,
                    cfr.getSourceSurl().toASCIIString(),
                    cfr.getSourceSurl().getPort(),
                    cfr.getSourceSurl().getPort(),
                    cfr.getSourceSurl().getScheme(),
                    cfr.getDestinationSurl().toASCIIString(),
                    cfr.getDestinationSurl().getPort(),
                    cfr.getDestinationSurl().getPort(),
                    cfr.getDestinationSurl().getScheme());
            getQosPlugin().addTicket(qosTicket);
            if (getQosPlugin().submit()) {
                cfr.setQOSTicket(qosTicket);
                LOGGER.debug("QOS Ticket Received {}", getQosPlugin());
            }
        } catch (Exception e) {
            LOGGER.error("Could not create QOS reservation: {}", e.getMessage());
        }
    }

    private void getTURLs() throws SRMException, IOException,
            InterruptedException, IllegalStateTransition, DataAccessException
    {
        if (isSourceSrm() && !isSourceLocal()) { // implying destination is local
            if (getStorageType() != null && !storageType.equals(TFileStorageType.PERMANENT)) {
                  throw new SRMNotSupportedException("TargetFileStorageType " + getStorageType() + " is not supported");
            }
            RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
            LOGGER.debug("obtained credential={} id={}", credential, credential.getId());

            for (int i = 0; i < getNumOfFileRequest(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if (cfr.getState() == State.UNSCHEDULED && cfr.getSchedulerId() == null) {
                    if (cfr.getSourceTurl() != null) {
                        cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
                    } else {
                        // Since source SURLs are local, we can just set the path
                        remoteSurlToFileReqIds.put(cfr.getSourceSurl().toASCIIString(), cfr.getId());
                        String path = localPathFromSurl(cfr.getDestinationSurl());
                        LOGGER.debug("setting destination path to {}", path);
                        cfr.setLocalDestinationPath(path);
                        cfr.saveJob();
                    }
                }
            }
            String[] remoteSurlsUniqueArray = remoteSurlToFileReqIds.keySet()
                            .toArray(new String[remoteSurlToFileReqIds.size()]);
            if (LOGGER.isDebugEnabled()) {
                for (int i = 0; i < remoteSurlsUniqueArray.length; ++i) {
                    LOGGER.debug("remoteSurlsUniqueArray[{}]={}", i,
                            remoteSurlsUniqueArray[i]);
                }
            }
            // need to fetch files from remote SRM system
            setRemoteTurlClient(new RemoteTurlGetterV2(getStorage(),
                    credential, remoteSurlsUniqueArray, getProtocols(),
                    this, getConfiguration().getCopyMaxPollPeriod(), 2,
                    this.getRemainingLifetime(), getConfiguration().getCaCertificatePath(),
                    clientTransport));
            getRemoteTurlClient().getInitialRequest();
            getRemoteTurlClient().run();
            return;
        }

        if (isSourceSrm()) { // source is this storage system
            for (int i = 0; i < getNumOfFileRequest(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);

                if (cfr.getState() == State.UNSCHEDULED && cfr.getSchedulerId() == null &&
                        cfr.getLocalSourcePath() == null) {
                    String path = localPathFromSurl(cfr.getSourceSurl());
                    LOGGER.debug("setting source path to {}", path);
                    cfr.setLocalSourcePath(path);
                    cfr.saveJob();
                }
            }
        } else { // source is not SRM, so supplied values are the TURL(s)
            for (int i = 0; i < getNumOfFileRequest(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);

                if (cfr.getState() == State.UNSCHEDULED && cfr.getSchedulerId() == null &&
                        cfr.getSourceTurl() == null) {
                    LOGGER.debug("getTurlsArrived, setting \"from\" turl to {}", cfr.getSourceSurl());
                    cfr.setSourceTurl(cfr.getSourceSurl());
                    cfr.saveJob();
                }
            }
        }

        // now source is known, either as a TURL or a local path.

        if (isDestinationSrm() && isDestinationLocal()) { // destination is this system
            // As we have a source (either a path or a TURL) for all files and
            // the destination is local, we have all the information needed.
            for (int i = 0; i < getNumOfFileRequest(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if (cfr.getState() == State.UNSCHEDULED && cfr.getSchedulerId() == null) {
                    String path = localPathFromSurl(cfr.getDestinationSurl());
                    LOGGER.debug("setting local destination path to {}", path);
                    cfr.setLocalDestinationPath(path);
                    cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
                }
            }
            return;
        }

        if (!isDestinationSrm()) { // destination is some TURL
            // As we have a source (either a path or a TURL) for all files and
            // the destination is a TURL, we have all the information needed.
            for (int i = 0; i < getNumOfFileRequest(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if (cfr.getState() == State.UNSCHEDULED && cfr.getSchedulerId() == null) {
                    LOGGER.debug("setting destination to {}", cfr.getDestinationSurl());
                    cfr.setDestinationTurl(cfr.getDestinationSurl());
                    cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
                }
            }
            return;
        }

        // The only possibility left is a remote destination SURLs, for which
        // we need to discover a TURLs.

        for (int i = 0; i < getNumOfFileRequest(); ++i) {
            CopyFileRequest cfr = getFileRequests().get(i);
            if (cfr.getState() != State.UNSCHEDULED || cfr.getSchedulerId() != null) {
                // copy file request has being canceled,failed or scheduled before
            } else if (cfr.getDestinationTurl() != null) {
                //destination TURL has arrived, but request has not been scheduled
                cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
            } else {
                remoteSurlToFileReqIds.put(cfr.getDestinationSurl().toASCIIString(), cfr.getId());
            }
        }

        int uniqueCount = remoteSurlToFileReqIds.size();
        String[] uniqueSurls = remoteSurlToFileReqIds.keySet().toArray(new String[uniqueCount]);
        String[] destinationSurls = new String[uniqueCount];
        long[] sizes = new long[uniqueCount];
        for (int i = 0; i < uniqueCount; ++i) {
            long id = Iterables.get(remoteSurlToFileReqIds.get(uniqueSurls[i]), 0);
            CopyFileRequest cfr = getFileRequest(id);
            sizes[i] = getStorage().getFileMetaData(getUser(), cfr.getSourceSurl(), false).size;
            LOGGER.debug("local size is {}", sizes[i]);
            cfr.setSize(sizes[i]);
            destinationSurls[i] = cfr.getDestinationSurl().toString();
            if (getQosPlugin() != null) {
                makeQosReservation(i);
            }
        }

        // Now create an SRM client to fetch a TURL for each SURL.

        RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
        setRemoteTurlClient(new RemoteTurlPutterV2(getStorage(),
                                                   credential, destinationSurls, sizes, getProtocols(), this,
                                                   getConfiguration().getCopyMaxPollPeriod(), 2,
                                                   this.getRemainingLifetime(), getStorageType(),
                                                   getTargetRetentionPolicy(), getTargetAccessLatency(),
                                                   getOverwriteMode(), getTargetSpaceToken(),
                                                   getConfiguration().getCaCertificatePath(),
                                                   clientTransport));
        getRemoteTurlClient().getInitialRequest();

        getRemoteTurlClient().run();
    }

    public void turlArrived(String surl, String turl, String remoteRequestId,
            String remoteFileId, Long size)
    {
        Collection<Long> fileRequestIds;
        synchronized (remoteSurlToFileReqIds) {
            fileRequestIds = remoteSurlToFileReqIds.removeAll(surl);
        }
        if (fileRequestIds.isEmpty()) {
            LOGGER.error("turlArrived for unknown SURL = {} !!!!!!!", surl);
        } else {
            for (long id : fileRequestIds) {
                CopyFileRequest cfr = getFileRequest(id);
                if (getQosPlugin() != null && cfr.getQOSTicket() != null) {
                    getQosPlugin().sayStatus(cfr.getQOSTicket());
                }

                if (isSourceSrm() && !isSourceLocal()) {
                    cfr.setSourceTurl(URI.create(turl));
                } else {
                    cfr.setDestinationTurl(URI.create(turl));
                }
                if (size != null) {
                    cfr.setSize(size);
                }
                cfr.setRemoteRequestId(remoteRequestId);
                cfr.setRemoteFileId(remoteFileId);
                cfr.saveJob();

                try {
                    String theSchedulerId = getSchedulerId();
                    State state = cfr.getState();
                    if (theSchedulerId != null && !state.isFinal()) {
                        cfr.scheduleWith(Scheduler.getScheduler(theSchedulerId));
                    }
                } catch (IllegalStateException | IllegalArgumentException |
                        IllegalStateTransition e) {
                    LOGGER.error("failed to schedule CopyFileRequest {}: {}", cfr, e.toString());
                    try {
                        cfr.setState(State.FAILED, "Failed to schedule request: " + e.getMessage());
                    } catch (IllegalStateTransition ist) {
                        LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                    }
                }
            }
        }
    }

    public void turlRetrievalFailed(String surl, String reason,
            String remoteRequestId, String remoteFileId)
    {
        Collection<Long> fileRequestSet;
        synchronized (remoteSurlToFileReqIds) {
            fileRequestSet = remoteSurlToFileReqIds.removeAll(surl);
        }
        if (fileRequestSet.isEmpty()) {
            LOGGER.error("turlArrived for unknown SURL = {}", surl);
        } else {
            for (long id : fileRequestSet) {
                CopyFileRequest cfr = getFileRequest(id);
                try {
                    String type = isSourceSrm() && !isSourceLocal() ? "source" : "destination";
                    String error = "retrieval of " + type + " TURL failed with error " + reason;
                    LOGGER.error(error);
                    cfr.setState(State.FAILED, error);
                } catch (IllegalStateTransition ist) {
                    LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                }
            }
        }
        remoteFileRequestDone(surl, remoteRequestId, remoteFileId);
    }

    public void turlsRetrievalFailed(Object reason)
    {
        ArrayList<Map.Entry<String, Long>> entries;
        synchronized (remoteSurlToFileReqIds) {
            entries = new ArrayList<>(remoteSurlToFileReqIds.entries());
            remoteSurlToFileReqIds.clear();
        }
        for (Map.Entry<String, Long> entry : entries) {
            long id = entry.getValue();
            CopyFileRequest cfr = getFileRequest(id);
            try {
                String type = isSourceSrm() && !isSourceLocal() ? "source" : "destination";
                String error = "retrieval of " + type + " TURL failed with error " + reason;
                LOGGER.error(error);
                cfr.setState(State.FAILED, error);
            } catch (IllegalStateTransition ist) {
                LOGGER.error("Illegal State Transition : {}", ist.getMessage());
            }
        }
    }

    public void remoteFileRequestDone(String surl, String requestId, String fileId)
    {
        try {
            RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
            String caCertificatePath = getConfiguration().getCaCertificatePath();
            if (isSourceSrm() && !isSourceLocal()) {
               RemoteTurlGetterV2.staticReleaseFile(credential,
                                                    surl, requestId,
                                                    0,
                                                    0,
                                                    caCertificatePath,
                                                    clientTransport);
            } else {
                RemoteTurlPutterV2.staticPutDone(credential,
                                                 surl, requestId,
                                                 0,
                                                 0,
                                                 caCertificatePath,
                                                 clientTransport);
            }
        } catch (Exception e) {
            LOGGER.error("set remote file status to done failed, surl={}, " +
                     "requestId={}, fileId={}", surl, requestId, fileId);
        }
    }

    private CopyFileRequest getFileRequestBySurls(String source, String destination)
            throws SRMInvalidRequestException, SRMInvalidPathException
    {
        for (CopyFileRequest request : getFileRequests()) {
            if (request.getSourceSurl().toString().equals(source) &&
                    request.getDestinationSurl().toString().equals(destination)) {
                return request;
            }
        }
        throw new SRMInvalidPathException("request not found");
    }


    @Override
    public void run() throws SRMException, IllegalStateTransition
    {
        if (!getState().isFinal() && !isProcessingDone()) {
            try {
                proccessRequest();
                boolean hasOnlyFinalFileRequests = true;
                for (CopyFileRequest request : getFileRequests()) {
                    State state = request.getState();
                    if (!state.isFinal()) {
                        hasOnlyFinalFileRequests = false;
                    }
                }

                setProcessingDone(true);
                if (hasOnlyFinalFileRequests) {
                    setState(State.DONE, "All transfers completed.");
                } else {
                    addHistoryEvent("Waiting for transfers to complete.");
                }
            } catch (IOException e) {
                throw new SRMException(e.getMessage(), e);
            } catch (InterruptedException e) {
                throw new SRMException("shutting down.", e);
            }
        }
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        if (newState.isFinal()) {
            TurlGetterPutter client = getRemoteTurlClient();
            if (client != null) {
                LOGGER.debug("copyRequest TURL-fetching client is non null, stopping");
                client.stop();
            }
            LOGGER.debug("copy request state changed to {}", newState);
            for (CopyFileRequest request : getFileRequests()) {
                try {
                    State frState = request.getState();
                    if (!frState.isFinal()) {
                        LOGGER.debug("changing fr#{} to {}", request.getId(), newState);
                        request.setState(newState, "Request changed: " + description);
                    }
                } catch (IllegalStateTransition ist) {
                    LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                }
            }
        }

        super.processStateChange(newState, description);
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt)
    {
        LOGGER.debug("propertyChange");
        try {
            if (evt instanceof TURLsArrivedEvent) {
                TURLsArrivedEvent tae = (TURLsArrivedEvent) evt;
                String SURL = tae.getSURL();
                String TURL = tae.getTURL();
                String remoteRequestId = tae.getRequestId();
                String remoteFileId = tae.getFileRequestId();
                Long size = tae.getSize();
                turlArrived(SURL, TURL, remoteRequestId,remoteFileId, size);
            } else if (evt instanceof TURLsGetFailedEvent) {
                TURLsGetFailedEvent tgfe = (TURLsGetFailedEvent)evt;
                String SURL = tgfe.getSURL();
                String reason = tgfe.getReason();
                String remoteRequestId = tgfe.getRequestId();
                String remoteFileId = tgfe.getFileRequestId();
                turlRetrievalFailed(SURL, reason, remoteRequestId, remoteFileId);
            } else if (evt instanceof RequestFailedEvent) {
                RequestFailedEvent rfe = (RequestFailedEvent)evt;
                Object reason = rfe.getReason();
                turlsRetrievalFailed(reason);
            }
        } catch (Exception e) {
            LOGGER.error(e.toString());
        }
    }

    public void fileRequestCompleted()
    {
        resetRetryDeltaTime();

        if (isProcessingDone()) {
            try {
                boolean hasOnlyFinalFileRequests = true;
                for (CopyFileRequest request : getFileRequests()) {
                    State state = request.getState();
                    if (!(state.isFinal())) {
                        hasOnlyFinalFileRequests = false;
                    }
                }

                State state = getState();
                if (!state.isFinal() && hasOnlyFinalFileRequests) {
                    setState(State.DONE, "All transfers have completed.");
                }
            } catch (IllegalStateTransition e) {
                LOGGER.error("setting to done anyway: {}", e.toString());
                try {
                    State state = getState();
                    if (!state.isFinal()) {
                        setState(State.DONE, e.toString());
                    }
                } catch (IllegalStateTransition ist) {
                    LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                }
            }
        }
    }

    public final SrmCopyResponse getSrmCopyResponse() throws SRMInvalidRequestException
    {
        SrmCopyResponse response = new SrmCopyResponse();
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());
        response.setArrayOfFileStatuses(new ArrayOfTCopyRequestFileStatus(getArrayOfTCopyRequestFileStatuses()));
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));
        return response;
    }

    private String getTRequestToken()
    {
        return String.valueOf(getId());
    }

    public final TCopyRequestFileStatus[] getArrayOfTCopyRequestFileStatuses()
            throws SRMInvalidRequestException
    {
        List<CopyFileRequest> fileRequests = getFileRequests();
        int len = fileRequests.size();
        TCopyRequestFileStatus[] copyRequestFileStatuses = new TCopyRequestFileStatus[len];
        for (int i = 0; i < len; ++i) {
            copyRequestFileStatuses[i] = fileRequests.get(i).getTCopyRequestFileStatus();
        }
        return copyRequestFileStatuses;
    }

    public final TCopyRequestFileStatus[] getArrayOfTCopyRequestFileStatuses(
            org.apache.axis.types.URI[] fromurls, org.apache.axis.types.URI[] tourls)
            throws SRMInvalidRequestException
    {
        if (fromurls == null && tourls == null) {
            return getArrayOfTCopyRequestFileStatuses();
        }
        checkArgument(fromurls != null && tourls != null && fromurls.length == tourls.length);
        int len = fromurls.length;
        TCopyRequestFileStatus[] copyRequestFileStatuses = new TCopyRequestFileStatus[len];
        for (int i = 0; i < len; ++i) {
            try {
                copyRequestFileStatuses[i] = getFileRequestBySurls(fromurls[i].toString(), tourls[i].toString()).getTCopyRequestFileStatus();
            } catch (SRMInvalidPathException e) {
                copyRequestFileStatuses[i] = new TCopyRequestFileStatus(fromurls[i], tourls[i],
                        new TReturnStatus(TStatusCode.SRM_INVALID_PATH, "No such file request"), null, null, null);
            }
        }
        return copyRequestFileStatuses;
    }

    public final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest()
            throws SRMInvalidRequestException
    {
        return getSrmStatusOfCopyRequest(null,null);
    }

    public final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest(org.apache.axis.types.URI[] fromurls, org.apache.axis.types.URI[] tourls)
            throws SRMInvalidRequestException
    {
        SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
        response.setReturnStatus(getTReturnStatus());
        TCopyRequestFileStatus[] fileStatuses = getArrayOfTCopyRequestFileStatuses(fromurls, tourls);
        response.setArrayOfFileStatuses(new ArrayOfTCopyRequestFileStatus(fileStatuses));
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));
        return response;
    }

    @Nonnull
    @Override
    public final CopyFileRequest getFileRequestBySurl(URI surl) throws SRMFileRequestNotFoundException
    {
        for (CopyFileRequest request : getFileRequests()) {
            if (request.getSourceSurl().equals(surl) || request.getDestinationSurl().equals(surl)) {
                return request;
            }
        }
        throw new SRMFileRequestNotFoundException("file request for url=" + surl + " is not found");
    }

    @Override
    public TRequestType getRequestType()
    {
        rlock();
        try {
            return TRequestType.COPY;
        } finally {
            runlock();
        }
    }

    public TFileStorageType getStorageType()
    {
        rlock();
        try {
            return storageType;
        } finally {
            runlock();
        }
    }

    public TRetentionPolicy getTargetRetentionPolicy()
    {
        return targetRetentionPolicy;
    }

    public TAccessLatency getTargetAccessLatency()
    {
        return targetAccessLatency;
    }

    public TOverwriteMode getOverwriteMode()
    {
        return overwriteMode;
    }

    public boolean isOverwrite()
    {
        if (getConfiguration().isOverwrite()) {
            if (getOverwriteMode() == null) {
                return getConfiguration().isOverwrite_by_default();
            }
            return getOverwriteMode().equals(TOverwriteMode.ALWAYS);
        }
        return false;
    }

    @Override
    public long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException
    {
        /* [ SRM 2.2, 5.16.2 ]
         *
         * h) Lifetime cannot be extended on the released files, aborted files, expired
         *    files, and suspended files. For example, pin lifetime cannot be extended
         *    after srmPutDone is requested on SURLs for srmPrepareToPut request. In
         *    such case, SRM_INVALID_REQUEST at the file level must be returned, and
         *    SRM_PARTIAL_SUCCESS or SRM_FAILURE must be returned at the request level.
         */
        try {
            return super.extendLifetimeMillis(newLifetimeInMillis);
        } catch (SRMReleasedException releasedException) {
            throw new SRMInvalidRequestException(releasedException.getMessage());
        }
    }

    /**
     * @return true if the source SURL starts 'srm://'
     */
    private boolean isSourceSrm()
    {
        rlock();
        try {
            return isSourceSrm;
        } finally {
            runlock();
        }
    }

    /**
     * @return true if the destination SURL starts 'srm://'
     */
    private boolean isDestinationSrm()
    {
        rlock();
        try {
            return isDestinationSrm;
        } finally {
            runlock();
        }
    }

    /**
     * @return true if the source is this storage system
     */
    private boolean isSourceLocal()
    {
        rlock();
        try {
            return isSourceLocal;
        } finally {
            runlock();
        }
    }

    /**
     * @return true if the destination is this storage system
     */
    private boolean isDestinationLocal()
    {
        rlock();
        try {
            return isDestinationLocal;
        } finally {
            runlock();
        }
    }

    /**
     * @return the protocols
     */
    private String[] getProtocols()
    {
        rlock();
        try {
            return protocols;
        } finally {
            runlock();
        }
    }

    /**
     * @return the targetSpaceToken
     */
    private String getTargetSpaceToken()
    {
        rlock();
        try {
            return targetSpaceToken;
        } finally {
            runlock();
        }
    }

    /**
     * @return the getter_putter
     */
    private TurlGetterPutter getRemoteTurlClient()
    {
        rlock();
        try {
            return remoteTurlClient;
        } finally {
            runlock();
        }
    }

    /**
     * @param client the getter_putter to set
     */
    private void setRemoteTurlClient(TurlGetterPutter client)
    {
        wlock();
        try {
            this.remoteTurlClient = client;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the qosPlugin
     */
    private QOSPlugin getQosPlugin()
    {
        rlock();
        try {
            return qosPlugin;
        } finally {
            runlock();
        }
    }

    /**
     * @return the processingDone
     */
    private boolean isProcessingDone()
    {
        rlock();
        try {
            return processingDone;
        } finally {
            runlock();
        }
    }

    /**
     * @param processingDone the processingDone to set
     */
    private void setProcessingDone(boolean processingDone)
    {
        wlock();
        try {
            this.processingDone = processingDone;
        } finally {
            wunlock();
        }
    }

    private static String localPathFromSurl(URI surl)
    {
        String path = surl.getPath();
        int index = path.indexOf(SFN_STRING);
        if (index != -1) {
            path = path.substring(index+SFN_STRING.length());
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }

    @Override
    public String getNameForRequestType() {
        return "Copy";
    }
}
