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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMProtocol;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.client.RemoteTurlGetterV1;
import org.dcache.srm.client.RemoteTurlGetterV2;
import org.dcache.srm.client.RemoteTurlPutterV1;
import org.dcache.srm.client.RemoteTurlPutterV2;
import org.dcache.srm.client.RequestFailedEvent;
import org.dcache.srm.client.TURLsArrivedEvent;
import org.dcache.srm.client.TURLsGetFailedEvent;
import org.dcache.srm.client.Transport;
import org.dcache.srm.client.TurlGetterPutter;
import org.dcache.srm.client.TurlGetterPutterV1;
import org.dcache.srm.qos.QOSPlugin;
import org.dcache.srm.qos.QOSPluginFactory;
import org.dcache.srm.qos.QOSTicket;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.SrmUrl;
import org.dcache.srm.util.Tools;
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
import static com.google.common.base.Preconditions.checkNotNull;


/**
 *
 * @author  timur
 */
public final class CopyRequest extends ContainerRequest<CopyFileRequest>
        implements PropertyChangeListener
{
    private final static Logger LOG = LoggerFactory.getLogger(CopyRequest.class);
    private final static String SFN_STRING = "?SFN=";

    private boolean isSourceSrm;
    private boolean isDestinationSrm;
    private boolean isSourceGsiftp;
    private boolean isDestinationGsiftp;
    private boolean isSourceLocal;
    private boolean isDestinationLocal;
    private SrmUrl sourceUrl[];
    private SrmUrl destinationUrls[];
    private int fileCount;

    private String[] protocols;
    private SRMProtocol callerSrmProtocol;
    private SRMProtocol remoteSrmProtocol;
    private TFileStorageType storageType;
    private final TRetentionPolicy targetRetentionPolicy;
    private final TAccessLatency targetAccessLatency;
    private final TOverwriteMode overwriteMode;
    private String targetSpaceToken;

    private final Transport clientTransport;

    private transient final Multimap<String,Long> remoteSurlToFileReqIds =
            HashMultimap.create();
    private transient TurlGetterPutter remoteTurlClient;
    private transient QOSPlugin qosPlugin;

    private volatile boolean processingDone;

    public CopyRequest(SRMUser user,
            Long requestCredentialId,
            URI[] sourceUrl,
            URI[] destinationUrl,
            String spaceToken,
            long lifetime,
            long maxUpdatePeriod,
            int maxNumberOfRetries,
            SRMProtocol callerProtocolVersion,
            TFileStorageType storageType,
            TRetentionPolicy targetRetentionPolicy,
            TAccessLatency targetAccessLatency,
            String description,
            String clientHost,
            TOverwriteMode overwriteMode)
    {
        super(user, requestCredentialId, maxNumberOfRetries, maxUpdatePeriod,
                lifetime, description, clientHost);

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
        int requestCount = sourceUrl.length;
        checkArgument(requestCount == destinationUrl.length,
                "unequal number of elements in url arrays");
        List<CopyFileRequest> requests = Lists.newArrayListWithCapacity(requestCount);
        for (int i = 0; i < requestCount; ++i) {
            CopyFileRequest request = new CopyFileRequest(getId(),
                    requestCredentialId, sourceUrl[i], destinationUrl[i], spaceToken,
                    lifetime, maxNumberOfRetries);
            requests.add(request);
        }
        setFileRequests(requests);
        this.callerSrmProtocol = checkNotNull(callerProtocolVersion);
        if (getConfiguration().getQosPluginClass() != null) {
            this.qosPlugin = QOSPluginFactory.createInstance(SRM.getSRM());
        }
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = overwriteMode;
        this.targetSpaceToken = spaceToken;
        LOG.debug("Request.createCopyRequest : created new request succesfully");
    }

    /**
     * restore constructor
     */
    public CopyRequest(long id,
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
            Long credentialId,
            CopyFileRequest[] fileRequest,
            int retryDeltaTime,
            boolean should_updateretryDeltaTime,
            String description,
            String client_host,
            String statusCodeString,
            TFileStorageType storageType,
            TRetentionPolicy targetRetentionPolicy,
            TAccessLatency targetAccessLatency)
    {
        super(id, nextJobId, creationTime, lifetime, stateId, errorMessage,
                user, scheduelerId, schedulerTimeStamp, numberOfRetries,
                maxNumberOfRetries, lastStateTransitionTime, jobHistoryArray,
                credentialId, fileRequest, retryDeltaTime, should_updateretryDeltaTime,
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
        if (getConfiguration().getQosPluginClass() != null) {
            this.qosPlugin = QOSPluginFactory.createInstance(SRM.getSRM());
        }
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = null;
     }


    public void proccessRequest() throws DataAccessException, IOException,
            SRMException, InterruptedException, IllegalStateTransition,
            FatalJobFailure
    {
        LOG.debug("Proccessing request");
        if (getNumOfFileRequest() == 0) {
            try {
                setState(State.FAILED, "Request contains zero file requests.");
                return;
            } catch (IllegalStateTransition ist) {
                LOG.error("Illegal State Transition : {}", ist.getMessage());
            }

        }
        setFileCount(getNumOfFileRequest());
        LOG.debug("file count = {}", getFileCount());
        wlock();
        try {
            List<CopyFileRequest> requests = getFileRequests();
            sourceUrl = new SrmUrl[getFileCount()];
            destinationUrls = new SrmUrl[getFileCount()];
            for (int i = 0; i < getFileCount(); ++i) {
                CopyFileRequest cfr = requests.get(i);
                sourceUrl[i] = new SrmUrl(cfr.getSourceSurl());
                destinationUrls[i] = new SrmUrl(cfr.getDestinationSurl());
            }
        } catch (MalformedURLException murle) {
            LOG.error(murle.toString());
            try {
                setState(State.FAILED, murle.toString());
            } catch (IllegalStateTransition ist) {
                LOG.error("Illegal State Transition : {}", ist.getMessage());
            }
            return;
        } finally {
            wunlock();
        }
        identify();
        getTURLs();
    }

    private void identify() throws IOException, SRMException
    {
        wlock();
        try {
            String sourceProtocol = sourceUrl[0].getProtocol();
            String sourceHost = sourceUrl[0].getHost();
            int sourcePort = sourceUrl[0].getPort();

            String destinationProtocol = destinationUrls[0].getProtocol();
            String destinationHost = destinationUrls[0].getHost();
            int destinationPort = destinationUrls[0].getPort();

            for (int i = 1; i < getFileCount(); ++i) {
                if (!sourceUrl[i].getProtocol().equals(sourceProtocol) ||
                        !sourceUrl[i].getHost().equals(sourceHost) ||
                        sourceUrl[i].getPort() != sourcePort) {
                    String err = "Source URL " + sourceUrl[i].getURL() +
                            " is inconsistent with first source URL";
                    LOG.error(err);
                    throw new IOException(err);
                }

                if (!destinationUrls[i].getProtocol().equals(destinationProtocol) ||
                        !destinationUrls[i].getHost().equals(destinationHost) ||
                        destinationUrls[i].getPort() != destinationPort) {
                    String err = "Destination URL " + destinationUrls[i].getURL() +
                            " is inconsistent with first destination URL";
                    LOG.error(err);
                    throw new IOException(err);
                }
            }

            isSourceSrm = sourceProtocol.equals("srm");
            isDestinationSrm = destinationProtocol.equals("srm");
            isSourceGsiftp = sourceProtocol.equals("gsiftp");
            isDestinationGsiftp = sourceProtocol.equals("gsiftp");

            if (isSourceSrm) {
                isSourceLocal = sourcePort == getConfiguration().getPort() &&
                        Tools.sameHost(getConfiguration().getSrmHosts(), sourceHost);
            } else {
                isSourceLocal = getStorage().isLocalTransferUrl(sourceUrl[0].getURI());
            }

            if (isDestinationSrm) {
                isDestinationLocal = destinationPort == getConfiguration().getPort() &&
                        Tools.sameHost(getConfiguration().getSrmHosts(), destinationHost);
            } else {
                isDestinationLocal = getStorage().isLocalTransferUrl(
                            destinationUrls[0].getURI());
            }

            LOG.debug("src (srm={}, gsiftp={}, local={}), " +
                    "dest (srm={}, gsiftp={}, local={})",
                    isSourceSrm, isSourceGsiftp, isSourceLocal,
                    isDestinationSrm, isDestinationGsiftp, isDestinationLocal);

            if (!isSourceLocal && !isDestinationLocal) {
                LOG.error("Both source and destination URLs are remote");
                throw new IOException("Both source and destination URLs are remote");
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
            RequestCredential credential = getCredential();
            QOSTicket qosTicket = getQosPlugin().createTicket(
                    credential.getCredentialName(),
                    getStorage().getFileMetaData(getUser(), cfr.getSourceSurl(), false).size,
                    getSourceUrl(fileIndex).getURL(),
                    getSourceUrl(fileIndex).getPort(),
                    getSourceUrl(fileIndex).getPort(),
                    getSourceUrl(fileIndex).getProtocol(),
                    getDestinationUrl(fileIndex).getURL(),
                    getDestinationUrl(fileIndex).getPort(),
                    getDestinationUrl(fileIndex).getPort(),
                    getDestinationUrl(fileIndex).getProtocol());
            getQosPlugin().addTicket(qosTicket);
            if (getQosPlugin().submit()) {
                cfr.setQOSTicket(qosTicket);
                LOG.debug("QOS Ticket Received {}", getQosPlugin());
            }
        } catch (Exception e) {
            LOG.error("Could not create QOS reservation: {}", e.getMessage());
        }
    }

    private void getTURLs() throws SRMException, IOException,
            InterruptedException,IllegalStateTransition, DataAccessException,
            FatalJobFailure
    {
        if (isSourceSrm() && !isSourceLocal()) { // implying destination is local
            if (getStorageType() != null && !storageType.equals(TFileStorageType.PERMANENT)) {
                  throw new FatalJobFailure("TargetFileStorageType " +
                          getStorageType() + " is not supported");
            }
            RequestCredential credential = getCredential();
            LOG.debug("obtained credential={} id={}", credential, credential.getId());

            for (int i = 0; i < getFileCount(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if (cfr.getState() == State.PENDING && cfr.getSchedulerId() == null) {
                    if (cfr.getSourceTurl() != null) {
                        cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
                    } else {
                        // Since source SURLs are local, we can just set the path
                        remoteSurlToFileReqIds.put(getSourceUrl(i).getURL(), cfr.getId());
                        String path = localPathFromSurl(cfr.getDestinationSurl());
                        LOG.debug("setting destination path to {}", path);
                        cfr.setLocalDestinationPath(path);
                        cfr.saveJob();
                    }
                }
            }
            String[] remoteSurlsUniqueArray = remoteSurlToFileReqIds.keySet()
                            .toArray(new String[remoteSurlToFileReqIds.size()]);
            if (LOG.isDebugEnabled()) {
                for (int i = 0; i < remoteSurlsUniqueArray.length; ++i) {
                    LOG.debug("remoteSurlsUniqueArray[{}]={}", i,
                            remoteSurlsUniqueArray[i]);
                }
            }
            // need to fetch files from remote SRM system

            if (getCallerSrmProtocol() == null || getCallerSrmProtocol() == SRMProtocol.V1_1) {
                try {
                    setRemoteTurlClient(new RemoteTurlGetterV1(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            clientTransport));
                    getRemoteTurlClient().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V1_1);
                 } catch (SRMException e) {
                    LOG.error("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                    setRemoteTurlClient(new RemoteTurlGetterV2(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            this.getRemainingLifetime(), clientTransport));
                    getRemoteTurlClient().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V2_1);
                 }
            } else if (getCallerSrmProtocol() == SRMProtocol.V2_1) {
                try {
                    setRemoteTurlClient(new RemoteTurlGetterV2(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            this.getRemainingLifetime(), clientTransport));
                    getRemoteTurlClient().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V2_1);
                } catch (SRMException e) {
                    LOG.error("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                    setRemoteTurlClient(new RemoteTurlGetterV1(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            clientTransport));
                    getRemoteTurlClient().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V1_1);
                }
            } else {
                throw new FatalJobFailure("unsupported SRM protocol");
            }
            getRemoteTurlClient().run();
            return;
        }

        if (isSourceSrm()) { // source is this storage system
            for (int i = 0; i < this.getFileCount(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);

                if (cfr.getState() == State.PENDING && cfr.getSchedulerId() == null &&
                        cfr.getLocalSourcePath() == null) {
                    String path = localPathFromSurl(cfr.getSourceSurl());
                    LOG.debug("setting source path to {}", path);
                    cfr.setLocalSourcePath(path);
                    cfr.saveJob();
                }
            }
        } else { // source is not SRM, so supplied values are the TURL(s)
            for (int i = 0; i < this.getFileCount(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);

                if (cfr.getState() == State.PENDING && cfr.getSchedulerId() == null &&
                        cfr.getSourceTurl() == null) {
                    LOG.debug("getTurlsArrived, setting \"from\" turl to {}",
                            getSourceUrl(i).getURL());
                    cfr.setSourceTurl(getSourceUrl(i).getURI());
                    cfr.saveJob();
                }
            }
        }

        // now source is known, either as a TURL or a local path.

        if (isDestinationSrm() && isDestinationLocal()) { // destination is this system
            // As we have a source (either a path or a TURL) for all files and
            // the destination is local, we have all the information needed.
            for (int i = 0; i < this.getFileCount(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if (cfr.getState() == State.PENDING && cfr.getSchedulerId() == null) {
                    String path = localPathFromSurl(cfr.getDestinationSurl());
                    LOG.debug("setting local destination path to {}", path);
                    cfr.setLocalDestinationPath(path);
                    cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
                }
            }
            return;
        }

        if (!isDestinationSrm()) { // destination is some TURL
            // As we have a source (either a path or a TURL) for all files and
            // the destination is a TURL, we have all the information needed.
            for (int i = 0; i < this.getFileCount(); ++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if (cfr.getState() == State.PENDING && cfr.getSchedulerId() == null) {
                    LOG.debug("setting destination to {}", getDestinationUrl(i).getURL());
                    cfr.setDestinationTurl(getDestinationUrl(i).getURI());
                    cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
                }
            }
            return;
        }

        // The only possibility left is a remote destination SURLs, for which
        // we need to discover a TURLs.

        for (int i = 0; i < getFileCount(); ++i) {
            CopyFileRequest cfr = getFileRequests().get(i);
            if (cfr.getState() != State.PENDING || cfr.getSchedulerId() != null) {
                // copy file request has being canceled,failed or scheduled before
            } else if (cfr.getDestinationTurl() != null) {
                //destination TURL has arrived, but request has not been scheduled
                cfr.scheduleWith(Scheduler.getScheduler(schedulerId));
            } else {
                remoteSurlToFileReqIds.put(getDestinationUrl(i).getURL(), cfr.getId());
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
            LOG.debug("local size is {}", sizes[i]);
            cfr.setSize(sizes[i]);
            destinationSurls[i] = cfr.getDestinationSurl().toString();
            if (getQosPlugin() != null) {
                makeQosReservation(i);
            }
        }

        // Now create an SRM client to fetch a TURL for each SURL.

        RequestCredential credential = getCredential();
        if (getCallerSrmProtocol() == null || getCallerSrmProtocol() == SRMProtocol.V1_1) {
            try {
                setRemoteTurlClient(new RemoteTurlPutterV1(getStorage(),
                        credential, destinationSurls, sizes, getProtocols(), this,
                        getConfiguration().getCopyRetryTimeout(), 2,
                        clientTransport));
                getRemoteTurlClient().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V1_1);
             } catch (SRMException srme) {
                 LOG.error("connecting with SRM v1.1 failed, trying with v2.2");
                 setRemoteTurlClient(new RemoteTurlPutterV2(getStorage(),
                         credential, destinationSurls, sizes, getProtocols(), this,
                         getConfiguration().getCopyRetryTimeout(), 2,
                         this.getRemainingLifetime(), getStorageType(),
                         getTargetRetentionPolicy(), getTargetAccessLatency(),
                         getOverwriteMode(), getTargetSpaceToken(),
                         clientTransport));
                 getRemoteTurlClient().getInitialRequest();
                 setRemoteSrmProtocol(SRMProtocol.V2_1);
             }
        } else if (getCallerSrmProtocol() == SRMProtocol.V2_1) {
            try {
                setRemoteTurlClient(new RemoteTurlPutterV2(getStorage(),
                        credential, destinationSurls, sizes, getProtocols(), this,
                        getConfiguration().getCopyRetryTimeout(), 2,
                        this.getRemainingLifetime(), getStorageType(),
                        getTargetRetentionPolicy(), getTargetAccessLatency(),
                        getOverwriteMode(), getTargetSpaceToken(),
                        clientTransport));
                getRemoteTurlClient().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V2_1);
             } catch (SRMException srme) {
                 LOG.error("connecting with SRM v2.2 failed, trying with SRM v1.1");
                setRemoteTurlClient(new RemoteTurlPutterV1(getStorage(),
                        credential, destinationSurls, sizes, getProtocols(), this,
                        getConfiguration().getCopyRetryTimeout(), 2,
                        clientTransport));
                getRemoteTurlClient().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V1_1);
             }
         } else {
             throw new FatalJobFailure("usupported SRM protocol: " + getCallerSrmProtocol());
         }

         getRemoteTurlClient().run();
    }

    public void turlArrived(String surl, String turl, String remoteRequestId,
            String remoteFileId, Long size)
    {
        synchronized (remoteSurlToFileReqIds) {
            Collection<Long> fileRequestIds = remoteSurlToFileReqIds.get(surl);
            if (fileRequestIds == null || fileRequestIds.isEmpty()) {
                LOG.error("turlArrived for unknown SURL = "+surl+" !!!!!!!");
                return;
            }
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
                        IllegalStateTransition | InterruptedException e) {
                    LOG.error("failed to schedule CopyFileRequest {}: {}", cfr,
                            e.toString());
                    try {
                        cfr.setState(State.FAILED, "Failed to schedule request: " + e.getMessage());
                    } catch (IllegalStateTransition ist) {
                        LOG.error("Illegal State Transition : {}" + ist.getMessage());
                    }
                }
                remoteSurlToFileReqIds.remove(surl, id);
            }
        }
    }

    public void turlRetrievalFailed(String surl, String reason,
            String remoteRequestId, String remoteFileId)
    {
        synchronized (remoteSurlToFileReqIds) {
            Collection<Long> fileRequestSet = remoteSurlToFileReqIds.get(surl);
            if (fileRequestSet == null || fileRequestSet.isEmpty()) {
                LOG.error("turlArrived for unknown SURL = "+surl);
                return;
            }
            for (long id : fileRequestSet) {
                CopyFileRequest cfr = getFileRequest(id);

                try {
                    String type = isSourceSrm() && !isSourceLocal() ? "source"
                            : "destination";
                    String error = "retrieval of " + type +
                            " TURL failed with error " + reason;
                    LOG.error(error);
                    cfr.setState(State.FAILED, error);
                } catch (IllegalStateTransition ist) {
                    LOG.error("Illegal State Transition : " + ist.getMessage());
                }
                cfr.saveJob();

                remoteSurlToFileReqIds.remove(surl, id);
            }
        }
        remoteFileRequestDone(surl, remoteRequestId, remoteFileId);
    }

    public void turlsRetrievalFailed(Object reason)
    {
        synchronized (remoteSurlToFileReqIds) {
            for (String surl : remoteSurlToFileReqIds.keySet()) {
                for (long id : remoteSurlToFileReqIds.get(surl)) {
                    CopyFileRequest cfr = getFileRequest(id);
                    try {
                        String type = isSourceSrm() && !isSourceLocal() ? "source"
                            : "destination";
                        String error = "retrieval of " + type +
                                " TURL failed with error " + reason;
                        LOG.error(error);
                        cfr.setState(State.FAILED, error);
                    } catch (IllegalStateTransition ist) {
                        LOG.error("Illegal State Transition : " + ist.getMessage());
                    }
                    cfr.saveJob();
                    remoteSurlToFileReqIds.remove(surl, id);
                }
            }
        }
    }

    public void remoteFileRequestDone(String surl, String requestId,
            String fileId)
    {
        synchronized (remoteSurlToFileReqIds) {
            try {
                if (getRemoteSrmProtocol() == SRMProtocol.V1_1) {
                    TurlGetterPutterV1.staticSetFileStatus(getCredential(), surl,
                            Integer.parseInt(requestId), Integer.parseInt(fileId),
                            "Done", getConfiguration().getCopyRetryTimeout(),
                            getConfiguration().getCopyMaxNumOfRetries(),
                            clientTransport);
                } else if (getRemoteSrmProtocol() == SRMProtocol.V2_1) {
                    if (isSourceSrm() && !isSourceLocal()) {
                       RemoteTurlGetterV2.staticReleaseFile(getCredential(),
                               surl, requestId,
                               getConfiguration().getCopyRetryTimeout(),
                               getConfiguration().getCopyMaxNumOfRetries(),
                               clientTransport);
                    } else {
                        RemoteTurlPutterV2.staticPutDone(getCredential(),
                               surl, requestId,
                               getConfiguration().getCopyRetryTimeout(),
                               getConfiguration().getCopyMaxNumOfRetries(),
                               clientTransport);
                    }
                } else {
                    LOG.error("unknown or null callerSrmProtocol");
                }
            } catch (Exception e) {
                LOG.error("set remote file status to done failed, surl={}, " +
                         "requestId={}, fileId={}", surl, requestId, fileId);
            }
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
    public String getMethod()
    {
        return "Copy";
    }

    //we want to stop handler if the
    //the request is ready (all file reqs are ready), since all copy transfers are
    // competed by now
    public boolean shouldStopHandlerIfReady()
    {
        return true;
    }

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure
    {
        if (isProcessingDone()) {
            return;
        }
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
                setState(State.ASYNCWAIT, "Waiting for transfers to complete.");
            }
        } catch (SRMException | IllegalStateTransition | DataAccessException e) {
            // FIXME some SRMException failures are temporary and others are
            // permanent.  Code currently doesn't distinguish between them and
            // always retries, even if problem isn't transitory.
            throw new NonFatalJobFailure(e.toString());
        } catch (IOException | InterruptedException e) {
            throw new FatalJobFailure(e.toString());
        }
    }

    @Override
    protected void stateChanged(State oldState)
    {
        State state = getState();
        if (state.isFinal()) {
            TurlGetterPutter client = getRemoteTurlClient();
            if (client != null) {
                LOG.debug("copyRequest TURL-fetching client is non null, stopping");
                client.stop();
            }
            LOG.debug("copy request state changed to {}", state);
            for (CopyFileRequest request : getFileRequests()) {
                try {
                    State frState = request.getState();
                    if (!(frState.isFinal())) {
                        LOG.debug("changing fr#{} to {}", request.getId(), state);
                        request.setState(state, "Request now " + state);
                    }
                } catch (IllegalStateTransition ist) {
                    LOG.error("Illegal State Transition : " + ist.getMessage());
                }
            }
        }
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt)
    {
        LOG.debug("propertyChange");
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
            LOG.error(e.toString());
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
                LOG.error("setting to done anyway: {}", e.toString());
                try {
                    State state = getState();
                    if (!state.isFinal()) {
                        setState(State.DONE, e.toString());
                    }
                } catch (IllegalStateTransition ist) {
                    LOG.error("Illegal State Transition : {}", ist.getMessage());
                }
            }
        }
    }

    public final SrmCopyResponse getSrmCopyResponse() throws SRMInvalidRequestException
    {
        ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus =
                new ArrayOfTCopyRequestFileStatus(getArrayOfTCopyRequestFileStatuses());
        return new SrmCopyResponse(getTReturnStatus(), getTRequestToken(), arrayOfTCopyRequestFileStatus, null);
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
        ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus =
            new ArrayOfTCopyRequestFileStatus();
        arrayOfTCopyRequestFileStatus.setStatusArray(
            getArrayOfTCopyRequestFileStatuses(fromurls,tourls));
        response.setArrayOfFileStatuses(arrayOfTCopyRequestFileStatus);
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

    public void setStorageType(TFileStorageType storageType)
    {
        wlock();
        try {
            this.storageType = storageType;
        } finally {
            wunlock();
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
     * @param i indext of the "from" url in the array
     * @return the from_urls
     */
    private SrmUrl getSourceUrl(int i)
    {
        rlock();
        try {
            return sourceUrl[i];
        } finally {
            runlock();
        }
    }

    /**
     * @param i indext of the "to" url in the array
     * @return the to_urls
     */
    private SrmUrl getDestinationUrl(int i)
    {
        rlock();
        try {
            return destinationUrls[i];
        } finally {
            runlock();
        }
    }

    /**
     * @return the number of files to be transfered in this request
     */
    private int getFileCount()
    {
        rlock();
        try {
            return fileCount;
        } finally {
            runlock();
        }
    }

    /**
     * @param files the number of files to be transferred in this request.
     */
    private void setFileCount(int files)
    {
        wlock();
        try {
            this.fileCount = files;
        } finally {
            wunlock();
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
     * @return the callerSrmProtocol
     */
    private SRMProtocol getCallerSrmProtocol()
    {
        rlock();
        try {
            return callerSrmProtocol;
        } finally {
            runlock();
        }
    }

    /**
     * @return the remoteSrmProtocol
     */
    private SRMProtocol getRemoteSrmProtocol()
    {
        rlock();
        try {
            return remoteSrmProtocol;
        } finally {
            runlock();
        }
    }

    /**
     * @param remoteSrmProtocol the remoteSrmProtocol to set
     */
    private void setRemoteSrmProtocol(SRMProtocol remoteSrmProtocol)
    {
        wlock();
        try {
            this.remoteSrmProtocol = remoteSrmProtocol;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the remoteSrmGet
     */
    private boolean isRemoteSrmGet()
    {
        return isSourceSrm() && !isSourceLocal();
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
