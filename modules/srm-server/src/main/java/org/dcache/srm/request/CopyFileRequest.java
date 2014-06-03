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
import org.apache.axis.types.UnsignedLong;
import org.globus.util.GlobusURL;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.ShellCommandExecuter;
import org.dcache.srm.v2_2.TCopyRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;

public final class CopyFileRequest extends FileRequest<CopyRequest>
{
    private static final Logger LOG = LoggerFactory.getLogger(CopyFileRequest.class);

    private static long lastTime;

    private final URI sourceSurl;
    private final URI destinationSurl;
    private URI sourceTurl;
    private URI destinationTurl;
    private String localSourcePath;
    private String localDestinationPath;
    private long size;
    private String destinationFileId;
    private String remoteRequestId;
    private String remoteFileId;
    private String transferId;
    private Exception transferError;
    //these are used if the transfer is performed in the pull mode for
    // storage of the space reservation related info
    private final String spaceReservationId;

    public CopyFileRequest(long requestId,
                           Long requestCredentalId,
                           URI sourceSurl,
                           URI destinationSurl,
                           String spaceToken,
                           long lifetime,
                           int maxNumberOfRetries)
    {
        super(requestId, requestCredentalId, lifetime, maxNumberOfRetries);
        LOG.debug("CopyFileRequest");
        this.sourceSurl = sourceSurl;
        this.destinationSurl = destinationSurl;
        this.spaceReservationId = spaceToken;
        LOG.debug("constructor from={} to={}", sourceSurl, destinationSurl);
    }

    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */
    public CopyFileRequest(long id,
            Long nextJobId,
            JobStorage<CopyFileRequest> jobStorage,
            long creationTime,
            long lifetime,
            int stateId,
            String errorMessage,
            String scheduelerId,
            long schedulerTimeStamp,
            int numberOfRetries,
            int maxNumberOfRetries,
            long lastStateTransitionTime,
            JobHistory[] jobHistoryArray,
            long requestId,
            Long requestCredentalId,
            String statusCodeString,
            String sourceSurl,
            String destinationSurl,
            String sourceTurl,
            String destinationTurl,
            String localSourcePath,
            String localDestinationPath,
            long size,
            String fromFileId,
            String toFileId,
            String remoteRequestId,
            String remoteFileId,
            String spaceReservationId,
            String transferId)
    {
        super(id, nextJobId, creationTime, lifetime, stateId, errorMessage,
              scheduelerId, schedulerTimeStamp, numberOfRetries,
              maxNumberOfRetries, lastStateTransitionTime, jobHistoryArray,
              requestId, requestCredentalId, statusCodeString);
        this.sourceSurl = URI.create(sourceSurl);
        this.destinationSurl = URI.create(destinationSurl);
        if (sourceTurl != null && !sourceTurl.equalsIgnoreCase("null")) {
            this.sourceTurl = URI.create(sourceTurl);
        }
        if (destinationTurl != null && !destinationTurl.equalsIgnoreCase("null")) {
            this.destinationTurl = URI.create(destinationTurl);
        }
        this.localSourcePath = localSourcePath;
        this.localDestinationPath = localDestinationPath;
        this.size = size;
        this.destinationFileId = toFileId;
        if (remoteRequestId != null && (!remoteRequestId.equalsIgnoreCase("null"))) {
            this.remoteRequestId = remoteRequestId;
        }
        if (remoteFileId != null && (!remoteFileId.equalsIgnoreCase("null"))) {
            this.remoteFileId = remoteFileId;
        }
        this.spaceReservationId = spaceReservationId;
        this.transferId = transferId;
    }

    public void done()
    {
        LOG.debug("done");
    }

    public void error()
    {
        done();
    }

    @Override
    public RequestFileStatus getRequestFileStatus()
    {
        RequestFileStatus rfs = new RequestFileStatus();
        rfs.fileId = (int) getId();
        rfs.SURL = getSourceSurl().toString();
        rfs.size = 0;
        rfs.TURL = getDestinationSurl().toString();
        State state = getState();
        if (state == State.DONE) {
            rfs.state = "Done";
        } else if (state == State.READY) {
            rfs.state = "Ready";
        } else if (state == State.TRANSFERRING) {
            rfs.state = "Running";
        } else if (state == State.FAILED || state == State.CANCELED) {
            rfs.state = "Failed";
        } else {
            rfs.state = "Pending";
        }
        return rfs;
    }

    /**
     * The source location if remote, null otherwise.
     */
    public URI getSourceTurl()
    {
        rlock();
        try {
            return sourceTurl;
        } finally {
            runlock();
        }
    }

    /**
     * Set the source location; implies the source is remote.
     */
    public void setSourceTurl(URI location)
    {
        wlock();
        try {
            this.sourceTurl = location;
        } finally {
            wunlock();
        }
    }

    /**
     * The destination location if remote, null otherwise.
     */
    public URI getDestinationTurl()
    {
        rlock();
        try {
            return destinationTurl;
        } finally {
            runlock();
        }
    }

    /**
     * Set the destination location; implies the source is remote.
     */
    public void setDestinationTurl(URI location)
    {
        wlock();
        try {
            this.destinationTurl = location;
        } finally {
            wunlock();
        }
    }

    /** Getter for property size.
     * @return Value of property size.
     */
    public long getSize()
    {
        rlock();
        try {
            return size;
        } finally {
            runlock();
        }
    }

    /** Setter for property size.
     * @param size New value of property size.
     */
    public void setSize(long size)
    {
        rlock();
        try {
            this.size = size;
        } finally {
            runlock();
        }
    }

    @Override
    public void toString(StringBuilder sb, String padding, boolean longformat)
    {
        sb.append(padding);
        if (padding.isEmpty()) {
            sb.append("Copy ");
        }
        sb.append("file id:").append(getId());
        if (getPriority() != 0) {
            sb.append(" priority:").append(getPriority());
        }
        sb.append(" state:").append(getState());
        if (longformat) {
            sb.append(" source=");
            appendPathSurlAndTurl(sb, getLocalSourcePath(), getSourceSurl(),
                    getSourceTurl());
            sb.append(" destination=");
            appendPathSurlAndTurl(sb, getLocalDestinationPath(),
                    getDestinationSurl(), getDestinationTurl());
            sb.append('\n');
            TStatusCode status = getStatusCode();
            if (status != null) {
                sb.append(padding).append("   Status:").append(status).append('\n');
            }
            sb.append(padding).append("   History of State Transitions:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    private static void appendPathSurlAndTurl(StringBuilder sb,
            String path, URI surl, URI turl)
    {
        if (path != null) {
            sb.append(path);
        } else {
            if (surl.getScheme().equalsIgnoreCase("srm")) {
                sb.append(surl);
                if (turl != null) {
                    sb.append(" --> ").append(turl);
                }
            } else {
                sb.append(turl);
            }
        }
    }

    /**
     * The absolute path of the source if local, or null if remote.
     */
    public String getLocalSourcePath()
    {
        rlock();
        try {
            return localSourcePath;
        } finally {
            runlock();
        }
    }

    /**
     * Set the absolute path of source; implies the source is local.
     */
    public void setLocalSourcePath(String path)
    {
        wlock();
        try {
            this.localSourcePath = path;
        } finally {
            wunlock();
        }
    }

    /**
     * The absolute path of the destination if local, or null.
     */
    public String getLocalDestinationPath()
    {
        rlock();
        try {
            return localDestinationPath;
        } finally {
            runlock();
        }
    }

    /**
     * Set the absolute path of destination; implies the destination is local.
     */
    public void setLocalDestinationPath(String path)
    {
        wlock();
        try {
            this.localDestinationPath = path;
        } finally {
            wunlock();
        }
    }

    /** Getter for property toFileId.
     * @return Value of property toFileId.
     *
     */
    public String getDestinationFileId()
    {
        rlock();
        try {
            return destinationFileId;
        } finally {
            runlock();
        }
    }

    /** Setter for property toFileId.
     * @param id New value of property toFileId.
     *
     */
    public void setDestinationFileId(String id)
    {
        wlock();
        try {
            destinationFileId = id;
        } finally {
            wunlock();
        }
    }

    private void runScriptCopy() throws SRMException, IOException,
            GSSException, DataAccessException
    {
        URI from = getSourceTurl();
        URI to = getDestinationTurl();
        if (from == null && getLocalSourcePath() != null) {
            if (to.getScheme().equalsIgnoreCase("gsiftp") ||
                    to.getScheme().equalsIgnoreCase("http") ||
                    to.getScheme().equalsIgnoreCase("ftp") ||
                    to.getScheme().equalsIgnoreCase("dcap")) {
                //need to add support for getting
                from = getStorage().getGetTurl(getUser(), getSourceSurl(),
                        new String[] {"gsiftp","http","ftp"}, null);
            }
        }
        String fileId = null;
        if (to == null && getLocalDestinationPath() != null) {
            if (from.getScheme().equalsIgnoreCase("gsiftp") ||
                    from.getScheme().equalsIgnoreCase("http") ||
                    from.getScheme().equalsIgnoreCase("ftp") ||
                    from.getScheme().equalsIgnoreCase("dcap")) {
                fileId = getStorage().prepareToPut(getUser(),
                                              getDestinationSurl(),
                                              null,
                                              Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                                              Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                                              getSpaceReservationId(),
                                              getContainerRequest().isOverwrite())
                                .checkedGet();
                to = getStorage().getPutTurl(getUser(), fileId,
                        new String[] {"gsiftp","http","ftp"}, null);
            }
        }
        if (from == null || to == null) {
            String error = "could not resolve either source or destination"+
                    " from = "+from+" to = "+to;
            LOG.error(error);
            throw new SRMException(error);
        }
        LOG.debug("calling scriptCopy({},{})", from, to);
        RequestCredential credential = getCredential();
        scriptCopy(new GlobusURL(from.toString()),
                   new GlobusURL(to.toString()),
                   credential.getDelegatedCredential());
        if (fileId != null) {
            getStorage().putDone(getUser(), fileId, getDestinationSurl(), getConfiguration().isOverwrite());
        }
        setStateToDone();
    }

    private void runLocalToLocalCopy() throws IllegalStateTransition, SRMException
    {
        LOG.debug("copying from local to local");
        FileMetaData fmd ;
        try {
            fmd = getStorage().getFileMetaData(getUser(), getSourceSurl(), true);
        } catch (SRMException srme) {
            try {
                setStateAndStatusCode(State.FAILED,
                                      srme.getMessage(),
                                      TStatusCode.SRM_INVALID_PATH);
            } catch (IllegalStateTransition ist) {
                LOG.error("Illegal State Transition : " +ist.getMessage());
            }
            return;
        }
        size = fmd.size;

        if (getDestinationFileId() == null) {
            setState(State.ASYNCWAIT, "Doing name space lookup.");
            LOG.debug("calling storage.prepareToPut({})", getLocalDestinationPath());
            CheckedFuture<String,? extends SRMException> future =
                    getStorage().prepareToPut(
                            getUser(),
                            getDestinationSurl(),
                            size,
                            Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                            Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                            getSpaceReservationId(),
                            getContainerRequest().isOverwrite());
            future.addListener(new PutCallbacks(getId(), future), sameThreadExecutor());
            LOG.debug("callbacks.waitResult()");
            return;
        }

        LOG.debug("known source size is {}", size);

        try {
            getStorage().localCopy(getUser(), getSourceSurl(), getDestinationFileId());
            getStorage().putDone(getUser(), getDestinationFileId(), getDestinationSurl(), getContainerRequest().isOverwrite());
            setStateToDone();
        } catch (SRMException e) {
            getStorage().abortPut(getUser(), getDestinationFileId(), getDestinationSurl(), e.getMessage());
            throw e;
        }
    }

    private void runRemoteToLocalCopy() throws IllegalStateTransition,
            SRMException, NonFatalJobFailure
    {
        LOG.debug("copying from remote to local");
        RequestCredential credential = getCredential();
        if (getDestinationFileId() == null) {
            setState(State.ASYNCWAIT, "Doing name space lookup.");
            LOG.debug("calling storage.prepareToPut({})", getLocalDestinationPath());
            CheckedFuture<String,? extends SRMException> future =
                    getStorage().prepareToPut(
                            getUser(), getDestinationSurl(), size,
                            Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                            Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                            getSpaceReservationId(),
                            getContainerRequest().isOverwrite());
            future.addListener(new PutCallbacks(getId(), future), sameThreadExecutor());
            LOG.debug("callbacks.waitResult");
            return;
        }
        LOG.debug("known source size is {}", size);

        if (getTransferId() == null) {
            setState(State.RUNNINGWITHOUTTHREAD, "started remote transfer, waiting completion");
            TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId()) {
                @Override
                public void copyComplete()
                {
                    try {
                        getStorage().putDone(
                                getUser(),
                                getDestinationFileId(),
                                getDestinationSurl(),
                                getContainerRequest().isOverwrite());
                        super.copyComplete();
                    } catch (SRMException e) {
                        copyFailed(e);
                    }
                }
            };
            setTransferId(getStorage().getFromRemoteTURL(getUser(), getSourceTurl(), getDestinationFileId(), getUser(), credential.getId(), copycallbacks));
            saveJob();
        } else {
            // transfer id is not null and we are scheduled
            // there was some kind of error during the transfer

            getStorage().killRemoteTransfer(getTransferId());
            Exception transferError = getTransferError();
            getStorage().abortPut(getUser(), getDestinationFileId(), getDestinationSurl(),
                                  (transferError == null) ? null : transferError.getMessage());
            setDestinationFileId(null);
            setTransferId(null);
            throw new NonFatalJobFailure(transferError);
        }
    }

    private void setStateToDone()
    {
        try {
            setState(State.DONE, "setStateToDone called");
            try {
                getContainerRequest().fileRequestCompleted();
            } catch (SRMInvalidRequestException ire) {
                LOG.error(ire.toString());
            }
        } catch (IllegalStateTransition ist) {
            LOG.error("setStateToDone: Illegal State Transition : " +ist.getMessage());
        }
    }

    private void setStateToFailed(String error) throws SRMInvalidRequestException
    {
        try {
            setState(State.FAILED, error);
        } catch (IllegalStateTransition ist) {
            LOG.error("setStateToFailed: Illegal State Transition : " +ist.getMessage());
        }
        getContainerRequest().fileRequestCompleted();
    }

    private void runLocalToRemoteCopy() throws SRMException, NonFatalJobFailure,
            IllegalStateTransition
    {
        if (getTransferId() == null) {
            LOG.debug("copying using storage.putToRemoteTURL");
            RequestCredential credential = getCredential();
            TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId());
            setTransferId(getStorage().putToRemoteTURL(getUser(), getSourceSurl(), getDestinationTurl(), getUser(), credential.getId(), copycallbacks));
            setState(State.RUNNINGWITHOUTTHREAD, "Transferring file.");
            saveJob();
        } else {
            // transfer id is not null and we are scheduled
            // there was some kind of error durign the transfer
            getStorage().killRemoteTransfer(getTransferId());
            setTransferId(null);
            throw new NonFatalJobFailure(getTransferError());
        }
    }

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure
    {
        LOG.debug("copying");
        try {
            if (getSourceTurl() != null && getSourceTurl().getScheme().equalsIgnoreCase("dcap")  ||
                    getDestinationTurl() != null && getDestinationTurl().getScheme().equalsIgnoreCase("dcap") ||
                    getConfiguration().isUseUrlcopyScript()) {
                try {
                    runScriptCopy();
                    return;
                } catch (SRMException | IOException | GSSException e) {
                    LOG.warn("script failed: {}",
                                e.toString());
                    // fall-through to try other methods
                }
            }
            if (getLocalDestinationPath() != null && getLocalSourcePath() != null) {
                runLocalToLocalCopy();
            } else if (getLocalDestinationPath() != null && getSourceTurl() != null) {
                runRemoteToLocalCopy();
            } else if (getDestinationTurl() != null && getLocalSourcePath() != null) {
                runLocalToRemoteCopy();
            } else if (getSourceTurl() != null && getDestinationTurl() != null) {
                javaUrlCopy(getSourceTurl().toURL(),
                            getDestinationTurl().toURL());
                LOG.debug("copy succeeded");
                setStateToDone();
            } else {
                LOG.error("Unknown combination of to/from ursl");
                setStateToFailed("Unknown combination of to/from ursl");
            }
        } catch (IllegalStateTransition | IOException | SRMException | DataAccessException e) {
            throw new NonFatalJobFailure(e.toString());
        }
    }

    public synchronized static long uniqueCurrentTime()
    {
        long time = System.currentTimeMillis();
        lastTime = lastTime < time ? time : lastTime + 1;
        return lastTime;
    }

    public void scriptCopy(GlobusURL source, GlobusURL destination,
            GSSCredential credential) throws IOException, GSSException
    {
        String proxyFile = null;
        try {
            String command = getConfiguration().getTimeout_script();
            command = command + " " + getConfiguration().getTimeout();
            command = command + " " + getConfiguration().getUrlcopy();
            command = command + " -debug " + getConfiguration().isDebug();
            if (credential != null) {
                try {
                    byte [] data = ((ExtendedGSSCredential)(credential)).export(
                            ExtendedGSSCredential.IMPEXP_OPAQUE);
                    proxyFile = getConfiguration().getProxies_directory()+
                            "/proxy_"+credential.hashCode()+"_at_"+uniqueCurrentTime();
                    LOG.debug("saving credential {} in proxy_file {}",
                            credential.getName(), proxyFile);
                    FileOutputStream out = new FileOutputStream(proxyFile);
                    out.write(data);
                    out.close();
                    LOG.debug("save succeeded ");
                } catch (IOException ioe) {
                    LOG.error("saving credentials to "+proxyFile+" failed");
                    LOG.error(ioe.toString());
                    proxyFile = null;
                }
            }
            if (proxyFile != null) {
                command = command+" -x509_user_proxy " + proxyFile;
                command = command+" -x509_user_key " + proxyFile;
                command = command+" -x509_user_cert " + proxyFile;
            }
            int tcpBufferSize = getConfiguration().getTcp_buffer_size();
            if (tcpBufferSize > 0) {
                command = command + " -tcp_buffer_size " + tcpBufferSize;
            }
            int bufferSize = getConfiguration().getBuffer_size();
            if (bufferSize > 0) {
                command = command + " -buffer_size " + bufferSize;
            }
            int parallelStreams = getConfiguration().getParallel_streams();
            if (parallelStreams > 0) {
                command = command + " -parallel_streams " + parallelStreams;
            }
            command = command + " -src-protocol " + source.getProtocol();
            if (source.getProtocol().equals("file")) {
                command = command + " -src-host-port localhost";
            } else {
                command = command + " -src-host-port " + source.getHost() + ":"
                        + source.getPort();
            }
            command = command + " -src-path " + source.getPath() +
                    " -dst-protocol " + destination.getProtocol();
            if (destination.getProtocol().equals("file")) {
                command = command + " -dst-host-port localhost";
            } else {
                command = command + " -dst-host-port " + destination.getHost() +
                        ":" + destination.getPort();
            }
            command = command + " -dst-path " + destination.getPath();
            String sourceUsername = source.getUser();
            if (sourceUsername != null) {
                command = command + " -src_username " + sourceUsername;
            }
            String sourcePassword = source.getPwd();
            if (sourcePassword != null) {
                command = command + " -src_userpasswd " + sourcePassword;
            }
            String destinationUser = destination.getUser();
            if (destinationUser != null) {
                command = command + " -dst_username " + destinationUser;
            }
            String destinationPassword = destination.getPwd();
            if (destinationPassword != null) {
                command = command + " -dst_userpasswd " + destinationPassword;
            }
            String gsiftpclient = getConfiguration().getGsiftpclinet();
            if (gsiftpclient != null) {
                command = command + " -use-kftp " +
                        (gsiftpclient.toLowerCase().contains("kftp"));
            }
            int rc = ShellCommandExecuter.execute(command);
            LOG.debug("return code = {}", rc);
            if (rc != 0) {
                throw new IOException("return code = "+rc+", failure");
            }
        } finally {
            if (proxyFile != null) {
                try {
                    LOG.debug("deleting proxy file {}", proxyFile);
                    File f = new File(proxyFile);
                    if (!f.delete()) {
                        LOG.error("error deleting proxy cache {}", proxyFile);
                    }
                } catch (Exception e) {
                    LOG.error("error deleting proxy cache {}: {}", proxyFile,
                            e.toString());
                }
            }
        }
    }

    public void javaUrlCopy(URL source, URL destination) throws IOException
    {
        InputStream in;
        if (source.getProtocol().equals("file")) {
            in = new FileInputStream(source.getPath());
        } else {
            in = source.openConnection().getInputStream();
        }
        OutputStream out;
        if (destination.getProtocol().equals("file")) {
            out = new FileOutputStream(destination.getPath());
        } else {
            URLConnection to_connect = destination.openConnection();
            to_connect.setDoInput(false);
            to_connect.setDoOutput(true);
            out = to_connect.getOutputStream();
        }
        try {
            int bufferSize = 4096;
            byte[] bytes = new byte[bufferSize];
            long total = 0;
            int l;
            while ((l = in.read(bytes)) != -1) {
                total += l;
                out.write(bytes, 0, l);
            }
            LOG.debug("done, copied {} bytes", total);
        } finally {
            in.close();
            out.close();
        }
    }

    @Override
    protected void stateChanged(State oldState)
    {
        State state = getState();
        if (state.isFinal()) {
            if (getTransferId() != null && state != State.DONE) {
                getStorage().killRemoteTransfer(getTransferId());
                String toFileId = getDestinationFileId();
                if (toFileId != null) {
                    try {
                        Exception transferError = getTransferError();
                        getStorage().abortPut(getUser(), toFileId, getDestinationSurl(),
                                              (transferError == null) ? null : transferError.getMessage());
                    } catch (SRMException e) {
                        LOG.error("Failed to abort copy: {}", e.getMessage());
                    }
                }
            }
            String remoteRequestId = getRemoteRequestId();
            if (remoteRequestId != null) {
                if (getLocalSourcePath() != null) {
                    remoteFileRequestDone(getDestinationSurl(), remoteRequestId, getRemoteFileId());
                } else {
                    remoteFileRequestDone(getSourceSurl(), remoteRequestId, getRemoteFileId());
                }
            }
        }
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSourceSurl()) || surl.equals(getDestinationSurl());
    }

    public void remoteFileRequestDone(URI SURL,String remoteRequestId,String remoteFileId)
    {
        try {
            LOG.debug("setting remote file status to Done, SURL={} " +
                    "remoteRequestId={} remoteFileId={}", SURL,
                    remoteRequestId, remoteFileId);
            getContainerRequest().remoteFileRequestDone(SURL.toString(),
                    remoteRequestId, remoteFileId);
        } catch (Exception e) {
            LOG.error("set remote file status to done failed, surl={}, " +
                    "requestId={}, fileId={}", SURL, remoteRequestId, remoteFileId);
        }
    }

    /** Getter for property remoteFileId.
     * @return Value of property remoteFileId.
     *
     */
    public String getRemoteFileId()
    {
        rlock();
        try {
            return remoteFileId;
        } finally {
            runlock();
        }
    }

    /** Getter for property remoteRequestId.
     * @return Value of property remoteRequestId.
     *
     */
    public String getRemoteRequestId()
    {
        rlock();
        try {
            return remoteRequestId;
        } finally {
            runlock();
        }
    }
    /**
     * Getter for property from_surl.
     * @return Value of property from_surl.
     */
    public URI getSourceSurl()
    {
        return sourceSurl;
    }
    /**
     * Getter for property to_surl.
     * @return Value of property to_surl.
     */
    public URI getDestinationSurl()
    {
        return destinationSurl;
    }
    /**
     * Setter for property remoteRequestId.
     * @param remoteRequestId New value of property remoteRequestId.
     */
    public void setRemoteRequestId(String remoteRequestId)
    {
        wlock();
        try {
            this.remoteRequestId = remoteRequestId;
        } finally {
            wunlock();
        }
    }
    /**
     * Setter for property remoteFileId.
     * @param remoteFileId New value of property remoteFileId.
     */
    public void setRemoteFileId(String remoteFileId)
    {
        wlock();
        try {
            this.remoteFileId = remoteFileId;
        } finally {
            wunlock();
        }
    }
    /**
     * Getter for property spaceReservationId.
     * @return Value of property spaceReservationId.
     */
    public String getSpaceReservationId()
    {
        return spaceReservationId;
    }

    /**
     * @param transferId the transferId to set
     */
    private void setTransferId(String transferId)
    {
        wlock();
        try {
            this.transferId = transferId;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the transferError
     */
    private Exception getTransferError()
    {
        rlock();
        try {
            return transferError;
        } finally {
            runlock();
        }
    }

    /**
     * @param transferError the transferError to set
     */
    private void setTransferError(Exception transferError)
    {
        wlock();
        try {
            this.transferError = transferError;
        } finally {
            wunlock();
        }
    }

    private static class PutCallbacks implements Runnable
    {
        private final long fileRequestJobId;
        private final CheckedFuture<String, ? extends SRMException> future;

        public PutCallbacks(long fileRequestJobId, CheckedFuture<String, ? extends SRMException> future)
        {
            this.fileRequestJobId = fileRequestJobId;
            this.future = future;
        }

        @Override
        public void run()
        {
            try {
                CopyFileRequest fr = Job.getJob(fileRequestJobId, CopyFileRequest.class);
                try {
                    String fileId = future.checkedGet();
                    State state = fr.getState();
                    if (state == State.ASYNCWAIT) {
                        LOG.debug("PutCallbacks success for file {}", fr.getDestinationSurl());
                        fr.setDestinationFileId(fileId);
                        Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                        try {
                            scheduler.schedule(fr);
                        } catch (Exception ie) {
                            LOG.error(ie.toString());
                        }
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            e.getMessage(),
                            e.getStatusCode());
                }
            } catch (IllegalStateTransition e) {
                LOG.error("Illegal State Transition: {}", e.getMessage());
            } catch (SRMInvalidRequestException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private static class TheCopyCallbacks implements CopyCallbacks
    {
        private final long fileRequestJobId;

        public TheCopyCallbacks(long fileRequestJobId)
        {
            this.fileRequestJobId = fileRequestJobId;
        }

        private CopyFileRequest getCopyFileRequest()
                throws SRMInvalidRequestException
        {
            return Job.getJob(fileRequestJobId, CopyFileRequest.class);
        }

        @Override
        public void copyComplete()
        {
            try {
                CopyFileRequest copyFileRequest = getCopyFileRequest();
                LOG.debug("copy succeeded");
                copyFileRequest.setStateToDone();
            } catch (SRMInvalidRequestException ire) {
                LOG.error(ire.toString());
            }
        }

        @Override
        public void copyFailed(SRMException e)
        {
            CopyFileRequest copyFileRequest;
            try {
                copyFileRequest = getCopyFileRequest();
            } catch (SRMInvalidRequestException ire) {
                LOG.error(ire.toString());
                return;
            }
            copyFileRequest.setTransferError(e);
            LOG.error("copy failed: {}", e.getMessage());
            State state = copyFileRequest.getState();
            Scheduler scheduler = Scheduler.getScheduler(copyFileRequest.getSchedulerId());
            if (!state.isFinal() && scheduler != null) {
                try {
                    scheduler.schedule(copyFileRequest);
                } catch (IllegalStateTransition ie) {
                    LOG.error(ie.toString());
                }
            }
        }
    }

    public TCopyRequestFileStatus getTCopyRequestFileStatus() throws SRMInvalidRequestException
    {
        TCopyRequestFileStatus copyRequestFileStatus = new TCopyRequestFileStatus();
        copyRequestFileStatus.setFileSize(new UnsignedLong(size));
        copyRequestFileStatus.setEstimatedWaitTime((int) (getRemainingLifetime() / 1000));
        copyRequestFileStatus.setRemainingFileLifetime((int) (getRemainingLifetime() / 1000));
        org.apache.axis.types.URI sourceSurl;
        org.apache.axis.types.URI destinationSurl;
        try {
            sourceSurl = new org.apache.axis.types.URI(getDestinationSurl().toASCIIString());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOG.error(e.toString());
            throw new SRMInvalidRequestException("wrong SURL format: " + getDestinationSurl());
        }
        try {
            destinationSurl = new org.apache.axis.types.URI(getSourceSurl().toASCIIString());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOG.error(e.toString());
            throw new SRMInvalidRequestException("wrong SURL format: " + getSourceSurl());
        }
        copyRequestFileStatus.setSourceSURL(destinationSurl);
        copyRequestFileStatus.setTargetSURL(sourceSurl);
        TReturnStatus returnStatus = getReturnStatus();
        if (TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, null);
        }
        copyRequestFileStatus.setStatus(returnStatus);
        return copyRequestFileStatus;
    }

    @Override
    public TReturnStatus getReturnStatus()
    {
        String description = getLastJobChange().getDescription();
        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            return new TReturnStatus(statusCode, description);
        }
        switch (getState()) {
        case DONE:
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        case READY:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        case TRANSFERRING:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        case FAILED:
            return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
        case CANCELED:
            return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
        case TQUEUED:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        case RUNNING:
        case RQUEUED:
        case ASYNCWAIT:
        case PRIORITYTQUEUED:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        default:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        }
    }

    /**
     * Getter for property transferId.
     * @return Value of property transferId.
     */
    public String getTransferId()
    {
        return transferId;
    }

    /**
     *
     *
     * @param newLifetime  new lifetime in millis
     *  -1 stands for infinite lifetime
     * @return int lifetime left in millis
     *  -1 stands for infinite lifetime
     */
    @Override
    public long extendLifetime(long newLifetime) throws SRMException
    {
        long remainingLifetime = getRemainingLifetime();
        if (remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        long requestLifetime = getContainerRequest().extendLifetimeMillis(newLifetime);
        if (requestLifetime <newLifetime) {
            newLifetime = requestLifetime;
        }
        if (remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        return extendLifetimeMillis(newLifetime);
    }
}
