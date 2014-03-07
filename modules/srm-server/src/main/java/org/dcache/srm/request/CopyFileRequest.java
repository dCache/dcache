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

public final class CopyFileRequest extends FileRequest<CopyRequest> {

    private static final Logger logger =
            LoggerFactory.getLogger(CopyFileRequest.class);
    private static final String SFN_STRING="?SFN=";
    private final URI from_surl;
    private final URI to_surl;
    private URI from_turl;
    private URI to_turl;
    private String local_from_path;
    private String local_to_path;
    private long size;
    private String toFileId;
    private String remoteRequestId;
    private String remoteFileId;
    private String transferId;
    private Exception transferError;
    //these are used if the transfer is performed in the pull mode for
    // storage of the space reservation related info
    private final String spaceReservationId;

    public CopyFileRequest(long requestId,
                           Long  requestCredentalId,
                           URI from_surl,
                           URI to_surl,
                           String spaceToken,
                           long lifetime,
                           int max_number_of_retries) {
        super(requestId,
              requestCredentalId,
              lifetime, max_number_of_retries);
        logger.debug("CopyFileRequest");
        this.from_surl = from_surl;
        this.to_surl = to_surl;
        this.spaceReservationId = spaceToken;
        logger.debug("constructor from_url=" +from_surl+" to_url="+to_surl);
    }

    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */

    public CopyFileRequest(
            long id,
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
            String FROMURL,
            String TOURL,
            String FROMTURL,
            String TOTURL,
            String FROMLOCALPATH,
            String TOLOCALPATH,
            long size,
            String fromFileId,
            String toFileId,
            String REMOTEREQUESTID,
            String REMOTEFILEID,
            String spaceReservationId,
            String transferId
    ) {
        super(id,
              nextJobId,
              creationTime,
              lifetime,
              stateId,
              errorMessage,
              scheduelerId,
              schedulerTimeStamp,
              numberOfRetries,
              maxNumberOfRetries,
              lastStateTransitionTime,
              jobHistoryArray,
              requestId,
              requestCredentalId,
              statusCodeString);
        this.from_surl = URI.create(FROMURL);
        this.to_surl = URI.create(TOURL);
        if(FROMTURL != null && !FROMTURL.equalsIgnoreCase("null")) {
            this.from_turl = URI.create(FROMTURL);
        }
        if(TOTURL != null && !TOTURL.equalsIgnoreCase("null")) {
            this.to_turl = URI.create(TOTURL);
        }
        this.local_from_path = FROMLOCALPATH;
        this.local_to_path = TOLOCALPATH;
        this.size = size;
        this.toFileId = toFileId;
        if(REMOTEREQUESTID != null && (!REMOTEREQUESTID.equalsIgnoreCase("null"))) {
            this.remoteRequestId = REMOTEREQUESTID;
        }
        if(REMOTEFILEID != null && (!REMOTEFILEID.equalsIgnoreCase("null"))) {
            this.remoteFileId = REMOTEFILEID;
        }
        this.spaceReservationId = spaceReservationId;
        this.transferId = transferId;
    }

    public void done() {
        logger.debug("done()");
    }

    public void error() {
        done();
    }

    @Override
    public RequestFileStatus getRequestFileStatus() {
        RequestFileStatus rfs = new RequestFileStatus();
        rfs.fileId = (int) getId();
        rfs.SURL = getFrom_surl().toString();
        rfs.size = 0;
        rfs.TURL = getTo_surl().toString();
        State state = getState();
        if(state == State.DONE) {
            rfs.state = "Done";
        }
        else if(state == State.READY) {
            rfs.state = "Ready";
        }
        else if(state == State.TRANSFERRING) {
            rfs.state = "Running";
        }
        else if(state == State.FAILED
                || state == State.CANCELED ) {
            rfs.state = "Failed";
        }
        else {
            rfs.state = "Pending";
        }
        return rfs;
    }

    public String getToURL() {
        return getTo_surl().toString();
    }

    public String getFromURL() {
        return getFrom_surl().toString();
    }

    public String getFromPath() {
        String path = getFrom_surl().getPath();
        int indx=path.indexOf(SFN_STRING);
        if( indx != -1) {
            path=path.substring(indx+SFN_STRING.length());
        }
        if(!path.startsWith("/")) {
            path = "/"+path;
        }
        logger.debug("getFromPath() returns "+path);
        return path;
    }

    public String getToPath() {
        String path = getTo_surl().getPath();
        int indx=path.indexOf(SFN_STRING);
        if( indx != -1) {
            path=path.substring(indx+SFN_STRING.length());
        }
        if(!path.startsWith("/")) {
            path = "/"+path;
        }
        logger.debug("getToPath() returns "+path);
        return path;
    }

    /** Getter for property from_turl.
     * @return Value of property from_turl.
     */
    public URI getFrom_turl() {
        rlock();
        try {
            return from_turl;
        } finally {
            runlock();
        }
    }

    /** Setter for property from_turl.
     * @param from_turl New value of property from_turl.
     */
    public void setFrom_turl(URI from_turl) {
        wlock();
        try {
            this.from_turl = from_turl;
        } finally {
            wunlock();
        }
    }

    /** Getter for property to_turl.
     * @return Value of property to_turl.
     */
    public URI getTo_turl() {
        rlock();
        try {
            return to_turl;
        } finally {
            runlock();
        }
    }

    /** Setter for property to_turl.
     * @param to_turl New value of property to_turl.
     */
    public void setTo_turl(URI to_turl) {
        wlock();
        try {
            this.to_turl = to_turl;
        } finally {
            wunlock();
        }
    }

    /** Getter for property size.
     * @return Value of property size.
     */
    public long getSize() {
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
    public void setSize(long size) {
        rlock();
        try {
            this.size = size;
        } finally {
            runlock();
        }
    }
    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        sb.append(" CopyFileRequest ");
        sb.append(" id:").append(getId());
        sb.append(" priority:").append(getPriority());
        sb.append(" creator priority:");
        try {
            sb.append(getUser().getPriority());
        } catch (SRMInvalidRequestException ire) {
            sb.append("Unknown");
        }
        sb.append(" state:").append(getState());
        if(longformat) {
            sb.append(" fromSurl:").append(getFrom_surl());
            sb.append(" fromTurl:").append(getFrom_turl()==null?"null":getFrom_turl());
            sb.append(" toSurl:").append(getTo_surl());
            sb.append(" toTurl:").append(getTo_turl()==null?"null":getTo_turl());
            sb.append('\n').append("   status code:").append(getStatusCode());
            sb.append('\n').append("   error message:").append(getErrorMessage());
            sb.append('\n').append("   History of State Transitions: \n");
            sb.append(getHistory());
        }
    }

    /** Getter for property absolute_local_from_path.
     * @return Value of property absolute_local_from_path.
     */
    public String getLocal_from_path() {
        rlock();
        try {
            return local_from_path;
        } finally {
            runlock();
        }
    }

    /** Setter for property absolute_local_from_path.
     * @param local_from_path New value of property absolute_local_from_path.
     */
    public void setLocal_from_path(String local_from_path) {
        wlock();
        try {
            this.local_from_path = local_from_path;
        } finally {
            wunlock();
        }
    }

    /** Getter for property absolute_local_to_path.
     * @return Value of property absolute_local_to_path.
     */
    public String getLocal_to_path() {
        rlock();
        try {
            return local_to_path;
        } finally {
            runlock();
        }
    }

    /** Setter for property absolute_local_to_path.
     * @param local_to_path New value of property absolute_local_to_path.
     */
    public void setLocal_to_path( String local_to_path) {
        wlock();
        try {
            this.local_to_path = local_to_path;
        } finally {
            wunlock();
        }
    }

    /** Getter for property toFileId.
     * @return Value of property toFileId.
     *
     */
    public String getToFileId() {
        rlock();
        try {
            return toFileId;
        } finally {
            runlock();
        }
    }

    /** Setter for property toFileId.
     * @param toFileId New value of property toFileId.
     *
     */
    public void setToFileId(String toFileId) {
        wlock();
        try {
            this.toFileId = toFileId;
        } finally {
            wunlock();
        }
    }

    private void runScriptCopy() throws SRMException, IOException,
            GSSException, DataAccessException
    {
        URI from =getFrom_turl();
        URI to = getTo_turl();
        if(from == null && getLocal_from_path() != null ) {
            if(to.getScheme().equalsIgnoreCase("gsiftp") ||
                    to.getScheme().equalsIgnoreCase("http") ||
                    to.getScheme().equalsIgnoreCase("ftp") ||
                    to.getScheme().equalsIgnoreCase("dcap")) {
                //need to add support for getting
                from =
                        getStorage().getGetTurl(getUser(),
                                                getFrom_surl(),
                                                new String[] {"gsiftp","http","ftp"},
                                                null);
            }
        }
        String fileId = null;
        if(to == null && getLocal_to_path() != null) {
            if(from.getScheme().equalsIgnoreCase("gsiftp") ||
                    from.getScheme().equalsIgnoreCase("http") ||
                    from.getScheme().equalsIgnoreCase("ftp") ||
                    from.getScheme().equalsIgnoreCase("dcap")) {
                fileId =
                        getStorage()
                                .prepareToPut(getUser(),
                                              getTo_surl(),
                                              null,
                                              Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                                              Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                                              getSpaceReservationId(),
                                              getContainerRequest().isOverwrite())
                                .checkedGet();
                to =
                        getStorage().getPutTurl(getUser(),
                                                fileId,
                                                new String[] {"gsiftp","http","ftp"},
                                                null);
            }
        }
        if(from ==null || to == null) {
            String error = "could not resolve either source or destination"+
                    " from = "+from+" to = "+to;
            logger.error(error);
            throw new SRMException(error);
        }
        logger.debug("calling scriptCopy({},{})", from, to);
        RequestCredential credential = getCredential();
        scriptCopy(new GlobusURL(from.toString()),
                   new GlobusURL(to.toString()),
                   credential.getDelegatedCredential());
        if (fileId != null) {
            getStorage().putDone(getUser(), fileId, getTo_surl(), getConfiguration().isOverwrite());
        }
        setStateToDone();
    }

    private void runLocalToLocalCopy() throws IllegalStateTransition, SRMException {
        logger.debug("copying from local to local ");
        FileMetaData fmd ;
        try {
            fmd = getStorage().getFileMetaData(getUser(), getFrom_surl(),true);
        } catch (SRMException srme) {
            try {
                setStateAndStatusCode(State.FAILED,
                                      srme.getMessage(),
                                      TStatusCode.SRM_INVALID_PATH);
            } catch (IllegalStateTransition ist) {
                logger.error("Illegal State Transition : " +ist.getMessage());
            }
            return;

        }
        size = fmd.size;

        if(getToFileId() == null) {
            setState(State.ASYNCWAIT, "Doing name space lookup.");
            logger.debug("calling storage.prepareToPut("+getLocal_to_path()+")");
            CheckedFuture<String,? extends SRMException> future =
                    getStorage().prepareToPut(
                            getUser(),
                            getTo_surl(),
                            size,
                            Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                            Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                            getSpaceReservationId(),
                            getContainerRequest().isOverwrite());
            future.addListener(new PutCallbacks(getId(), future), sameThreadExecutor());
            logger.debug("callbacks.waitResult()");
            return;
        }
        logger.debug("known source size is "+size);

        try {
            getStorage().localCopy(getUser(), getFrom_surl(), getToFileId());
            getStorage().putDone(getUser(), getToFileId(), getTo_surl(), getContainerRequest().isOverwrite());
            setStateToDone();
        } catch (SRMException e) {
            getStorage().abortPut(getUser(), getToFileId(), getTo_surl(), e.getMessage());
            throw e;
        }
    }

    private void runRemoteToLocalCopy() throws IllegalStateTransition,
            SRMException, NonFatalJobFailure
    {
        logger.debug("copying from remote to local ");
        RequestCredential credential = getCredential();
        if (getToFileId() == null) {
            setState(State.ASYNCWAIT, "Doing name space lookup.");
            logger.debug("calling storage.prepareToPut("+getLocal_to_path()+")");
            CheckedFuture<String,? extends SRMException> future =
                    getStorage().prepareToPut(
                            getUser(), getTo_surl(), size,
                            Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                            Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                            getSpaceReservationId(),
                            getContainerRequest().isOverwrite());
            future.addListener(new PutCallbacks(getId(), future), sameThreadExecutor());
            logger.debug("callbacks.waitResult()");
            return;
        }
        logger.debug("known source size is "+size);

        if(getTransferId() == null) {
            setState(State.RUNNINGWITHOUTTHREAD,"started remote transfer, waiting completion");
            TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId()) {
                @Override
                public void copyComplete()
                {
                    try {
                        getStorage().putDone(
                                getUser(),
                                getToFileId(),
                                getTo_surl(),
                                getContainerRequest().isOverwrite());
                        super.copyComplete();
                    } catch (SRMException e) {
                        copyFailed(e);
                    }
                }
            };
            setTransferId(getStorage().getFromRemoteTURL(getUser(), getFrom_turl(), getToFileId(), getUser(), credential.getId(), copycallbacks));
            saveJob();
        }
        // transfer id is not null and we are scheduled
        // there was some kind of error during the transfer
        else {
            getStorage().killRemoteTransfer(getTransferId());
            Exception transferError = getTransferError();
            getStorage().abortPut(getUser(), getToFileId(), getTo_surl(),
                                  (transferError == null) ? null : transferError.getMessage());
            setTransferId(null);
            throw new NonFatalJobFailure(transferError);
        }
    }

    private void setStateToDone(){
        try {
            setState(State.DONE, "setStateToDone called");
            try {
                getContainerRequest().fileRequestCompleted();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }
        catch(IllegalStateTransition ist) {
            logger.error("setStateToDone: Illegal State Transition : " +ist.getMessage());
        }
    }

    private void setStateToFailed(String error) throws SRMInvalidRequestException {
        try {
            setState(State.FAILED, error);
        }
        catch(IllegalStateTransition ist) {
            logger.error("setStateToFailed: Illegal State Transition : " +ist.getMessage());
        }
        getContainerRequest().fileRequestCompleted();
    }

    private void runLocalToRemoteCopy() throws SRMException,
            IllegalStateTransition, NonFatalJobFailure
    {
        if(getTransferId() == null) {
            logger.debug("copying using storage.putToRemoteTURL");
            RequestCredential credential = getCredential();
            TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId());
            setTransferId(getStorage().putToRemoteTURL(getUser(), getFrom_surl(), getTo_turl(), getUser(), credential.getId(), copycallbacks));
            setState(State.RUNNINGWITHOUTTHREAD, "Transferring file.");
            saveJob();
        }
        // transfer id is not null and we are scheduled
        // there was some kind of error durign the transfer
        else {
            getStorage().killRemoteTransfer(getTransferId());
            setTransferId(null);
            throw new NonFatalJobFailure(getTransferError());
        }
    }

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure {
        logger.debug("copying " );
        try {
            if(getFrom_turl() != null && getFrom_turl().getScheme().equalsIgnoreCase("dcap")  ||
                    getTo_turl() != null && getTo_turl().getScheme().equalsIgnoreCase("dcap") ||
                    getConfiguration().isUseUrlcopyScript()) {
                try {
                    runScriptCopy();
                    return;
                } catch(SRMException | IOException | GSSException e) {
                    logger.warn("script failed: {}",
                                e.toString());
                    // fall-through to try other methods
                }
            }
            if(getLocal_to_path() != null && getLocal_from_path() != null) {
                runLocalToLocalCopy();
                return;
            }
            if(getLocal_to_path() != null && getFrom_turl() != null) {
                runRemoteToLocalCopy();
                return;
            }
            if(getTo_turl() != null && getLocal_from_path() != null) {
                runLocalToRemoteCopy();
                return;
            }
            if(getFrom_turl() != null && getTo_turl() != null) {
                javaUrlCopy(getFrom_turl().toURL(),
                            getTo_turl().toURL());
                logger.debug("copy succeeded");
                setStateToDone();
            }
            else {
                logger.error("Unknown combination of to/from ursl");
                setStateToFailed("Unknown combination of to/from ursl");
            }
        } catch (IllegalStateTransition | IOException | SRMException | DataAccessException e) {
            throw new NonFatalJobFailure(e.toString());
        }
    }

    private static long last_time;

    public synchronized static long unique_current_time() {
        long time =  System.currentTimeMillis();
        last_time = last_time < time ? time : last_time+1;
        return last_time;
    }

    public void scriptCopy(GlobusURL from, GlobusURL to, GSSCredential credential)
            throws IOException, GSSException
    {
        String proxy_file = null;
        try {
            String command = getConfiguration().getTimeout_script();
            command=command+" "+getConfiguration().getTimeout();
            command=command+" "+getConfiguration().getUrlcopy();
            //command=command+" -username "+ user.getName();
            command = command+" -debug "+getConfiguration().isDebug();
            if(credential != null) {
                try {
                    byte [] data = ((ExtendedGSSCredential)(credential)).export(
                            ExtendedGSSCredential.IMPEXP_OPAQUE);
                    proxy_file = getConfiguration().getProxies_directory()+
                            "/proxy_"+credential.hashCode()+"_at_"+unique_current_time();
                    logger.debug("saving credential "+credential.getName().toString()+
                                         " in proxy_file "+proxy_file);
                    FileOutputStream out = new FileOutputStream(proxy_file);
                    out.write(data);
                    out.close();
                    logger.debug("save succeeded ");
                }
                catch(IOException ioe) {
                    logger.error("saving credentials to "+proxy_file+" failed");
                    logger.error(ioe.toString());
                    proxy_file = null;
                }
            }
            if(proxy_file != null) {
                command = command+" -x509_user_proxy "+proxy_file;
                command = command+" -x509_user_key "+proxy_file;
                command = command+" -x509_user_cert "+proxy_file;
            }
            int tcp_buffer_size = getConfiguration().getTcp_buffer_size();
            if(tcp_buffer_size > 0) {
                command = command+" -tcp_buffer_size "+tcp_buffer_size;
            }
            int buffer_size = getConfiguration().getBuffer_size();
            if(buffer_size > 0) {
                command = command+" -buffer_size "+buffer_size;
            }
            int parallel_streams = getConfiguration().getParallel_streams();
            if(parallel_streams > 0) {
                command = command+" -parallel_streams "+parallel_streams;
            }
            command = command+
                    " -src-protocol "+from.getProtocol();
            if(from.getProtocol().equals("file")) {
                command = command+" -src-host-port localhost";
            }
            else {
                command = command+
                        " -src-host-port "+from.getHost()+":"+from.getPort();
            }
            command = command+
                    " -src-path "+from.getPath()+
                    " -dst-protocol "+to.getProtocol();
            if(to.getProtocol().equals("file")) {
                command = command+" -dst-host-port localhost";
            }
            else {
                command = command+
                        " -dst-host-port "+to.getHost()+":"+to.getPort();
            }
            command = command+
                    " -dst-path "+to.getPath();
            String from_username = from.getUser();
            if(from_username != null) {
                command = command +
                        " -src_username "+from_username;
            }
            String from_pwd = from.getPwd();
            if(from_pwd != null) {
                command = command +
                        " -src_userpasswd "+from_pwd;
            }
            String to_username = to.getUser();
            if(to_username != null) {
                command = command +
                        " -dst_username "+to_username;
            }
            String to_pwd = to.getPwd();
            if(to_pwd != null) {
                command = command +
                        " -dst_userpasswd "+to_pwd;
            }
            String gsiftpclient = getConfiguration().getGsiftpclinet();
            if(gsiftpclient != null) {
                command = command +
                        " -use-kftp "+
                        (gsiftpclient.toLowerCase().contains("kftp"));
            }
            int rc = ShellCommandExecuter.execute(command);
            if(rc == 0) {
                logger.debug("return code = 0, success");
            }
            else {
                logger.debug("return code = "+rc+", failure");
                throw new IOException("return code = "+rc+", failure");
            }
        }
        finally {
            if(proxy_file != null) {
                try {
                    logger.debug(" deleting proxy file"+proxy_file);
                    File f = new File(proxy_file);
                    if(!f.delete() ) {
                        logger.error("error deleting proxy cash "+proxy_file);
                    }
                }
                catch(Exception e) {
                    logger.error("error deleting proxy cash "+proxy_file);
                    logger.error(e.toString());
                }
            }
        }
    }

    public void javaUrlCopy(URL from, URL to) throws IOException
    {
        InputStream in;
        if(from.getProtocol().equals("file")) {
            in = new FileInputStream(from.getPath());
        }
        else {
            in = from.openConnection().getInputStream();
        }
        OutputStream out;
        if(to.getProtocol().equals("file")) {
            out = new FileOutputStream(to.getPath());
        }
        else {
            URLConnection to_connect = to.openConnection();
            to_connect.setDoInput(false);
            to_connect.setDoOutput(true);
            out = to_connect.getOutputStream();
        }
        try {
            int buffer_size = 0;//configuration.getBuffer_size();
            if(buffer_size <=0) {
                buffer_size = 4096;
            }
            byte[] bytes = new byte[buffer_size];
            long total = 0;
            int l;
            while( (l = in.read(bytes)) != -1) {
                total += l;
                out.write(bytes,0,l);
            }
            logger.debug("done, copied "+total +" bytes");
        }
        finally {
            in.close();
            out.close();
        }
    }

    @Override
    protected void stateChanged(State oldState) {
        State state = getState();
        if(state.isFinal()) {
            if (getTransferId() != null && state != State.DONE) {
                getStorage().killRemoteTransfer(getTransferId());
                String toFileId = getToFileId();
                if (toFileId != null) {
                    try {
                        Exception transferError = getTransferError();
                        getStorage().abortPut(getUser(), toFileId, getTo_surl(),
                                              (transferError == null) ? null : transferError.getMessage());
                    } catch (SRMException e) {
                        logger.error("Failed to abort copy: {}", e.getMessage());
                    }
                }
            }
            String remoteRequestId = getRemoteRequestId();
            if (remoteRequestId != null) {
                if (getLocal_from_path() != null) {
                    remoteFileRequestDone(getTo_surl(), remoteRequestId, getRemoteFileId());
                }
                else {
                    remoteFileRequestDone(getFrom_surl(), remoteRequestId, getRemoteFileId());
                }
            }
        }
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getFrom_surl()) || surl.equals(getTo_surl());
    }

    public void remoteFileRequestDone(URI SURL,String remoteRequestId,String remoteFileId) {
        try {
            logger.debug("setting remote file status to Done, SURL="+SURL+" remoteRequestId="+remoteRequestId+
                                 " remoteFileId="+remoteFileId);
            getContainerRequest().remoteFileRequestDone(SURL
                                                                .toString(), remoteRequestId, remoteFileId);
        }
        catch(Exception e) {
            logger.error("set remote file status to done failed, surl = "+SURL+
                                 " requestId = " +remoteRequestId+ " fileId = " +remoteFileId);
        }
    }
    /** Getter for property remoteFileId.
     * @return Value of property remoteFileId.
     *
     */
    public String getRemoteFileId() {
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
    public String getRemoteRequestId() {
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
    public URI getFrom_surl() {
        rlock();
        try {
            return from_surl;
        } finally {
            runlock();
        }
    }
    /**
     * Getter for property to_surl.
     * @return Value of property to_surl.
     */
    public URI getTo_surl() {
        rlock();
        try {
            return to_surl;
        } finally {
            runlock();
        }
    }
    /**
     * Setter for property remoteRequestId.
     * @param remoteRequestId New value of property remoteRequestId.
     */
    public void setRemoteRequestId(String remoteRequestId) {
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
    public void setRemoteFileId(String remoteFileId) {
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
    public String getSpaceReservationId() {
        rlock();
        try {
            return spaceReservationId;
        } finally {
            runlock();
        }
    }

    /**
     * @param transferId the transferId to set
     */
    private void setTransferId(String transferId) {
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
    private Exception getTransferError() {
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
    private void setTransferError(Exception transferError) {
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
                        logger.debug("PutCallbacks success for file {}", fr.getTo_surl());
                        fr.setToFileId(fileId);
                        Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                        try {
                            scheduler.schedule(fr);
                        } catch(Exception ie) {
                            logger.error(ie.toString());
                        }
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            e.getMessage(),
                            e.getStatusCode());
                }
            } catch(IllegalStateTransition e) {
                logger.error("Illegal State Transition: {}", e.getMessage());
            } catch(SRMInvalidRequestException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private static class TheCopyCallbacks implements CopyCallbacks {
        private final long fileRequestJobId;

        public TheCopyCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        private CopyFileRequest getCopyFileRequest()
                throws SRMInvalidRequestException{
            return Job.getJob(fileRequestJobId, CopyFileRequest.class);
        }

        @Override
        public void copyComplete() {
            try {
                CopyFileRequest  copyFileRequest = getCopyFileRequest();
                logger.debug("copy succeeded");
                copyFileRequest.setStateToDone();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }

        @Override
        public void copyFailed(SRMException e) {
            CopyFileRequest  copyFileRequest ;
            try {
                copyFileRequest = getCopyFileRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
                return;
            }
            copyFileRequest.setTransferError(e);
            logger.error("copy failed:");
            logger.error(e.toString());
            State state =  copyFileRequest.getState();
            Scheduler scheduler = Scheduler.getScheduler(copyFileRequest.getSchedulerId());
            if(!state.isFinal() && scheduler != null) {
                try {
                    scheduler.schedule(copyFileRequest);
                }
                catch(InterruptedException | IllegalStateTransition ie) {
                    logger.error(ie.toString());
                }
            }
        }
    }

    public  TCopyRequestFileStatus getTCopyRequestFileStatus() throws SRMInvalidRequestException {
        TCopyRequestFileStatus copyRequestFileStatus = new TCopyRequestFileStatus();
        copyRequestFileStatus.setFileSize(new UnsignedLong(size));
        copyRequestFileStatus.setEstimatedWaitTime((int) (getRemainingLifetime() / 1000));
        copyRequestFileStatus.setRemainingFileLifetime((int) (getRemainingLifetime() / 1000));
        org.apache.axis.types.URI to_surl;
        org.apache.axis.types.URI from_surl;
        try { to_surl= new org.apache.axis.types.URI(getTo_surl().toASCIIString());
        }
        catch (org.apache.axis.types.URI.MalformedURIException e) {
            logger.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }
        try {
            from_surl=new org.apache.axis.types.URI(getFrom_surl().toASCIIString());
        }
        catch (org.apache.axis.types.URI.MalformedURIException e) {
            logger.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }
        copyRequestFileStatus.setSourceSURL(from_surl);
        copyRequestFileStatus.setTargetSURL(to_surl);
        TReturnStatus returnStatus = getReturnStatus();
        if(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, null);
        }
        copyRequestFileStatus.setStatus(returnStatus);
        return copyRequestFileStatus;
    }

    @Override
    public TReturnStatus getReturnStatus() {
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
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        default:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        }
    }

    /**
     * Getter for property transferId.
     * @return Value of property transferId.
     */
    public String getTransferId() {
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
        return extendLifetimeMillis(newLifetime);
    }
}
