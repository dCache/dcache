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
import org.globus.util.GlobusURL;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.sql.SQLException;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.ShellCommandExecuter;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TCopyRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 * @version
 */
public final class CopyFileRequest extends FileRequest<CopyRequest> {

        private static final Logger logger =
                LoggerFactory.getLogger(CopyFileRequest.class);
        private static final String SFN_STRING="?SFN=";
	private URI from_surl;
	private URI to_surl;
	private URI from_turl;
	private URI to_turl;
	private String local_from_path;
	private String local_to_path;
	private long size;
	private String fromFileId;
	private String toFileId;
	private String toParentFileId;
	private transient FileMetaData toParentFmd;
	private String remoteRequestId;
	private String remoteFileId;
	private String transferId;
	private Exception transferError;
	//these are used if the transfer is performed in the pull mode for
	// storage of the space reservation related info
	private String spaceReservationId;
	private boolean weReservedSpace;
	private boolean spaceMarkedAsBeingUsed;

	/** Creates new FileRequest */

	public CopyFileRequest(long requestId,
			       Long  requestCredentalId,
			       String from_surl,
			       String to_surl,
			       String spaceToken,
			       long lifetime,
			       int max_number_of_retries) throws Exception {
		super(requestId,
		      requestCredentalId,
                    lifetime, max_number_of_retries);
		logger.debug("CopyFileRequest");
		this.from_surl = URI.create(from_surl);
		this.to_surl = URI.create(to_surl);
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
		)  throws SQLException {
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
		this.fromFileId =fromFileId;
		this.toFileId = toFileId;
		if(REMOTEREQUESTID != null && (!REMOTEREQUESTID.equalsIgnoreCase("null"))) {
			this.remoteRequestId = REMOTEREQUESTID;
		}
		if(REMOTEFILEID != null && (!REMOTEFILEID.equalsIgnoreCase("null"))) {
			this.remoteFileId = REMOTEFILEID;
		}
		this.spaceReservationId = spaceReservationId;
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
	 * @param absolute_local_from_path New value of property absolute_local_from_path.
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
	 * @param absolute_local_to_path New value of property absolute_local_to_path.
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

	/** Getter for property fromFileId.
	 * @return Value of property fromFileId.
	 *
	 */
	public String getFromFileId() {
        rlock();
        try {
            return fromFileId;
        } finally {
            runlock();
        }

	}

	/** Setter for property fromFileId.
	 * @param fromFileId New value of property fromFileId.
	 *
	 */
	public void setFromFileId(String fromFileId) {
        wlock();
        try {
            this.fromFileId = fromFileId;
        } finally {
            wunlock();
        }

	}

	private void runScriptCopy() throws SRMException, IOException,
                GSSException
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
                                                        new String[] {"gsiftp","http","ftp"});
			}
		}
		if(to == null && getLocal_to_path() != null) {
			if(from.getScheme().equalsIgnoreCase("gsiftp") ||
			   from.getScheme().equalsIgnoreCase("http") ||
			   from.getScheme().equalsIgnoreCase("ftp") ||
			   from.getScheme().equalsIgnoreCase("dcap")) {
                            to =
                                getStorage().getPutTurl(getUser(),
                                                        getTo_surl(),
                                                        new String[] {"gsiftp","http","ftp"});
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

		RequestCredential credential = getCredential();
		if(getToFileId() == null && getToParentFileId() == null) {
            setState(State.ASYNCWAIT,"calling storage.prepareToPut");
			PutCallbacks callbacks = new PutCallbacks(this.getId());
			logger.debug("calling storage.prepareToPut("+getLocal_to_path()+")");
			getStorage().prepareToPut(getUser(),getTo_surl(),
					     callbacks,
					     getContainerRequest().isOverwrite());
			logger.debug("callbacks.waitResult()");
			return;
		}
		logger.debug("known source size is "+size);
		//reserve space even if the size is not known (0) as
		// if only in order to select the pool corectly
		// use 1 instead of 0, since this will cause faulure if there is no space
		// available at all
		// Space manager will account for used size correctly
		// once it becomes available from the pool
		// and space is not reserved
		// or if the space is reserved and we already tried to use this
		// space reservation and failed
		// (releasing previous space reservation)
		//


        // Use pnfs tag for the default space token
        // if the conditions are right
		TAccessLatency accessLatency =
			getContainerRequest().getTargetAccessLatency();
		TRetentionPolicy retentionPolicy =
			getContainerRequest().getTargetRetentionPolicy();
		if (getSpaceReservationId()==null &&
            retentionPolicy==null&&
            accessLatency==null &&
            getToParentFmd().spaceTokens!=null &&
            getToParentFmd().spaceTokens.length>0 ) {
                setSpaceReservationId(Long.toString(getToParentFmd().spaceTokens[0]));
		}

		if (getConfiguration().isReserve_space_implicitely() &&
                getSpaceReservationId() == null) {
            setState(State.ASYNCWAIT,"reserving space");
			long remaining_lifetime =
                    lifetime - ( System.currentTimeMillis() -creationTime);
			logger.debug("reserving space, size="+(size==0?1L:size));
			//
			//the following code allows the inheritance of the
			// retention policy from the directory metatada
			//
			if(retentionPolicy == null &&
               getToParentFmd()!= null &&
               getToParentFmd().retentionPolicyInfo != null ) {
				retentionPolicy = getToParentFmd().retentionPolicyInfo.getRetentionPolicy();
			}

			//
			//the following code allows the inheritance of the
			// access latency from the directory metatada
			//
			if(accessLatency == null &&
               getToParentFmd() != null &&
               getToParentFmd().retentionPolicyInfo != null ) {
				accessLatency = getToParentFmd().retentionPolicyInfo.getAccessLatency();
			}

			SrmReserveSpaceCallbacks callbacks =
                    new TheReserveSpaceCallbacks (getId());
			getStorage().srmReserveSpace(
				getUser(),
				size==0?1L:size,
				remaining_lifetime,
				retentionPolicy == null ? null : retentionPolicy.getValue(),
				accessLatency == null ? null : accessLatency.getValue(),
				null,
				callbacks);
			return;
		}

		if( getSpaceReservationId() != null &&
		    !isSpaceMarkedAsBeingUsed()) {
            setState(State.ASYNCWAIT,"marking space as being used");
			long remaining_lifetime =
                    lifetime - ( System.currentTimeMillis() -creationTime);
			SrmUseSpaceCallbacks  callbacks = new CopyUseSpaceCallbacks(getId());
			getStorage().srmMarkSpaceAsBeingUsed(getUser(),getSpaceReservationId(),getTo_surl(),
							size==0?1:size,
							remaining_lifetime,
							getContainerRequest().isOverwrite(),
							callbacks );
			return;
		}

        getStorage().localCopy(getUser(),getFrom_surl(), getTo_surl());
        setStateToDone();
        }

	private void runRemoteToLocalCopy() throws IllegalStateTransition,
                SRMException, NonFatalJobFailure
        {
		logger.debug("copying from remote to local ");
		RequestCredential credential = getCredential();
		if(getToFileId() == null && getToParentFileId() == null) {
			setState(State.ASYNCWAIT,"calling storage.prepareToPut");
			PutCallbacks callbacks = new PutCallbacks(this.getId());
			logger.debug("calling storage.prepareToPut("+getLocal_to_path()+")");
			getStorage().prepareToPut(getUser(),
                                                  getTo_surl(),
                                                  callbacks,
                                                  getContainerRequest().isOverwrite());
			logger.debug("callbacks.waitResult()");
			return;
		}
		logger.debug("known source size is "+size);
		//reserve space even if the size is not known (0) as
		// if only in order to select the pool corectly
		// use 1 instead of 0, since this will cause faulure if there is no space
		// available at all
		// Space manager will account for used size correctly
		// once it becomes available from the pool
		// and space is not reserved
		// or if the space is reserved and we already tried to use this
		// space reservation and failed
		// (releasing previous space reservation)
		//

        // Use pnfs tag for the default space token
        // if the conditions are right
		TAccessLatency accessLatency =
			getContainerRequest().getTargetAccessLatency();
		TRetentionPolicy retentionPolicy =
			getContainerRequest().getTargetRetentionPolicy();
		if (getSpaceReservationId()==null &&
            retentionPolicy==null&&
            accessLatency==null &&
            getToParentFmd().spaceTokens!=null &&
            getToParentFmd().spaceTokens.length>0 ) {
                setSpaceReservationId(Long.toString(getToParentFmd().spaceTokens[0]));
		}

		if (getConfiguration().isReserve_space_implicitely()&&getSpaceReservationId() == null) {
			setState(State.ASYNCWAIT,"reserving space");
			long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
			logger.debug("reserving space, size="+(size==0?1L:size));
			//
			//the following code allows the inheritance of the
			// retention policy from the directory metatada
			//
			if(retentionPolicy == null && getToParentFmd()!= null && getToParentFmd().retentionPolicyInfo != null ) {
				retentionPolicy = getToParentFmd().retentionPolicyInfo.getRetentionPolicy();
			}
			//
			//the following code allows the inheritance of the
			// access latency from the directory metatada
			//
			if(accessLatency == null && getToParentFmd() != null && getToParentFmd().retentionPolicyInfo != null ) {
				accessLatency = getToParentFmd().retentionPolicyInfo.getAccessLatency();
			}
			SrmReserveSpaceCallbacks callbacks = new TheReserveSpaceCallbacks (getId());
			getStorage().srmReserveSpace(
				getUser(),
				size==0?1L:size,
				remaining_lifetime,
				retentionPolicy == null ? null : retentionPolicy.getValue(),
				accessLatency == null ? null : accessLatency.getValue(),
				null,
				callbacks);
			return;
		}
		if( getSpaceReservationId() != null &&
		    !isSpaceMarkedAsBeingUsed()) {
            setState(State.ASYNCWAIT,"marking space as being used");
			long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
			SrmUseSpaceCallbacks  callbacks = new CopyUseSpaceCallbacks(getId());
			getStorage().srmMarkSpaceAsBeingUsed(getUser(),getSpaceReservationId(),getTo_surl(),
							size==0?1:size,
							remaining_lifetime,
							getContainerRequest().isOverwrite(),
							callbacks );
			return;
		}
		if(getTransferId() == null) {
            setState(State.RUNNINGWITHOUTTHREAD,"started remote transfer, waiting completion");
			TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId());
			if(getSpaceReservationId() != null) {
				setTransferId(getStorage().getFromRemoteTURL(getUser(), getFrom_turl(), getTo_surl(), getUser(), credential.getId(), getSpaceReservationId(), size, copycallbacks));

			}
			else {
				setTransferId(getStorage().getFromRemoteTURL(getUser(), getFrom_turl(), getTo_surl(), getUser(), credential.getId(), copycallbacks));
			}
			long remaining_lifetime =
				this.getCreationTime() +
				this.getLifetime() -
				System.currentTimeMillis() ;
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
			setState(State.RUNNINGWITHOUTTHREAD,"started remote transfer, waiting completion");
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
		} catch (IllegalStateTransition | IOException | SRMException e) {
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
		if(State.isFinalState(state)) {
                        if (getTransferId() != null && state != State.DONE) {
				getStorage().killRemoteTransfer(getTransferId());
			}
                        SRMUser user ;
                        try {
                            user = getUser();
                        } catch (SRMInvalidRequestException ire) {
                            logger.error(ire.toString());
                            return;
                        }
			if(getSpaceReservationId() != null && isWeReservedSpace()) {
				logger.debug("storage.releaseSpace("+getSpaceReservationId()+"\"");
				SrmReleaseSpaceCallbacks callbacks = new TheReleaseSpaceCallbacks(this.getId());
				getStorage().srmReleaseSpace(  user,getSpaceReservationId(),
							  null,
							  callbacks);
			}
			if(getSpaceReservationId() != null &&
			   isSpaceMarkedAsBeingUsed() ) {
				SrmCancelUseOfSpaceCallbacks callbacks =
					new CopyCancelUseOfSpaceCallbacks(getId());
				getStorage().srmUnmarkSpaceAsBeingUsed(user,getSpaceReservationId(),getTo_surl(),callbacks);
			}
			if( getRemoteRequestId() != null ) {
				if(getLocal_from_path() != null ) {
					remoteFileRequestDone(getTo_surl(),getRemoteRequestId(), getRemoteFileId());
				}
				else {
					remoteFileRequestDone(getFrom_surl(),getRemoteRequestId(), getRemoteFileId());
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
	 * Setter for property spaceReservationId.
	 * @param spaceReservationId New value of property spaceReservationId.
	 */
	public void setSpaceReservationId(String spaceReservationId) {
        wlock();
        try {
            this.spaceReservationId = spaceReservationId;
        } finally {
            wunlock();
        }
	}

    /**
     * @return the toParentFileId
     */
    private String getToParentFileId() {
        rlock();
        try {
            return toParentFileId;
        } finally {
            runlock();
        }
    }

    /**
     * @param toParentFileId the toParentFileId to set
     */
    private void setToParentFileId(String toParentFileId) {
        wlock();
        try {
            this.toParentFileId = toParentFileId;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the toParentFmd
     */
    private FileMetaData getToParentFmd() {
        rlock();
        try {
            return toParentFmd;
        } finally {
            runlock();
        }
    }

    /**
     * @param toParentFmd the toParentFmd to set
     */
    private void setToParentFmd(FileMetaData toParentFmd) {
        wlock();
        try {
           this.toParentFmd = toParentFmd;
        } finally {
            wunlock();
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

	private static class PutCallbacks implements PrepareToPutCallbacks {
		final long fileRequestJobId;
		public boolean completed;
		public boolean success;
		public String fileId;
		public FileMetaData fmd;
		public String parentFileId;
		public FileMetaData parentFmd;
		public String error_message;

		public synchronized boolean waitResult(long timeout) {
			long start = System.currentTimeMillis();
			long current = start;
			while(true) {
				if(completed) {
					return success;
				}
				long wait = timeout - (current -start);
				if(wait > 0) {
					try {
						this.wait(wait);
					}
					catch(InterruptedException ie){

					}
				}
				else {
					completed = true;
					success = false;
					error_message = "PutCallbacks wait timeout expired";
					return false;
				}
				current = System.currentTimeMillis();
			}
		}

		public synchronized void complete(boolean success) {
			this.success = success;
			this.completed = true;
			this.notifyAll();
		}

		public PutCallbacks(long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}

		public CopyFileRequest getCopyFileRequest()
                throws SQLException, SRMInvalidRequestException{
			return Job.getJob(fileRequestJobId, CopyFileRequest.class);
		}

		@Override
                public void DuplicationError(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            error_message,
                            TStatusCode.SRM_DUPLICATION_ERROR);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
			complete(false);
		}

		@Override
                public void Error( String error) {
			error_message = error;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setState(State.FAILED,error_message);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
			complete(false);
		}

		@Override
                public void Exception( Exception e) {
			error_message = e.toString();
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setState(State.FAILED,error_message);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e1) {
				logger.error(e1.toString());
			}
			complete(false);
		}

		@Override
                public void GetStorageInfoFailed(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setState(State.FAILED,error_message);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}

				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
			complete(false);
		}


		@Override
                public void StorageInfoArrived(String fileId,
					       FileMetaData fmd,
					       String parentFileId,
					       FileMetaData parentFmd) {
			try {
				CopyFileRequest fr =  getCopyFileRequest();
				logger.debug("StorageInfoArrived: FileId:"+fileId);
				State state = fr.getState();
				if(state == State.ASYNCWAIT) {
					logger.debug("PutCallbacks StorageInfoArrived for file "+fr.getTo_surl()+" fmd ="+fmd);
					fr.setToFileId(fileId);
					fr.setToParentFileId(parentFileId);
					fr.setToParentFmd(parentFmd);
					Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
					try {
						scheduler.schedule(fr);
					}
					catch(Exception ie) {
						logger.error(ie.toString());
					}
				}
				complete(true);
			}
			catch(Exception e){
				logger.error(e.toString());
				complete(false);
			}

		}

		@Override
                public void Timeout() {
			error_message = "PutCallbacks Timeout";
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setState(State.FAILED,error_message);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
			complete(false);
		}

		@Override
                public void InvalidPathError(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            error_message,
                            TStatusCode.SRM_INVALID_PATH);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
			complete(false);
		}

		@Override
                public void AuthorizationError(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(State.FAILED,
                            error_message,
                            TStatusCode.SRM_AUTHORIZATION_FAILURE);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}

				logger.error("PutCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
			complete(false);
		}
	}

	private static class TheCopyCallbacks implements CopyCallbacks {
		private final long fileRequestJobId;
		private boolean completed;
		private boolean success;

		public TheCopyCallbacks(long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}
		public synchronized boolean waitResult(long timeout) {
			long start = System.currentTimeMillis();
			long current = start;
			while(true) {
				if(completed) {
					return success;
				}
				long wait = timeout - (current -start);
				if(wait > 0) {
					try {
						this.wait(wait);
					}
					catch(InterruptedException ie) {
					}
				}
				else {
					completed = true;
					success = false;
					return false;
				}
				current = System.currentTimeMillis();
			}
		}

		public synchronized void complete(boolean success) {
			this.success = success;
			this.completed = true;
			this.notifyAll();
		}

		private CopyFileRequest getCopyFileRequest()
                throws SRMInvalidRequestException{
			return Job.getJob(fileRequestJobId, CopyFileRequest.class);
		}

		@Override
                public void copyComplete(FileMetaData fmd) {
            try {
                CopyFileRequest  copyFileRequest = getCopyFileRequest();
                logger.debug("copy succeeded");
                copyFileRequest.setStateToDone();
                complete(true);
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
			if(!State.isFinalState(state) && scheduler != null) {
				try {
					scheduler.schedule(copyFileRequest);
				}
				catch(InterruptedException | IllegalStateTransition ie) {
					logger.error(ie.toString());
				}
                        }
			complete(false);
		}
	}

	public  TCopyRequestFileStatus getTCopyRequestFileStatus() throws SQLException {
		TCopyRequestFileStatus copyRequestFileStatus = new TCopyRequestFileStatus();
		copyRequestFileStatus.setFileSize(new UnsignedLong(size));
		copyRequestFileStatus.setEstimatedWaitTime((int)(getRemainingLifetime()/1000));
		copyRequestFileStatus.setRemainingFileLifetime((int)(getRemainingLifetime()/1000));
		org.apache.axis.types.URI to_surl;
		org.apache.axis.types.URI from_surl;
		try { to_surl= new org.apache.axis.types.URI(getTo_surl().toASCIIString());
		}
		catch (Exception e) {
			logger.error(e.toString());
			throw new SQLException("wrong surl format");
		}
		try {
			from_surl=new org.apache.axis.types.URI(getFrom_surl().toASCIIString());
		}
		catch (Exception e) {
			logger.error(e.toString());
			throw new SQLException("wrong surl format");
		}
		copyRequestFileStatus.setSourceSURL(from_surl);
		copyRequestFileStatus.setTargetSURL(to_surl);
		TReturnStatus returnStatus = getReturnStatus();
		if(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
			returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
		}
		copyRequestFileStatus.setStatus(returnStatus);
		return copyRequestFileStatus;
	}

	@Override
        public TReturnStatus getReturnStatus() {
		TReturnStatus returnStatus = new TReturnStatus();
		State state = getState();
		returnStatus.setExplanation(state.toString());
		if(getStatusCode() != null) {
			returnStatus.setStatusCode(getStatusCode());
		}
		else if(state == State.DONE) {
			returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		}
		else if(state == State.READY) {
			returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
		}
		else if(state == State.TRANSFERRING) {
			returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
		}
		else if(state == State.FAILED) {
			returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
			returnStatus.setExplanation("FAILED: "+getErrorMessage());
		}
		else if(state == State.CANCELED ) {
			returnStatus.setStatusCode(TStatusCode.SRM_ABORTED);
		}
		else if(state == State.TQUEUED ) {
			returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
		}
		else if(state == State.RUNNING ||
			state == State.RQUEUED ||
			state == State.ASYNCWAIT ) {
			returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
		}
		else {
			returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
		}
		return returnStatus;
	}


	public TSURLReturnStatus getTSURLReturnStatus(URI surl)
                throws SQLException
        {
		if(surl == null) {
			surl = getTo_surl();
		}
		org.apache.axis.types.URI tsurl;
		try {
			tsurl=new org.apache.axis.types.URI(surl.toASCIIString());
		}
		catch (Exception e) {
			logger.error(e.toString());
			throw new SQLException("wrong surl format");
		}
		TReturnStatus returnStatus =  getReturnStatus();
		if(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
			returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
		}
		TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
		surlReturnStatus.setSurl(tsurl);
		surlReturnStatus.setStatus(returnStatus);
		return surlReturnStatus;
	}

	public boolean isWeReservedSpace() {
        rlock();
        try {
            return weReservedSpace;
        } finally {
            runlock();
        }
	}

	public void setWeReservedSpace(boolean weReservedSpace) {
        wlock();
        try {
    		this.weReservedSpace = weReservedSpace;
        } finally {
            wunlock();
        }
	}

	public boolean isSpaceMarkedAsBeingUsed() {
        rlock();
        try {
    		return spaceMarkedAsBeingUsed;
        } finally {
            runlock();
        }
	}

	public void setSpaceMarkedAsBeingUsed(boolean spaceMarkedAsBeingUsed) {
        wlock();
        try {
    		this.spaceMarkedAsBeingUsed = spaceMarkedAsBeingUsed;
        } finally {
            wunlock();
        }
	}

	public static class TheReserveSpaceCallbacks implements SrmReserveSpaceCallbacks {
		private final long fileRequestJobId;
		public CopyFileRequest getCopyFileRequest()
                        throws SQLException, SRMInvalidRequestException
                {
		    return Job.getJob(fileRequestJobId, CopyFileRequest.class);
		}

		public TheReserveSpaceCallbacks(long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}

		@Override
                public void ReserveSpaceFailed(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setState(State.FAILED,reason);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyReserveSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
		@Override
                public void NoFreeSpace(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_NO_FREE_SPACE);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyReserveSpaceCallbacks error NoFreeSpace : "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
                public void SpaceReserved(String spaceReservationToken, long reservedSpaceSize) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				logger.debug("Space Reserved: spaceReservationToken:"+spaceReservationToken);
				State state = fr.getState();
				if(state == State.ASYNCWAIT) {
					logger.debug("CopyReserveSpaceCallbacks Space Reserved for file "+fr.getTo_surl());
					fr.setSpaceReservationId(spaceReservationToken);
					fr.setWeReservedSpace(true);
					Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
					try {
						scheduler.schedule(fr);
					}
					catch(Exception ie) {
						logger.error(ie.toString());
					}
				}
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
                public void ReserveSpaceFailed(Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				String error = e.toString();
				try {
                    fr.setState(State.FAILED,error);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyReserveSpaceCallbacks exception");
				logger.error(e.toString());
			}
			catch(Exception e1) {
				logger.error(e1.toString());
			}
		}
	}


	private  static class TheReleaseSpaceCallbacks implements  SrmReleaseSpaceCallbacks {
		private final long fileRequestJobId;

		public TheReleaseSpaceCallbacks(long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}

		public CopyFileRequest getCopyFileRequest()
                throws SQLException, SRMInvalidRequestException {
		    return Job.getJob(fileRequestJobId, CopyFileRequest.class);
		}

		@Override
                public void ReleaseSpaceFailed( String error) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.setSpaceReservationId(null);
				logger.error("TheReleaseSpaceCallbacks error: "+ error);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
                public void ReleaseSpaceFailed( Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.setSpaceReservationId(null);
				logger.error("TheReleaseSpaceCallbacks exception");
				logger.error(e.toString());
			}
			catch(Exception e1) {
				logger.error(e1.toString());
			}
		}

		public void Timeout() {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.setSpaceReservationId(null);
				logger.error("TheReleaseSpaceCallbacks Timeout");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
                public void SpaceReleased(String spaceReservationToken, long remainingSpaceSize) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				logger.debug("TheReleaseSpaceCallbacks: SpaceReleased");
				fr.setSpaceReservationId(null);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
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
		String spaceToken =getSpaceReservationId();

		if(!getConfiguration().isReserve_space_implicitely() ||
		   spaceToken == null ||
		   !isWeReservedSpace()) {
			return extendLifetimeMillis(newLifetime);
		}
		newLifetime = extendLifetimeMillis(newLifetime);
		if( remainingLifetime >= newLifetime) {
			return remainingLifetime;
		}
		SRMUser user = getUser();
		return getStorage().srmExtendReservationLifetime(user,spaceToken,newLifetime);
	}

	public static class CopyUseSpaceCallbacks implements SrmUseSpaceCallbacks {
		private final long fileRequestJobId;

		public CopyFileRequest getCopyFileRequest()
                throws SQLException, SRMInvalidRequestException{
			return Job.getJob(fileRequestJobId, CopyFileRequest.class);
		}

		public CopyUseSpaceCallbacks(long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}

		@Override
                public void SrmUseSpaceFailed( Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				String error = e.toString();
				try {
                    fr.setState(State.FAILED,error);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyUseSpaceCallbacks exception");
				logger.error(e.toString());
			}
			catch(Exception e1) {
				logger.error(e1.toString());
			}
		}

		@Override
                public void SrmUseSpaceFailed(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setState(State.FAILED,reason);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyUseSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
		/**
		 * call this if space reservation exists, but has no free space
		 */
		@Override
                public void SrmNoFreeSpace(String reason){
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_NO_FREE_SPACE);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyUseSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
		/**
		 * call this if space reservation exists, but has been released
		 */
		@Override
                public void SrmReleased(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_NO_FREE_SPACE);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyUseSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
		/**
		 * call this if space reservation exists, but not authorized
		 */
		@Override
                public void SrmNotAuthorized(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_AUTHORIZATION_FAILURE);
				}
				catch(IllegalStateTransition ist) {
					logger.error("can not fail state:"+ist);
				}
				logger.error("CopyUseSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
		/**
		 * call this if space reservation exists, but has been released
		 */
		@Override
                public void SrmExpired(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_SPACE_LIFETIME_EXPIRED);
				}
				catch(IllegalStateTransition ist) {
					logger.error("Illegal State Transition : " +ist.getMessage());
				}
				logger.error("CopyUseSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
                public void SpaceUsed() {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				logger.debug("Space Marked as Being Used");
				State state = fr.getState();
				if(state == State.ASYNCWAIT) {
					logger.debug("CopyUseSpaceCallbacks Space Marked as Being Used for file "+fr.getToURL());
					fr.setSpaceMarkedAsBeingUsed(true);
					Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
					try {
						scheduler.schedule(fr);
					}
					catch(Exception ie) {
						logger.error(ie.toString());
					}
				}
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
	}

	public static class CopyCancelUseOfSpaceCallbacks implements SrmCancelUseOfSpaceCallbacks {
		private final long fileRequestJobId;

		public CopyFileRequest getCopyFileRequest()
                throws SQLException, SRMInvalidRequestException {
			return Job.getJob(fileRequestJobId, CopyFileRequest.class);
		}

		public CopyCancelUseOfSpaceCallbacks(long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}

		@Override
                public void CancelUseOfSpaceFailed( Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				String error = e.toString();
				logger.error("CopyCancelUseOfSpaceCallbacks exception",e);
			}
			catch(Exception e1) {
				logger.error(e1.toString());
			}
		}

		@Override
                public void CancelUseOfSpaceFailed(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				logger.error("CopyCancelUseOfSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
                public void UseOfSpaceSpaceCanceled() {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				logger.debug("Umarked Space as Being Used");
			}
			catch(Exception e) {
				logger.error(e.toString());
			}
		}
	}
}
