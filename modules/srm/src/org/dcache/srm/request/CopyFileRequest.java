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

//import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
//import java.net.URLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.SRMException;
//import org.dcache.srm.ReleaseSpaceCallbacks;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.globus.util.GlobusURL;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
//import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Job;
//import org.dcache.srm.scheduler.JobCreator;
import org.dcache.srm.scheduler.JobStorage;
//import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.ietf.jgss.GSSCredential;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.dcache.srm.util.ShellCommandExecuter;
import org.dcache.srm.v2_2.*;
import org.apache.axis.types.URI;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMInvalidRequestException;
import org.apache.log4j.Logger;


/**
 *
 * @author  timur
 * @version
 */
public class CopyFileRequest extends FileRequest {
    
    private static final Logger logger = Logger.getLogger(CopyFileRequest.class);
	private String from_url;
	private String to_url;
	private GlobusURL from_turl;
	private GlobusURL to_turl;
	private String local_from_path;
	private String local_to_path;
	private long size = 0;
	private String fromFileId;
	private String toFileId;
	public FileMetaData toFmd;
	public String toParentFileId;
	public FileMetaData toParentFmd;
	private String remoteRequestId;
	private String remoteFileId;
	private String transferId;
	private Exception transferError;
	//these are used if the transfer is performed in the pull mode for 
	// storage of the space reservation related info
	private String spaceReservationId;
	private boolean weReservedSpace;
	private boolean spaceMarkedAsBeingUsed=false;
	
	/** Creates new FileRequest */
    
	public CopyFileRequest(Long requestId,
			       Long  requestCredentalId,
			       Configuration configuration,
			       String from_url,
			       String to_url,
			       String spaceToken,
			       long lifetime,
			       JobStorage jobStorage,
			       AbstractStorageElement storage,
			       int max_number_of_retries) throws Exception {
		super(requestId, 
		      requestCredentalId,
                    configuration, lifetime, jobStorage,max_number_of_retries);
		say("CopyFileRequest");
		this.from_url = from_url;
		this.to_url = to_url;
		this.spaceReservationId = spaceToken;
		say("constructor from_url=" +from_url+" to_url="+to_url);
	}
	
	/**
	 * restore constructore, used for restoring the existing
	 * file request from the database
	 */
	
	public CopyFileRequest(
		Long id,
		Long nextJobId,
		JobStorage jobStorage,
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
		Long requestId,
		Long requestCredentalId,
		String statusCodeString,            
		Configuration configuration,
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
		)  throws java.sql.SQLException {
		super(id,
		      nextJobId,
		      jobStorage,
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
		      statusCodeString,            
		      configuration);
		this.from_url = FROMURL;
		this.to_url = TOURL;
		try {
			if(FROMTURL != null && (!FROMTURL.equalsIgnoreCase("null"))) {
				this.from_turl = new GlobusURL(FROMTURL);
			}
			if(TOTURL != null && (!TOTURL.equalsIgnoreCase("null"))) {
				this.to_turl = new GlobusURL(TOTURL);
			}
		}
		catch(MalformedURLException murle) {
			throw new IllegalArgumentException(murle.toString());
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
    
	public void say(String s) {
		if(storage != null) {
			storage.log("CopyFileRequest #"+getId()+": "+s);
		}
	}
    
	public void esay(String s) {
		if(storage != null) {
			storage.elog("CopyFileRequest #"+getId()+": "+s);
		}
	}
	
	public void esay(Throwable t) {
		if(storage != null) {
			storage.elog(t);
		}
	}
	
	public void done() {
		say("done()");
	}
	
	public void error() {
		done();
	}
    
	public RequestFileStatus getRequestFileStatus() {
		RequestFileStatus rfs = new RequestFileStatus();
		rfs.fileId = getId().intValue();
		rfs.SURL = from_url;
		rfs.size = 0;
		rfs.TURL = to_url;
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
		return to_url;
	}
    
	public String getFromURL() {
		return from_url;
	}
    
	public String getFromPath() throws java.net.MalformedURLException {
		String path = new GlobusURL(from_url).getPath();
		int indx=path.indexOf(SFN_STRING);
		if( indx != -1) {
			path=path.substring(indx+SFN_STRING.length());
		}
		if(!path.startsWith("/")) {
			path = "/"+path;
		}
		say("getFromPath() returns "+path);
		return path;
	}
    
	public String getToPath() throws java.net.MalformedURLException {
		String path = new GlobusURL(to_url).getPath();
		int indx=path.indexOf(SFN_STRING);
		if( indx != -1) {
			path=path.substring(indx+SFN_STRING.length());
		}
		if(!path.startsWith("/")) {
			path = "/"+path;
		}
		say("getToPath() returns "+path);
		return path;
	}
    
	/** Getter for property from_turl.
	 * @return Value of property from_turl.
	 */
	public org.globus.util.GlobusURL getFrom_turl() {
		return from_turl;
	}
    
	/** Setter for property from_turl.
	 * @param from_turl New value of property from_turl.
	 */
	public void setFrom_turl(org.globus.util.GlobusURL from_turl) {
		this.from_turl = from_turl;
	}
    
	/** Getter for property to_turl.
	 * @return Value of property to_turl.
	 */
	public org.globus.util.GlobusURL getTo_turl() {
		return to_turl;
	}
	
	/** Setter for property to_turl.
	 * @param to_turl New value of property to_turl.
	 */
	public void setTo_turl(org.globus.util.GlobusURL to_turl) {
		this.to_turl = to_turl;
	}
	
	/** Getter for property size.
	 * @return Value of property size.
	 */
	public long getSize() {
		return size;
	}
	
	/** Setter for property size.
	 * @param size New value of property size.
	 */
	public void setSize(long size) {
		this.size = size;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(" CopyFileRequest ");
		sb.append(" id =").append(getId());
		sb.append(" FromSurl=").append(from_url);
		sb.append(" FromTurl=").append(from_turl==null?"null":from_turl.getURL());
		sb.append(" toSurl=").append(to_url);
		sb.append(" toTurl=").append(to_turl==null?"null":to_turl.getURL());
		return sb.toString();
	}
	
	/** Getter for property absolute_local_from_path.
	 * @return Value of property absolute_local_from_path.
	 */
	public String getLocal_from_path() {
		return local_from_path;
	}
	
	/** Setter for property absolute_local_from_path.
	 * @param absolute_local_from_path New value of property absolute_local_from_path.
	 */
	public void setLocal_from_path(String local_from_path) {
		this.local_from_path = local_from_path;
	}
	
	/** Getter for property absolute_local_to_path.
	 * @return Value of property absolute_local_to_path.
	 */
	public String getLocal_to_path() {
		return local_to_path;
	}
	
	/** Setter for property absolute_local_to_path.
	 * @param absolute_local_to_path New value of property absolute_local_to_path.
	 */
	public void setLocal_to_path( String local_to_path) {
		this.local_to_path = local_to_path;
	}
	
	/** Getter for property toFileId.
	 * @return Value of property toFileId.
	 *
	 */
	public String getToFileId() {
		return toFileId;
	}
	
	/** Setter for property toFileId.
	 * @param toFileId New value of property toFileId.
	 *
	 */
	public void setToFileId(String toFileId) {
		this.toFileId = toFileId;
	}
	
	/** Getter for property fromFileId.
	 * @return Value of property fromFileId.
	 *
	 */
	public String getFromFileId() {
		return fromFileId;
	}
	
	/** Setter for property fromFileId.
	 * @param fromFileId New value of property fromFileId.
	 *
	 */
	public void setFromFileId(String fromFileId) {
		this.fromFileId = fromFileId;
	}
	
	private void runScriptCopy() throws Exception {
		GlobusURL from =from_turl;
		GlobusURL to = to_turl;
		if(from == null && local_from_path != null ) {
			if(to.getProtocol().equalsIgnoreCase("gsiftp") ||
			   to.getProtocol().equalsIgnoreCase("http") ||
			   to.getProtocol().equalsIgnoreCase("ftp") ||
			   to.getProtocol().equalsIgnoreCase("dcap")) {
				//need to add support for getting
				String fromturlstr = storage.getGetTurl(getUser(),local_from_path,new String[]
					{"gsiftp","http","ftp"});
				from = new GlobusURL(fromturlstr);
			}
		}
		if(to == null && local_to_path != null) {
			if(from.getProtocol().equalsIgnoreCase("gsiftp") ||
			   from.getProtocol().equalsIgnoreCase("http") ||
			   from.getProtocol().equalsIgnoreCase("ftp") ||
			   from.getProtocol().equalsIgnoreCase("dcap")) {
				String toturlstr = storage.getPutTurl(getUser(),local_to_path,new String[]
					{"gsiftp","http","ftp"});
				to = new GlobusURL(toturlstr);
			}
		}
		if(from ==null || to == null) {
			String error = "could not resolve either source or destination"+
				" from = "+from+" to = "+to;
			esay(error);
			throw new SRMException(error);
		}
		say("calling scriptCopy("+from.getURL()+","+to.getURL()+")");
		RequestCredential credential = getCredential();
		scriptCopy(from,to,credential.getDelegatedCredential());
		setStateToDone();
	}

	private void runLocalToLocalCopy() throws Exception {
		say("copying from local to local ");
        FileMetaData fmd ;
        try {
            fmd = storage.getFileMetaData(getUser(), local_from_path);
        } catch (SRMException srme) {
            try {
                synchronized (this) {
                    State state = getState();
                    if (!State.isFinalState(state)) {
                        setStatusCode(TStatusCode.SRM_INVALID_PATH);
                        setState(State.FAILED, srme.getMessage());
                    }
                }
            } catch (IllegalStateTransition ist) {
                esay("can not fail state:" + ist);
            }
            return;

        }
        size = fmd.size;

		RequestCredential credential = getCredential();
		if(toFileId == null && toParentFileId == null) {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.ASYNCWAIT,"calling storage.prepareToPut");
				}
				else {
					throw new org.dcache.srm.scheduler.FatalJobFailure("request state is a final state");
				}
			}
			PutCallbacks callbacks = new PutCallbacks(this.getId());
			say("calling storage.prepareToPut("+local_to_path+")");
			storage.prepareToPut(getUser(),
					     local_to_path,
					     callbacks,
					     ((CopyRequest)getRequest()).isOverwrite());
			say("callbacks.waitResult()");
			return;
		}
		say("known source size is "+size);
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
			((CopyRequest)getRequest()).getTargetAccessLatency();
		TRetentionPolicy retentionPolicy =
			((CopyRequest)getRequest()).getTargetRetentionPolicy();
		if (spaceReservationId==null &&
            retentionPolicy==null&&
            accessLatency==null &&
            toParentFmd.spaceTokens!=null &&
            toParentFmd.spaceTokens.length>0 ) {
                spaceReservationId=Long.toString(toParentFmd.spaceTokens[0]);
		}

		if (configuration.isReserve_space_implicitely() &&
                spaceReservationId == null) {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.ASYNCWAIT,"reserving space");
				}
				else {
					throw new org.dcache.srm.scheduler.FatalJobFailure("request state is a final state");
				}
			}

			long remaining_lifetime =
                    lifetime - ( System.currentTimeMillis() -creationTime);
			say("reserving space, size="+(size==0?1L:size));
			//
			//the following code allows the inheritance of the
			// retention policy from the directory metatada
			//
			if(retentionPolicy == null && 
               toParentFmd!= null &&
               toParentFmd.retentionPolicyInfo != null ) {
				retentionPolicy = toParentFmd.retentionPolicyInfo.getRetentionPolicy();
			}

			//
			//the following code allows the inheritance of the
			// access latency from the directory metatada
			//
			if(accessLatency == null && 
               toParentFmd != null &&
               toParentFmd.retentionPolicyInfo != null ) {
				accessLatency = toParentFmd.retentionPolicyInfo.getAccessLatency();
			}

			SrmReserveSpaceCallbacks callbacks =
                    new TheReserveSpaceCallbacks (getId());
			storage.srmReserveSpace(
				getUser(),
				size==0?1L:size,
				remaining_lifetime,
				retentionPolicy == null ? null : retentionPolicy.getValue(),
				accessLatency == null ? null : accessLatency.getValue(),
				null,
				callbacks);
			return;
		}

		if( spaceReservationId != null &&
		    !spaceMarkedAsBeingUsed) {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.ASYNCWAIT,"marking space as being used");
				}
			}
			long remaining_lifetime =
                    lifetime - ( System.currentTimeMillis() -creationTime);
			SrmUseSpaceCallbacks  callbacks = new CopyUseSpaceCallbacks(getId());
			storage.srmMarkSpaceAsBeingUsed(getUser(),
							spaceReservationId,
							local_to_path,
							size==0?1:size,
							remaining_lifetime,
							((CopyRequest)getRequest()).isOverwrite(),
							callbacks );
			return;
		}

        storage.localCopy(getUser(),local_from_path,local_to_path);
        setStateToDone();
        return;
	}
    
	private void runRemoteToLocalCopy() throws Exception {
		say("copying from remote to local ");
		RequestCredential credential = getCredential();
		if(toFileId == null && toParentFileId == null) {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.ASYNCWAIT,"calling storage.prepareToPut");
				} 
				else {
					throw new org.dcache.srm.scheduler.FatalJobFailure("request state is a final state");
				}
			}
			PutCallbacks callbacks = new PutCallbacks(this.getId());
			say("calling storage.prepareToPut("+local_to_path+")");
			storage.prepareToPut(getUser(), 
					     local_to_path, 
					     callbacks,
					     ((CopyRequest)getRequest()).isOverwrite());
			say("callbacks.waitResult()");
			return;
		}
		say("known source size is "+size);
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
			((CopyRequest)getRequest()).getTargetAccessLatency();
		TRetentionPolicy retentionPolicy =
			((CopyRequest)getRequest()).getTargetRetentionPolicy();
		if (spaceReservationId==null &&
            retentionPolicy==null&&
            accessLatency==null &&
            toParentFmd.spaceTokens!=null &&
            toParentFmd.spaceTokens.length>0 ) {
                spaceReservationId=Long.toString(toParentFmd.spaceTokens[0]);
		}

		if (configuration.isReserve_space_implicitely()&&spaceReservationId == null) { 
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.ASYNCWAIT,"reserving space");
				} 
				else {
					throw new org.dcache.srm.scheduler.FatalJobFailure("request state is a final state");
				}
			}
			long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
			say("reserving space, size="+(size==0?1L:size));
			//
			//the following code allows the inheritance of the 
			// retention policy from the directory metatada
			//
			if(retentionPolicy == null && toParentFmd!= null && toParentFmd.retentionPolicyInfo != null ) {
				retentionPolicy = toParentFmd.retentionPolicyInfo.getRetentionPolicy();
			}
			//
			//the following code allows the inheritance of the 
			// access latency from the directory metatada
			//
			if(accessLatency == null && toParentFmd != null && toParentFmd.retentionPolicyInfo != null ) {
				accessLatency = toParentFmd.retentionPolicyInfo.getAccessLatency();
			}
			SrmReserveSpaceCallbacks callbacks = new TheReserveSpaceCallbacks (getId());
			storage.srmReserveSpace(
				getUser(), 
				size==0?1L:size, 
				remaining_lifetime, 
				retentionPolicy == null ? null : retentionPolicy.getValue(),
				accessLatency == null ? null : accessLatency.getValue(),
				null,
				callbacks);
			return;
		}
		if( spaceReservationId != null &&
		    !spaceMarkedAsBeingUsed) {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.ASYNCWAIT,"marking space as being used");
				}
			}
			long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
			SrmUseSpaceCallbacks  callbacks = new CopyUseSpaceCallbacks(getId());
			storage.srmMarkSpaceAsBeingUsed(getUser(),
							spaceReservationId,
							local_to_path,
							size==0?1:size,
							remaining_lifetime,
							((CopyRequest)getRequest()).isOverwrite(),
							callbacks );
			return;
		}
		if(transferId == null) {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.RUNNINGWITHOUTTHREAD,"started remote transfer, waiting completion");
					}
				else {
					throw new org.dcache.srm.scheduler.FatalJobFailure("request state is a final state");
				}
			}
			TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId());
			if(spaceReservationId != null) {
				transferId = storage.getFromRemoteTURL(getUser(),
								       from_turl.getURL(),
								       local_to_path, 
								       getUser(),
								       credential.getId(), 
								       spaceReservationId.toString(),
								       size,
								       copycallbacks);
				
			} 
			else {
				transferId = storage.getFromRemoteTURL(getUser(),
								       from_turl.getURL(),
								       local_to_path, 
								       getUser(),
								       credential.getId(),
								       copycallbacks);
			}
			long remaining_lifetime = 
				this.getCreationTime() + 
				this.getLifetime() -
				System.currentTimeMillis() ;
			saveJob();
			return;
		}
		// transfer id is not null and we are scheduled
		// there was some kind of error durign the transfer
		else {
			storage.killRemoteTransfer(transferId);
			transferId = null;
			throw new org.dcache.srm.scheduler.NonFatalJobFailure(transferError);
		}
	}
    
	private void setStateToDone(){
		synchronized(this) {
			State s = getState();
			if(State.isFinalState(s)) {
				return;
			}
			try {
				setState(State.DONE, "setStateToDone called");
                try {
                    ((CopyRequest)getRequest()).fileRequestCompleted();
                } catch (SRMInvalidRequestException ire) {
                    esay(ire);
                }
			}
			catch(IllegalStateTransition ist) {
				esay(ist);
			}
		}
	}
    
	private void setStateToFailed(String error) throws Exception {
		synchronized(this) {
			State s = getState();
			if(State.isFinalState(s)) {
				return;
			}
			try {
				setState(State.FAILED, error);
			}
			catch(IllegalStateTransition ist) {
				esay(ist);
			}
		}
		((CopyRequest)getRequest()).fileRequestCompleted();
	}
    
	private void runLocalToRemoteCopy() throws Exception {
		if(transferId == null) {
			say("copying using storage.putToRemoteTURL");
			RequestCredential credential = getCredential();
			TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId());
			transferId = storage.putToRemoteTURL(getUser(),
							     local_from_path,
							     to_turl.getURL(),
							     getUser(),
							     credential.getId(),
							     copycallbacks);
			long remaining_lifetime = this.getCreationTime() + this.getLifetime() -System.currentTimeMillis() ;
			setState(State.RUNNINGWITHOUTTHREAD,"started remote transfer, waiting completion");
			saveJob();
			return;
		}      
		// transfer id is not null and we are scheduled
		// there was some kind of error durign the transfer
		else {
			storage.killRemoteTransfer(transferId);
			transferId = null;
			throw new org.dcache.srm.scheduler.NonFatalJobFailure(transferError);
		}
	}
    
	public void run() throws NonFatalJobFailure, FatalJobFailure{
		say("copying " );
		try {
			if(from_turl != null && from_turl.getProtocol().equalsIgnoreCase("dcap")  ||
			   to_turl != null && to_turl.getProtocol().equalsIgnoreCase("dcap") ||
			   configuration.isUseUrlcopyScript()) {
				try {
					runScriptCopy();
					return;
				}
				catch(Exception e) {
					esay(e);
					esay("copying using script failed, trying java");
				}
			}
			if(local_to_path != null && local_from_path != null) {
                runLocalToLocalCopy();
				return;
			}
			if(local_to_path != null && from_turl != null) {
				runRemoteToLocalCopy();
				return;
			}
			if(to_turl != null && local_from_path != null) {
				runLocalToRemoteCopy();
				return;
			}
			if(from_turl != null && to_turl != null) {
				URL fromURL = new URL(from_turl.getURL());
				URL toURL   = new URL(to_turl.getURL());
				javaUrlCopy(fromURL,toURL);
				say("copy succeeded");
				setStateToDone();
				return;
			}
			else {
				esay("Unknown combination of to/from ursl");
				setStateToFailed("Unknown combination of to/from ursl");
			}
		}
		catch(Exception e) {
			esay(e);
			esay("copy  failed");
			throw new NonFatalJobFailure(e.toString());
		}
		catch(Throwable t) {
			throw new FatalJobFailure(t.toString());
		}
	}
    
	private static long last_time=0L;
	private static final long serialVersionUID = 1749445378403850845L;

	public synchronized static long unique_current_time() {
		long time =  System.currentTimeMillis();
		last_time = last_time < time ? time : last_time+1;
		return last_time;
	}
    
	public void scriptCopy(GlobusURL from, GlobusURL to, GSSCredential credential) throws Exception {
		String proxy_file = null;
		try {
			String command = configuration.getTimeout_script();
			command=command+" "+configuration.getTimeout();
			command=command+" "+configuration.getUrlcopy();
			//command=command+" -username "+ user.getName();
			command = command+" -debug "+configuration.isDebug();
			if(credential != null) {
				try {
					byte [] data = ((ExtendedGSSCredential)(credential)).export(
						ExtendedGSSCredential.IMPEXP_OPAQUE);
					proxy_file = configuration.getProxies_directory()+
						"/proxy_"+credential.hashCode()+"_at_"+unique_current_time();
					say("saving credential "+credential.getName().toString()+
					    " in proxy_file "+proxy_file);
					FileOutputStream out = new FileOutputStream(proxy_file);
					out.write(data);
					out.close();
					say("save succeeded ");
				}
				catch(IOException ioe) {
					esay("saving credentials to "+proxy_file+" failed");
					esay(ioe);
					proxy_file = null;
				}
			}
			if(proxy_file != null) {
				command = command+" -x509_user_proxy "+proxy_file;
				command = command+" -x509_user_key "+proxy_file;
				command = command+" -x509_user_cert "+proxy_file;
			}
			int tcp_buffer_size = configuration.getTcp_buffer_size();
			if(tcp_buffer_size > 0) {
				command = command+" -tcp_buffer_size "+tcp_buffer_size;
			}
			int buffer_size = configuration.getBuffer_size();
			if(buffer_size > 0) {
				command = command+" -buffer_size "+buffer_size;
			}
			int parallel_streams = configuration.getParallel_streams();
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
			String gsiftpclient = configuration.getGsiftpclinet();
			if(gsiftpclient != null) {
				command = command +
					" -use-kftp "+
					(gsiftpclient.toLowerCase().indexOf("kftp") != -1);
			}
			int rc = ShellCommandExecuter.execute(command);
			if(rc == 0) {
				say("return code = 0, success");
			}
			else {
				say("return code = "+rc+", failure");
				throw new java.io.IOException("return code = "+rc+", failure");
			}
		}
		finally {
			if(proxy_file != null) {
				try {
					say(" deleting proxy file"+proxy_file);
					java.io.File f = new java.io.File(proxy_file);
					f.delete();
				}
				catch(Exception e) {
					esay("error deleting proxy cash "+proxy_file);
					esay(e);
				}
			}
		}
	}
	
	public void javaUrlCopy(URL from, URL to) throws Exception {
		try {
			InputStream in = null;
			if(from.getProtocol().equals("file")) {
				in = new FileInputStream(from.getPath());
			}
			else {
				in = from.openConnection().getInputStream();
			}
			OutputStream out = null;
			if(to.getProtocol().equals("file")) {
				out = new FileOutputStream(to.getPath());
			}
			else {
				java.net.URLConnection to_connect = to.openConnection();
				to_connect.setDoInput(false);
				to_connect.setDoOutput(true);
				out = to_connect.getOutputStream();
			}
			try {
				int buffer_size = 0;//configuration.getBuffer_size();
				if(buffer_size <=0) buffer_size = 4096;
				byte[] bytes = new byte[buffer_size];
				long total = 0;
				int l;
				while( (l = in.read(bytes)) != -1) {
					total += l;
					out.write(bytes,0,l);
				}
				say("done, copied "+total +" bytes");
			} 
			finally {
				in.close();
				out.close();
			}
		}
		catch(Exception e) {
			say("failure : "+e.getMessage());
			throw e;
		}
	}
	
	protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
		State state = getState();
		if(State.isFinalState(state)) {
			if( transferId != null)           {
				storage.killRemoteTransfer(transferId);
			}
            SRMUser user ;
            try {
                user = getUser();
            } catch (SRMInvalidRequestException ire) {
                esay(ire);
                return;
            }
			if(spaceReservationId != null && weReservedSpace) {
				say("storage.releaseSpace("+spaceReservationId+"\"");
				SrmReleaseSpaceCallbacks callbacks = new TheReleaseSpaceCallbacks(this.getId());
				storage.srmReleaseSpace(  user,
							  spaceReservationId,
							  null,
							  callbacks);
			}
			if(spaceReservationId != null &&
			   spaceMarkedAsBeingUsed ) {
				SrmCancelUseOfSpaceCallbacks callbacks =
					new CopyCancelUseOfSpaceCallbacks(getId());
				storage.srmUnmarkSpaceAsBeingUsed(user,
								  spaceReservationId,local_to_path,
								  callbacks);
			}
			if( remoteRequestId != null ) {
				if(local_from_path != null ) {
					remoteFileRequestDone(to_url,remoteRequestId,remoteFileId);
				}
				else {
					remoteFileRequestDone(from_url,remoteRequestId,remoteFileId);
				}
			}
		}
	}
    
	public void remoteFileRequestDone(String SURL,String remoteRequestId,String remoteFileId) {
		try {
			say("setting remote file status to Done, SURL="+SURL+" remoteRequestId="+remoteRequestId+
			    " remoteFileId="+remoteFileId);
			(( CopyRequest)(getRequest())).remoteFileRequestDone(SURL,remoteRequestId,remoteFileId);
                }
                catch(Exception e) {
			esay("set remote file status to done failed, surl = "+SURL+
			     " requestId = " +remoteRequestId+ " fileId = " +remoteFileId);
                }
	}    
	/** Getter for property remoteFileId.
	 * @return Value of property remoteFileId.
	 *
	 */
	public String getRemoteFileId() {
		return remoteFileId;
	}
	/** Getter for property remoteRequestId.
	 * @return Value of property remoteRequestId.
	 *
	 */
	public String getRemoteRequestId() {
		return remoteRequestId;
	}
	/**
	 * Getter for property from_url.
	 * @return Value of property from_url.
	 */
	public java.lang.String getFrom_url() {
		return from_url;
	}
	/**
	 * Getter for property to_url.
	 * @return Value of property to_url.
	 */
	public java.lang.String getTo_url() {
		return to_url;
	}
	/**
	 * Setter for property remoteRequestId.
	 * @param remoteRequestId New value of property remoteRequestId.
	 */
	public void setRemoteRequestId(String remoteRequestId) {
		this.remoteRequestId = remoteRequestId;
	}
	/**
	 * Setter for property remoteFileId.
	 * @param remoteFileId New value of property remoteFileId.
	 */
	public void setRemoteFileId(String remoteFileId) {
		this.remoteFileId = remoteFileId;
	}
	/**
	 * Getter for property spaceReservationId.
	 * @return Value of property spaceReservationId.
	 */
	public String getSpaceReservationId() {
		return spaceReservationId;
	}
	/**
	 * Setter for property spaceReservationId.
	 * @param spaceReservationId New value of property spaceReservationId.
	 */
	public void setSpaceReservationId(String spaceReservationId) {
		this.spaceReservationId = spaceReservationId;
	}
	
	private static class PutCallbacks implements PrepareToPutCallbacks {
		Long fileRequestJobId;
		public boolean completed = false;
		public boolean success = false;
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
        
		public PutCallbacks(Long fileRequestJobId) {
			if(fileRequestJobId == null) {
				throw new NullPointerException("fileRequestJobId should be non-null");
			}
			this.fileRequestJobId = fileRequestJobId;
		}
        
		public CopyFileRequest getCopyFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException{
			Job job = Job.getJob(fileRequestJobId);
			if(job != null) {
				return (CopyFileRequest) job;
			}
			throw new java.sql.SQLException("CopyFileRequest for id="+fileRequestJobId+" is not found");
		}
		
		public void DuplicationError(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_DUPLICATION_ERROR);
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			complete(false);
		}
       
		public void Error( String error) {
			error_message = error;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			complete(false);
		}
		
		public void Exception( Exception e) {
			error_message = e.toString();
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e1) {
				e1.printStackTrace();
			}
			complete(false);
		}
		
		public void GetStorageInfoFailed(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			complete(false);
		}
		
		
		public void StorageInfoArrived(String fileId,
					       FileMetaData fmd,
					       String parentFileId, 
					       FileMetaData parentFmd) {
			try {
				CopyFileRequest fr =  getCopyFileRequest();
				fr.say("StorageInfoArrived: FileId:"+fileId);
				State state;
				synchronized(fr) {
					state = fr.getState();
				}
				if(state == State.ASYNCWAIT) {
					fr.say("PutCallbacks StorageInfoArrived for file "+fr.getToPath()+" fmd ="+fmd);
					fr.toFileId = fileId;
					fr.toFmd = fmd;
					fr.toParentFileId = parentFileId;
					fr.toParentFmd = parentFmd;
					Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
					try {
						scheduler.schedule(fr);
					}
					catch(Exception ie) {
						fr.esay(ie);
					}
				}
				complete(true);
			}
			catch(Exception e){
				e.printStackTrace();
				complete(false);
			}
			
		}
		
		public void Timeout() {
			error_message = "PutCallbacks Timeout";
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			complete(false);
		}
        
		public void InvalidPathError(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_INVALID_PATH);
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			complete(false);
		}
		
		public void AuthorizationError(String reason) {
			error_message = reason;
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
							fr.setState(State.FAILED,error_message);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				
				fr.esay("PutCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			complete(false);
		}
	}
	
	private static class TheCopyCallbacks implements org.dcache.srm.CopyCallbacks {
		private Long fileRequestJobId;
		private boolean completed = false;
		private boolean success = false;
		private String error_message;
		private boolean isTimeout = false;
		
		public TheCopyCallbacks ( Long fileRequestJobId ) {
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
					isTimeout = true;
					error_message = "CopyCallbacks wait timeout expired";
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
			Job job = Job.getJob(fileRequestJobId);
			if(job != null) {
				return (CopyFileRequest) job;
			}
			return null;
		}
        
		public void copyComplete(String fileId, FileMetaData fmd) {
            try {
                CopyFileRequest  copyFileRequest = getCopyFileRequest();
                copyFileRequest.say("copy succeeded");
                copyFileRequest.setStateToDone();
                complete(true);
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
		}
         
		public void copyFailed(Exception e) {
			CopyFileRequest  copyFileRequest ;
            try {
                 copyFileRequest   = getCopyFileRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
                return;
            }
			copyFileRequest.transferError = e;
			copyFileRequest.esay("copy failed:");
			copyFileRequest.esay(e);
			State state =  copyFileRequest.getState();
			Scheduler scheduler = Scheduler.getScheduler(copyFileRequest.getSchedulerId());
			if(!State.isFinalState(state) && scheduler != null) {
				try {
					scheduler.schedule(copyFileRequest);
				}
				catch(InterruptedException ie) {
					copyFileRequest.esay(ie);
				}
				catch(org.dcache.srm.scheduler.IllegalStateTransition ist) {
					copyFileRequest.esay(ist);
				}
			}
			complete(false);
		}
	}
     
	public  TCopyRequestFileStatus getTCopyRequestFileStatus() throws java.sql.SQLException {
		TCopyRequestFileStatus copyRequestFileStatus = new TCopyRequestFileStatus();
		copyRequestFileStatus.setFileSize(new org.apache.axis.types.UnsignedLong(size));
		copyRequestFileStatus.setEstimatedWaitTime(new Integer((int)(getRemainingLifetime()/1000)));
		copyRequestFileStatus.setRemainingFileLifetime(new Integer((int)(getRemainingLifetime()/1000)));
		org.apache.axis.types.URI to_surl;
		org.apache.axis.types.URI from_surl;
		try { to_surl= new URI(to_url);
		}
		catch (Exception e) { 
			esay(e);
			throw new java.sql.SQLException("wrong surl format");
		}
		try { 
			from_surl=new URI(from_url);
		}
		catch (Exception e) { 
			esay(e);
			throw new java.sql.SQLException("wrong surl format");
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
	
	
	public TSURLReturnStatus  getTSURLReturnStatus(String surl ) throws java.sql.SQLException{
		if(surl == null) {
			surl = getToURL();
		}
		URI tsurl;
		try {
			tsurl=new URI(surl);
		} 
		catch (Exception e) {
			esay(e);
			throw new java.sql.SQLException("wrong surl format");
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
		return weReservedSpace;
	}
	
	public void setWeReservedSpace(boolean weReservedSpace) {
		this.weReservedSpace = weReservedSpace;
	}
    
	public boolean isSpaceMarkedAsBeingUsed() {
		return spaceMarkedAsBeingUsed;
	}
    
	public void setSpaceMarkedAsBeingUsed(boolean spaceMarkedAsBeingUsed) {
		this.spaceMarkedAsBeingUsed = spaceMarkedAsBeingUsed;
	}
    
	public static class TheReserveSpaceCallbacks implements SrmReserveSpaceCallbacks {
		Long fileRequestJobId;
		public CopyFileRequest getCopyFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException
        {
			Job job = Job.getJob(fileRequestJobId);
			if(job != null) {
				return (CopyFileRequest) job;
			}
			return null;
		}

		public TheReserveSpaceCallbacks(Long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}
        
		public void ReserveSpaceFailed(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
							fr.setState(State.FAILED,reason);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyReserveSpaceCallbacks error: "+ reason);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		public void NoFreeSpace(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
							fr.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
							fr.setState(State.FAILED,reason);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyReserveSpaceCallbacks error NoFreeSpace : "+ reason);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		public void SpaceReserved(String spaceReservationToken, long reservedSpaceSize) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.say("Space Reserved: spaceReservationToken:"+spaceReservationToken);
				State state;
				synchronized(fr) {
					state = fr.getState();
				}
				if(state == State.ASYNCWAIT) {
					fr.say("CopyReserveSpaceCallbacks Space Reserved for file "+fr.getToPath());
					fr.setSpaceReservationId(spaceReservationToken);
					fr.weReservedSpace = true;
					Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
					try {
						scheduler.schedule(fr);
					}
					catch(Exception ie) {
						fr.esay(ie);
					}
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		public void ReserveSpaceFailed(Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				String error = e.toString();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
							fr.setState(State.FAILED,error);
						}
					}
				}
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyReserveSpaceCallbacks exception");
				fr.esay(e);
			}
			catch(Exception e1) {
				e1.printStackTrace();
			}
		}
	}
     
	
	private  static class TheReleaseSpaceCallbacks implements  SrmReleaseSpaceCallbacks {
		Long fileRequestJobId;
		
		public TheReleaseSpaceCallbacks(Long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}
		
		public CopyFileRequest getCopyFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException {
			Job job = Job.getJob(fileRequestJobId);
			if(job != null) {
				return (CopyFileRequest) job;
			}
			return null;
		}
		
		public void ReleaseSpaceFailed( String error) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.setSpaceReservationId(null);
				fr.esay("TheReleaseSpaceCallbacks error: "+ error);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		public void ReleaseSpaceFailed( Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.setSpaceReservationId(null);    
				fr.esay("TheReleaseSpaceCallbacks exception");
				fr.esay(e);
			}
			catch(Exception e1) {
				e1.printStackTrace();
			}
		}
        
		public void Timeout() {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.setSpaceReservationId(null);
				fr.esay("TheReleaseSpaceCallbacks Timeout");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
        
		public void SpaceReleased(String spaceReservationToken, long remainingSpaceSize) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.say("TheReleaseSpaceCallbacks: SpaceReleased");
				fr.setSpaceReservationId(null);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
        /**
         * Getter for property transferId.
         * @return Value of property transferId.
         */
        public java.lang.String getTransferId() {
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
	public long extendLifetime(long newLifetime) throws SRMException {
		long remainingLifetime = getRemainingLifetime();
		if(remainingLifetime >= newLifetime) {
			return remainingLifetime;
		}
		long requestLifetime = getRequest().extendLifetimeMillis(newLifetime);
		if(requestLifetime <newLifetime) {
			newLifetime = requestLifetime;
		}
		if(remainingLifetime >= newLifetime) {
			return remainingLifetime;
		}
		String spaceToken =spaceReservationId;
		
		if(!configuration.isReserve_space_implicitely() ||
		   spaceToken == null ||
		   !weReservedSpace) {
			return extendLifetimeMillis(newLifetime);      
		} 
		newLifetime = extendLifetimeMillis(newLifetime);
		if( remainingLifetime >= newLifetime) {
			return remainingLifetime;
		}
		SRMUser user =(SRMUser) getUser();
		return storage.srmExtendReservationLifetime(user,spaceToken,newLifetime);
	}
	
	public static class CopyUseSpaceCallbacks implements SrmUseSpaceCallbacks {
		Long fileRequestJobId;

		public CopyFileRequest getCopyFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException{
			Job job = Job.getJob(fileRequestJobId);
			if(job != null) {
				return (CopyFileRequest) job;
			}
			return null;
		}
		
		public CopyUseSpaceCallbacks(Long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}
		
		public void SrmUseSpaceFailed( Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				String error = e.toString();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setState(State.FAILED,error);
						}
					}
				} 
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyUseSpaceCallbacks exception");
				fr.esay(e);
			} 
			catch(Exception e1) {
				e1.printStackTrace();
			}
		}
		
		public void SrmUseSpaceFailed(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setState(State.FAILED,reason);
						}
					}
				} 
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyUseSpaceCallbacks error: "+ reason);
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		/**
		 * call this if space reservation exists, but has no free space
		 */
		public void SrmNoFreeSpace(String reason){
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
							fr.setState(State.FAILED,reason);
						}
					}
				} 
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyUseSpaceCallbacks error: "+ reason);
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		/**
		 * call this if space reservation exists, but has been released
		 */
		public void SrmReleased(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
							fr.setState(State.FAILED,reason);
						}
					}
				} 
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyUseSpaceCallbacks error: "+ reason);
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		/**
		 * call this if space reservation exists, but not authorized
		 */
		public void SrmNotAuthorized(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
							fr.setState(State.FAILED,reason);
						}
					}
				} 
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyUseSpaceCallbacks error: "+ reason);
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		/**
		 * call this if space reservation exists, but has been released
		 */
		public void SrmExpired(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				try {
					synchronized(fr) {
						State state = fr.getState();
						if(!State.isFinalState(state)) {
							fr.setStatusCode(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED);
							fr.setState(State.FAILED,reason);
						}
					}
				} 
				catch(IllegalStateTransition ist) {
					fr.esay("can not fail state:"+ist);
				}
				fr.esay("CopyUseSpaceCallbacks error: "+ reason);
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		public void SpaceUsed() {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.say("Space Marked as Being Used");
				State state;
				synchronized(fr) {
					state = fr.getState();
				}
				if(state == State.ASYNCWAIT) {
					fr.say("CopyUseSpaceCallbacks Space Marked as Being Used for file "+fr.getToURL());
					fr.setSpaceMarkedAsBeingUsed(true);
					Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
					try {
						scheduler.schedule(fr);
					} 
					catch(Exception ie) {
						fr.esay(ie);
					}
				}
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class CopyCancelUseOfSpaceCallbacks implements SrmCancelUseOfSpaceCallbacks {
		Long fileRequestJobId;
		
		public CopyFileRequest getCopyFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException {
			Job job = Job.getJob(fileRequestJobId);
			if(job != null) {
				return (CopyFileRequest) job;
			}
			return null;
		}
        
		public CopyCancelUseOfSpaceCallbacks(Long fileRequestJobId) {
			this.fileRequestJobId = fileRequestJobId;
		}
        
		public void CancelUseOfSpaceFailed( Exception e) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				String error = e.toString();
				fr.esay("CopyCancelUseOfSpaceCallbacks exception");
				fr.esay(e);
			} 
			catch(Exception e1) {
				e1.printStackTrace();
			}
		}
		
		public void CancelUseOfSpaceFailed(String reason) {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.esay("CopyCancelUseOfSpaceCallbacks error: "+ reason);
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		public void UseOfSpaceSpaceCanceled() {
			try {
				CopyFileRequest fr = getCopyFileRequest();
				fr.say("Umarked Space as Being Used");
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
}
