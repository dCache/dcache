package org.dcache.srm.request;


import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.FatalJobFailure;

import org.dcache.srm.util.Permissions;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMInvalidPathException;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LsFileRequest extends FileRequest {
        private static final Logger logger =
                LoggerFactory.getLogger(LsFileRequest.class);
        private org.apache.axis.types.URI surl;
        private TMetaDataPathDetail metaDataPathDetail;
        private int recursionDepth;
        private boolean longFormat;

        private static final Comparator<FileMetaData> DIRECTORY_LAST_ORDER =
                new Comparator<FileMetaData>() {
                public int compare(FileMetaData f1,
                                   FileMetaData f2) {
                        if (f1.isDirectory&&f2.isRegular) return 1;
                        if (f1.isRegular&&f2.isDirectory) return -1;
                        return 0;
                }
        };

        public LsFileRequest(LsRequest request,
                             Long  requestCredentalId,
                             org.apache.axis.types.URI url,
                             long lifetime,
                             int maxNumberOfRetries) throws Exception {

                super(request.getId(),
                      requestCredentalId,
                      lifetime,
                      maxNumberOfRetries);
                this.surl=url;
                recursionDepth=request.getNumOfLevels();
                longFormat=request.getLongFormat();
        }

        public LsFileRequest(
                Long id,
                Long nextJobId,
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
                Long  requestCredentalId,
                String statusCodeString,
                String SURL)
                throws java.sql.SQLException {
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
                try {
                        this.surl = new org.apache.axis.types.URI(SURL);
                }
                catch(org.apache.axis.types.URI.MalformedURIException murle) {
                        throw new IllegalArgumentException(murle.toString());
                }
                try {
                        this.recursionDepth=((LsRequest)getRequest()).getNumOfLevels();
                        this.longFormat=((LsRequest)getRequest()).getLongFormat();
                }
                catch (Exception e){
                        throw new RuntimeException("Got exception attempting to access container request "+e.getMessage());
                }
        }

        public String getPath() {
                String path = surl.getPath(true,true);
                int indx=path.indexOf(SFN_STRING);
                if( indx != -1) {
                        path=path.substring(indx+SFN_STRING.length());
                }
                if(!path.startsWith("/")) {
                        path = "/"+path;
                }
                return path;
        }

        public org.apache.axis.types.URI getSurl() {
                return surl;
         }

        public String getSurlString() {
                return surl.toString();
        }

        public RequestFileStatus getRequestFileStatus(){
                RequestFileStatus rfs;
                rfs = new RequestFileStatus();
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


        public synchronized void run() throws NonFatalJobFailure, FatalJobFailure {
                String path = getPath();
                try {
                        LsRequest parent = (LsRequest)getRequest();
                        long t0=0;
                        if (logger.isDebugEnabled()){
                                t0=System.currentTimeMillis();
                        }
                        metaDataPathDetail =
                                getMetaDataPathDetail(path,
                                                      0,
                                                      parent.getOffset(),
                                                      parent.getCount());
                        if (logger.isDebugEnabled()) {
                                logger.debug("LsFileRequest.run(), TOOK "+(System.currentTimeMillis()-t0));
                        }
                }
                catch (Exception e) {
                        wlock();
                        try {
                                TReturnStatus status;
                                String msg=e.getMessage();
                                if (e instanceof SRMInternalErrorException) {
                                        status = new TReturnStatus(TStatusCode.SRM_FAILURE,
                                                                   msg);
                                        setStatusCode(TStatusCode.SRM_FAILURE);
                                }
                                else if (e instanceof SRMTooManyResultsException) {
                                        status = new TReturnStatus(TStatusCode.SRM_TOO_MANY_RESULTS,
                                                                   msg);
                                        setStatusCode(TStatusCode.SRM_TOO_MANY_RESULTS);
                                }
                                else if (e instanceof SRMAuthorizationException) {
                                        status =  new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE,
                                                                    msg);
                                        setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                                }
                                else if (e instanceof SRMInvalidPathException) {
                                        status = new TReturnStatus(TStatusCode.SRM_INVALID_PATH,
                                                                   msg);
                                        setStatusCode(TStatusCode.SRM_INVALID_PATH);
                                }
                                else {
                                        status = new TReturnStatus(TStatusCode.SRM_FAILURE,
                                                                   msg);
                                        setStatusCode(TStatusCode.SRM_FAILURE);
                                }
                                metaDataPathDetail =  new TMetaDataPathDetail();
                                metaDataPathDetail.setPath(path);
                                metaDataPathDetail.setStatus(status);
                                setState(State.FAILED,e.toString());
                        }
                        catch(IllegalStateTransition ist) {
                                logger.error("Illegal State Transition : " +ist.getMessage());
                        }
                        finally {
                                wunlock();
                        }
                }
        }


        protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
                State state = getState();
                logger.debug("State changed from "+oldState+" to "+getState());
                if(state == State.READY) {
                        try {
                                getRequest().resetRetryDeltaTime();
                        }
                        catch(SRMInvalidRequestException ire) {
                                logger.error(ire.toString());
                        }
                }
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
                        returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
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

        public long extendLifetime(long newLifetime) throws SRMException {
                long remainingLifetime = getRemainingLifetime();
                if(remainingLifetime >= newLifetime) {
                        return remainingLifetime;
                }
                long requestLifetime = getRequest().extendLifetimeMillis(newLifetime);
                return requestLifetime;
        }

        public TMetaDataPathDetail getMetaDataPathDetail() {
                if (getState()==State.READY||getState()==State.RQUEUED) {
                        try {
                                setState(State.DONE,State.DONE.toString());
                        }
                        catch(IllegalStateTransition ist) {
                                 logger.error("Illegal State Transition : " +ist.getMessage());
                        }
                }
                return metaDataPathDetail;
        }

        public final TMetaDataPathDetail getMetaDataPathDetail(String path,
                                                               int depth,
                                                               long offset,
                                                               long count)
                throws SRMException {
                FileMetaData fmd = getStorage().getFileMetaData(getUser(),
                                                                path,
                                                                false);
                TMetaDataPathDetail metaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(path,
                                                                                                fmd,
                                                                                                depth,
                                                                                                longFormat);
                if(!((LsRequest)getRequest()).increaseResultsNumAndContinue()) {
                        return metaDataPathDetail;
                }
                if (fmd.isDirectory && depth<recursionDepth) {
                        if (recursionDepth==1) {
                                //
                                // for simplicity break up code into two blocks - one block
                                // works for the simple case recursionDepth=1, the other
                                // block works for recursionDepth>1
                                // there is a bit of code duplication, but code is
                                // relatively straightforward this way
                                //
                                 getMetaDataPathDetail(metaDataPathDetail,
                                                       offset,
                                                       count);
                        }
                        else {
                                getRecursiveMetaDataPathDetail(metaDataPathDetail,
                                                               fmd,
                                                               depth,
                                                               offset,
                                                               count);
                        }
                }
                return metaDataPathDetail;
        }

        public final void getMetaDataPathDetail(TMetaDataPathDetail metaDataPathDetail,
                                                long offset,
                                                long count)
                throws SRMException {
                List<FileMetaData> directoryList;
                //
                // simplify things for the most common case when people perform
                // ls on directory w/o specifying recursionDepth
                //
                directoryList =
                        getStorage().listDirectory(getUser(),
                                                   metaDataPathDetail.getPath(),
                                                   longFormat,
                                                   offset,
                                                   count);
                ((LsRequest)getRequest()).setCounter(offset);
                List<TMetaDataPathDetail> metadataPathDetailList =
                        new LinkedList<TMetaDataPathDetail>();
                for (FileMetaData md : directoryList) {
                        String subpath = md.SURL;
                        TMetaDataPathDetail dirMetaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                                           md,
                                                                                                           1,
                                                                                                           longFormat);
                        if (!((LsRequest)getRequest()).shouldSkipThisRecord()) {
                                metadataPathDetailList.add(dirMetaDataPathDetail);
                                try {
                                        if(!((LsRequest)getRequest()).increaseResultsNumAndContinue()) {
                                                break;
                                        }
                                }
                                catch (SRMTooManyResultsException e) {
                                        metaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_FAILURE,
                                                                                       e.getMessage()));
                                        break;
                                }
                        }
                        //
                        // increment global entries counter
                        //
                        ((LsRequest)getRequest()).incrementGlobalEntryCounter();
                }
                metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(metadataPathDetailList.toArray(new TMetaDataPathDetail[0])));
        }

        public final void getRecursiveMetaDataPathDetail(TMetaDataPathDetail metaDataPathDetail,
                                                         final FileMetaData fmd,
                                                         int depth,
                                                         long offset,
                                                         long count)
                throws SRMException {
                if (!fmd.isDirectory || depth>=recursionDepth)  return;
                List<FileMetaData> directoryList;
                //
                // cannot use offset or count in this case since
                // we are trying to flatten tree structure.
                // rely on our own counting
                if (offset==0) {
                        //
                        // if offset=0, trivial case, just grab information w/ verbosity level
                        // provided by the user
                        //
                        directoryList =
                                getStorage().listDirectory(getUser(),
                                                           metaDataPathDetail.getPath(),
                                                           longFormat,
                                                           0,
                                                           count);
                }
                else {
                        //
                        // if offset!=0, we loop over direntries in non-verbose mode until
                        // we hit offset, then start getting information with verbosity
                        // level specified by the user by calling getStorage().getFileMetaData on
                        // each entry
                        //
                        directoryList =
                                getStorage().listDirectory(getUser(),
                                                           metaDataPathDetail.getPath(),
                                                           false,
                                                           0,
                                                           Long.MAX_VALUE);
                }
                //
                // sort list such that directories are at the end of the list after
                // sorting. The intent is to leave the recursion calls at the
                // end of the tree, so we have less chance to even get there
                //
                Collections.sort(directoryList,
                                 DIRECTORY_LAST_ORDER);
                List<TMetaDataPathDetail> metadataPathDetailList =
                        new LinkedList<TMetaDataPathDetail>();
                for (FileMetaData md : directoryList) {
                        String subpath = md.SURL;
                        TMetaDataPathDetail dirMetaDataPathDetail;
                        if (offset==0) {
                                dirMetaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                               md,
                                                                                               depth,
                                                                                               longFormat);
                        }
                        else {
                                FileMetaData fileMetaData=md;
                                if (!((LsRequest)getRequest()).shouldSkipThisRecord()) {
                                        if (longFormat) {
                                                fileMetaData = getStorage().getFileMetaData(getUser(),
                                                                                            subpath,
                                                                                            false);
                                        }
                                        dirMetaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                                       fileMetaData,
                                                                                                       depth,
                                                                                                       longFormat);
                                }
                                else {
                                        //
                                        // skip this record - meaning count it, and request only minimal details, do not store it
                                        //
                                        dirMetaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                                       fileMetaData,
                                                                                                       depth,
                                                                                                       false);
                                }
                        }
                        if (!((LsRequest)getRequest()).shouldSkipThisRecord()) {
                                metadataPathDetailList.add(dirMetaDataPathDetail);
                                try {
                                        if(!((LsRequest)getRequest()).increaseResultsNumAndContinue()) {
                                                break;
                                        }
                                }
                                catch (SRMTooManyResultsException e) {
                                        metaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_FAILURE,
                                                                                       e.getMessage()));
                                        break;
                                }
                        }
                        //
                        // increment global entries counter
                        //
                        ((LsRequest)getRequest()).incrementGlobalEntryCounter();
                        if (md.isDirectory) {
                                try {
                                        getRecursiveMetaDataPathDetail(dirMetaDataPathDetail,
                                                                       md,
                                                                       depth+1,
                                                                       offset,
                                                                       count);
                                }
                                catch (SRMException e) {
                                        TReturnStatus rs = new TReturnStatus();
                                        String msg = e.getMessage();
                                        rs.setExplanation(msg);
                                        if (e instanceof SRMAuthorizationException) {
                                                rs.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                                        }
                                        else if (e instanceof SRMInvalidPathException) {
                                                rs.setStatusCode(TStatusCode.SRM_INVALID_PATH);
                                        }
                                        else {
                                                rs.setStatusCode(TStatusCode.SRM_FAILURE);
                                        }
                                        dirMetaDataPathDetail.setStatus(rs);
                                }
                        }
                }
                metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(metadataPathDetailList.toArray(new TMetaDataPathDetail[0])));
        }

        public boolean canRead(SRMUser user, FileMetaData fmd) {
                int uid = Integer.parseInt(fmd.owner);
                int gid = Integer.parseInt(fmd.group);
                int permissions = fmd.permMode;
                if(permissions == 0 ) {
                        return false;
                }
                if(Permissions.worldCanRead(permissions)) {
                        return true;
                }
                if(uid == -1 || gid == -1) {
                        return false;
                }
                if(user == null ) {
                        return false;
                }
                if(fmd.isGroupMember(user) && Permissions.groupCanRead(permissions)) {
                        return true;
                }
                if(fmd.isOwner(user) && Permissions.userCanRead(permissions)) {
                        return true;
                }
                return false;
        }

        public TPermissionMode maskToTPermissionMode(int permMask) {
                switch(permMask) {
                case 0: return TPermissionMode.NONE;
                case 1: return TPermissionMode.X;
                case 2: return TPermissionMode.W;
                case 3: return TPermissionMode.WX;
                case 4: return TPermissionMode.R;
                case 5: return TPermissionMode.RX;
                case 6: return TPermissionMode.RW;
                case 7: return TPermissionMode.RWX;
                default:
                        throw new IllegalArgumentException("illegal perm mask: "+permMask);
                }
        }

        @Override
        public void toString(StringBuilder sb, boolean longformat) {
                sb.append(" LsFileRequest ");
                sb.append(" id:").append(getId());
                sb.append(" priority:").append(getPriority());
                sb.append(" creator priority:");
                try {
                        sb.append(getUser().getPriority());
                }
                catch (SRMInvalidRequestException ire) {
                        sb.append("Unknown");
                }
                State state = getState();
                sb.append(" state:").append(state);
                if(longformat) {
                        sb.append('\n').append("   SURL: ").append(getSurl());
                        sb.append('\n').append("   status code:").append(getStatusCode());
                        sb.append('\n').append("   error message:").append(getErrorMessage());
                        sb.append('\n').append("   History of State Transitions: \n");
                        sb.append(getHistory());
                }
        }

        private final TMetaDataPathDetail
                convertFileMetaDataToTMetaDataPathDetail(final String path,
                                                         final FileMetaData fmd,
                                                         final int depth,
                                                         final boolean verbose)
                throws SRMException {
                TMetaDataPathDetail metaDataPathDetail =
                        new TMetaDataPathDetail();
                metaDataPathDetail.setPath(path);
                metaDataPathDetail.setLifetimeAssigned(new Integer(-1));
                metaDataPathDetail.setLifetimeLeft(new Integer(-1));
                metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(fmd.size));
                if(fmd.isDirectory) {
                        metaDataPathDetail.setType(TFileType.DIRECTORY);
                }
                else if(fmd.isLink) {
                        metaDataPathDetail.setType(TFileType.LINK);
                }
                else if(fmd.isRegular) {
                        metaDataPathDetail.setType(TFileType.FILE);
                }
                else {
                        logger.debug("file type is Unknown");
                }
                if(verbose) {
                        // TODO: this needs to be rewritten to
                        // take the ACLs into account.
                        TUserPermission userPermission = new TUserPermission();
                        userPermission.setUserID(fmd.owner);
                        TPermissionMode permissionMode;
                        int userPerm = (fmd.permMode >> 6) & 7;
                        userPermission.setMode(maskToTPermissionMode(userPerm));
                        metaDataPathDetail.setOwnerPermission(userPermission);
                        TGroupPermission groupPermission = new TGroupPermission();
                        groupPermission.setGroupID(fmd.group);
                        int groupPerm = (fmd.permMode >> 3) & 7;
                        groupPermission.setMode(maskToTPermissionMode(groupPerm));
                        metaDataPathDetail.setGroupPermission(groupPermission);
                        metaDataPathDetail.setOtherPermission(maskToTPermissionMode(fmd.permMode&7));
                        java.util.GregorianCalendar td =
                                new java.util.GregorianCalendar();
                        td.setTimeInMillis(fmd.creationTime);
                        metaDataPathDetail.setCreatedAtTime(td);
                        td = new java.util.GregorianCalendar();
                        td.setTimeInMillis(fmd.lastModificationTime);
                        metaDataPathDetail.setLastModificationTime(td);
                        if(fmd.checksumType != null && fmd.checksumValue != null ) {
                                metaDataPathDetail.setCheckSumType(fmd.checksumType);
                                metaDataPathDetail.setCheckSumValue(fmd.checksumValue);
                        }
                        metaDataPathDetail.setFileStorageType(TFileStorageType.PERMANENT);
                        if (!fmd.isPermanent) {
                                if (fmd.isPinned) {
                                        metaDataPathDetail.setFileStorageType(TFileStorageType.DURABLE);
                                }
                                else {
                                        metaDataPathDetail.setFileStorageType(TFileStorageType.VOLATILE);
                                }
                        }
                        metaDataPathDetail.setFileLocality(fmd.locality);
                        if (fmd.retentionPolicyInfo!=null) {
                                TAccessLatency al = fmd.retentionPolicyInfo.getAccessLatency();
                                TRetentionPolicy rp = fmd.retentionPolicyInfo.getRetentionPolicy();
                                metaDataPathDetail.setRetentionPolicyInfo(new TRetentionPolicyInfo(rp,al));
                        }
                        if (fmd.spaceTokens!=null) {
                                if (fmd.spaceTokens.length > 0) {
                                        ArrayOfString arrayOfSpaceTokens = new ArrayOfString(new String[fmd.spaceTokens.length]);
                                        for (int st=0;st<fmd.spaceTokens.length;st++) {
                                                StringBuffer spaceToken = new StringBuffer();
                                                spaceToken.append(fmd.spaceTokens[st]);
                                                arrayOfSpaceTokens.setStringArray(st,spaceToken.toString());
                                        }
                                        metaDataPathDetail.setArrayOfSpaceTokens(arrayOfSpaceTokens);
                                }
                        }
                }
                TReturnStatus returnStatus = new TReturnStatus();
                returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
                metaDataPathDetail.setStatus(returnStatus);
                return metaDataPathDetail;
        }
}

