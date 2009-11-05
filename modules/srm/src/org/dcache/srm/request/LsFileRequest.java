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

public final class LsFileRequest extends FileRequest {
        private org.apache.axis.types.URI surl;
        private TMetaDataPathDetail metaDataPathDetail;

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
        }

        public void say(String s) {
                if(getStorage() != null) {
                        getStorage().log("LsFileRequest reqId #"+requestId+" ls#"+getId()+": "+s);
                }
        }

        public void esay(String s) {
                if(getStorage() != null) {
                        getStorage().elog("LsFileRequest reqId #"+requestId+" ls#"+getId()+": "+s);
                }
        }

        public void esay(Throwable t) {
                if(getStorage() != null) {
                        getStorage().elog(t);
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
                        metaDataPathDetail =
                                getMetaDataPathDetail(path,
                                                          0,
                                                          ((LsRequest)getRequest()).getOffset(),
                                                          ((LsRequest)getRequest()).getCount(),
                                                          null);
                }
                catch (Exception e) {
                        wlock();
                        try {
                                setStatusCode(TStatusCode.SRM_FAILURE);
                                TReturnStatus status=null;
                                if (e instanceof SRMInternalErrorException) {
                                        status = new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
                                        setStatusCode(TStatusCode.SRM_FAILURE);
                                }
                                else if (e instanceof SRMTooManyResultsException) {
                                        status = new TReturnStatus(TStatusCode.SRM_TOO_MANY_RESULTS, e.getMessage());
                                        setStatusCode(TStatusCode.SRM_TOO_MANY_RESULTS);
                                }
                                else if (e instanceof SRMAuthorizationException) {
                                        status =  new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
                                        setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                                }
                                else {
                                        status = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
                                        setStatusCode(TStatusCode.SRM_INVALID_PATH);
                                }
                                metaDataPathDetail =  new TMetaDataPathDetail(path,
                                                                              status,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null);
                                setState(State.FAILED,e.toString());
                        }
                        catch(IllegalStateTransition ist) {
                                esay("Illegal State Transition : " +ist.getMessage());
                        } finally {
                            wunlock();
                        }
                }
        }
        

        protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
            State state = getState();
            say("State changed from "+oldState+" to "+getState());
                if(state == State.READY) {
                    try {
                        getRequest().resetRetryDeltaTime();
                    } catch(SRMInvalidRequestException ire) {
                        esay(ire);
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
                                 esay("Illegal State Transition : " +ist.getMessage());
                        }                                                                                                 
                }
                return metaDataPathDetail;
        }

        public TMetaDataPathDetail getMetaDataPathDetail(String path,
                                                         int depth,
                                                         int offset,
                                                         int count,
                                                         FileMetaData parent_fmd)
                throws SRMException,org.apache.axis.types.URI.MalformedURIException {
                if(!((LsRequest)getRequest()).increaseResultsNumAndContinue()) {
                        throw new SRMTooManyResultsException("max results number of "+
                            ((LsRequest)getRequest()).getMaxNumOfResults()+
                            " exceeded. Try to narrow down with count and use" +
                            " offset to get complete listing \n");
                }
                FileMetaData fmd = getStorage().getFileMetaData(getUser(), path,parent_fmd);
                TMetaDataPathDetail metaDataPathDetail =
                        new TMetaDataPathDetail();
                metaDataPathDetail.setLifetimeAssigned(new Integer(-1));
                metaDataPathDetail.setLifetimeLeft(new Integer(-1));
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
                metaDataPathDetail.setOtherPermission(maskToTPermissionMode(fmd.permMode & 7));
                org.apache.axis.types.URI turi =
                        new org.apache.axis.types.URI();
                turi.setScheme("srm");
                metaDataPathDetail.setPath(path);
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
                        say("file type is Unknown");
                }
                TFileLocality fileLocality = TFileLocality.NONE;
                if (fmd.isCached) {
                        if (fmd.isStored) {
                                fileLocality = TFileLocality.ONLINE_AND_NEARLINE;
                        }
                        else {
                                fileLocality = TFileLocality.ONLINE;
                        }
                }
                else {
                        if (fmd.isStored) {
                                fileLocality = TFileLocality.NEARLINE;
                        }
                        else {
                                fileLocality = TFileLocality.UNAVAILABLE;
                        }
                }
                if (fmd.isDirectory) {
                        fileLocality = TFileLocality.NONE;	
                }
                metaDataPathDetail.setFileLocality(fileLocality);
                if (fmd.retentionPolicyInfo!=null) {
                        metaDataPathDetail.setRetentionPolicyInfo(new TRetentionPolicyInfo(fmd.retentionPolicyInfo.getRetentionPolicy(),
                                                                                           fmd.retentionPolicyInfo.getAccessLatency()));
                }
                metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(fmd.size));
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
                TReturnStatus returnStatus = new TReturnStatus();
                returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
                metaDataPathDetail.setStatus(returnStatus);
                //
                // behavior below is equivalent to this:
                // supose we have file and dirtectory:
                //
                //drw-------   2 root     root      4096 Feb 25 13:49 blah
                //-rw-------   1 root     root         0 Feb 25 13:49 blah.txt
                // the code below should behave like this:
                //   [litvinse@uqbar Desktop]$ ls blah.txt
                //   blah.txt
                //   [litvinse@uqbar Desktop]$ ls blah
                //   ls: blah: Permission denied
                //
                if(!canRead(getUser(),fmd)) {
                        if (depth>0) {
                                if (fmd.isDirectory) {
                                        returnStatus.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                                        returnStatus.setExplanation("Permission mask does not allow directory listing");
                                        metaDataPathDetail.setStatus(returnStatus);
                                }
                                return metaDataPathDetail;
                        }
                        else {
                                if (fmd.isDirectory) {
                                        throw new SRMAuthorizationException("Permission denied");
                                }
                        }
                }
                //
                // check if the number of entries does not exceed count
                //
                if (!((LsRequest)getRequest()).checkCounter()) {
                        metaDataPathDetail.setStatus(returnStatus);
                        return metaDataPathDetail;
                }
                if (metaDataPathDetail.getType() == TFileType.DIRECTORY && 
                        depth<((LsRequest)getRequest()).getNumOfLevels()) {
                        java.io.File dirFiles[] = getStorage().listDirectoryFiles(getUser(),path,fmd);
                        TMetaDataPathDetail dirMetaDataPathDetails[]=null;
                        if(dirFiles != null && dirFiles.length >0) {
                                int end   = dirFiles.length;
                                int start = offset;
                                if ( count != 0 &&  offset + count <= dirFiles.length) {
                                        end = offset + count;
                                }
                                int len = end - start;
                                if ( offset <  dirFiles.length ) {
                                        dirMetaDataPathDetails = new TMetaDataPathDetail[len];
                                        for (int j = start; j< end; j++) {
                                                String subpath = path+'/'+dirFiles[j].getName();
                                                try {
                                                        TMetaDataPathDetail dirMetaDataPathDetail;
                                                        if (((LsRequest)getRequest()).getLongFormat()) {
                                                                dirMetaDataPathDetail = getMetaDataPathDetail(subpath, depth+1,offset,count,fmd);
                                                        }
                                                        else {
                                                                if(depth+1>=((LsRequest)getRequest()).getNumOfLevels()||dirFiles[j].isFile()) {
                                                                        dirMetaDataPathDetail =  getMinimalMetaDataPathDetail(subpath,dirFiles[j]);
                                                                }
                                                                else {
                                                                        dirMetaDataPathDetail = getMetaDataPathDetail(subpath, depth+1,offset,count,fmd);
                                                                }
                                                        }
                                                        dirMetaDataPathDetails[j-start] = dirMetaDataPathDetail;
                                                }
                                                catch (SRMException srme) {
                                                        if (srme instanceof SRMTooManyResultsException) {
                                                                returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
                                                                returnStatus.setExplanation(srme.getMessage());
                                                                ((LsRequest)getRequest()).setStatusCode(TStatusCode.SRM_TOO_MANY_RESULTS);
                                                                ((LsRequest)getRequest()).setExplanation(srme.getMessage());
                                                                metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(dirMetaDataPathDetails));
                                                                metaDataPathDetail.setStatus(returnStatus);
                                                                return metaDataPathDetail;
                                                        }
                                                        returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
                                                        returnStatus.setExplanation(srme.getMessage());
                                                        dirMetaDataPathDetails[j-start] = new TMetaDataPathDetail(subpath,
                                                                                                                  returnStatus,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null,
                                                                                                                  null);
                                                }
                                        }
                                }
                        }
                        metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(dirMetaDataPathDetails));
                }
                return metaDataPathDetail;
        }
        
        public TMetaDataPathDetail getMinimalMetaDataPathDetail(String path,
                                                                java.io.File file)
                throws SRMException,org.apache.axis.types.URI.MalformedURIException {
                if(!((LsRequest)getRequest()).increaseResultsNumAndContinue()) {
                        throw new SRMTooManyResultsException("max results number of "+
                                ((LsRequest)getRequest()).getMaxNumOfResults()+
                                " exceeded. Try to narrow down with count and use " +
                                "offset to get complete listing \n");
                }
                TMetaDataPathDetail metaDataPathDetail =
                        new TMetaDataPathDetail();
                metaDataPathDetail.setLifetimeAssigned(new Integer(-1));
                metaDataPathDetail.setLifetimeLeft(new Integer(-1));
                org.apache.axis.types.URI turi =
                        new org.apache.axis.types.URI();
                turi.setScheme("srm");
                metaDataPathDetail.setPath(path);
                java.util.GregorianCalendar td =
                        new java.util.GregorianCalendar();
                td.setTimeInMillis(file.lastModified());
                metaDataPathDetail.setCreatedAtTime(td);
                metaDataPathDetail.setLastModificationTime(td);
                metaDataPathDetail.setFileStorageType(TFileStorageType.PERMANENT);
                if(file.isDirectory()) {
                        metaDataPathDetail.setType(TFileType.DIRECTORY);
                }
                else if(file.isFile()) {
                        metaDataPathDetail.setType(TFileType.FILE);
                }
                else {
                        say("file type is Unknown");
                }
                if (file.length()==1) {
                        FileMetaData fmd = getStorage().getFileMetaData(getUser(), path, null);
                        metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(fmd.size));
                }
                else {
                        metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(file.length()));
                }
                TReturnStatus returnStatus = new TReturnStatus();
                returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
                metaDataPathDetail.setStatus(returnStatus);
                return metaDataPathDetail;
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
        } catch (SRMInvalidRequestException ire) {
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
}

