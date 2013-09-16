package org.dcache.srm.request;


import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TUserPermission;

public final class LsFileRequest extends FileRequest<LsRequest> {
        private static final Logger logger =
                LoggerFactory.getLogger(LsFileRequest.class);
        private static final String SFN_STRING="SFN=";
        private URI surl;
        private TMetaDataPathDetail metaDataPathDetail;
        private static final Comparator<FileMetaData> DIRECTORY_LAST_ORDER =
                new Comparator<FileMetaData>() {
                @Override
                public int compare(FileMetaData f1,
                                   FileMetaData f2) {
                        if (f1.isDirectory&&f2.isRegular) {
                            return 1;
                        }
                        if (f1.isRegular&&f2.isDirectory) {
                            return -1;
                        }
                        return 0;
                }
        };

        public LsFileRequest(long requestId,
                             Long  requestCredentalId,
                             org.apache.axis.types.URI url,
                             long lifetime,
                             int maxNumberOfRetries) throws Exception {

                super(requestId,
                      requestCredentalId,
                      lifetime,
                      maxNumberOfRetries);
                this.surl = URI.create(url.toString());
        }

        public LsFileRequest(
                long id,
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
                long requestId,
                Long  requestCredentalId,
                String statusCodeString,
                String SURL)
                throws SQLException {
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
                this.surl = URI.create(SURL);
        }

        public String getPath(URI uri) {
            String path = uri.getPath();
            String query = uri.getQuery();
            if (query != null) {
                int i = query.indexOf(SFN_STRING);
                if (i != -1) {
                    path = query.substring(i + SFN_STRING.length()).replaceAll("//*", "/");
                }
            }
            return path;
        }

        public URI getSurl() {
                return surl;
        }

        public String getSurlString() {
                return surl.toString();
        }

        @Override
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


        @Override
        public synchronized void run() throws NonFatalJobFailure, FatalJobFailure {
                try {
                        LsRequest parent = getContainerRequest();
                        long t0=0;
                        if (logger.isDebugEnabled()){
                                t0=System.currentTimeMillis();
                        }

                        if (SRM.getSRM().isFileBusy(surl)) {
                            // [SRM 2.2, 4.4.3] client requests for a file which there is an active
                            // srmPrepareToPut (no srmPutDone is yet called) request for.
                            metaDataPathDetail = new TMetaDataPathDetail();
                            metaDataPathDetail.setPath(getPath(surl));
                            metaDataPathDetail.setType(TFileType.FILE);
                            metaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_FILE_BUSY,
                                    "The requested SURL is being used by another client."));
                        } else {
                            metaDataPathDetail =
                                    getMetaDataPathDetail(surl,
                                                          0,
                                                          parent.getOffset(),
                                                          parent.getCount(),
                                                          parent.getNumOfLevels(),
                                                          parent.getLongFormat());
                        }
                        if (logger.isDebugEnabled()) {
                                logger.debug("LsFileRequest.run(), TOOK "+(System.currentTimeMillis()-t0));
                        }
                        try {
                                getContainerRequest().resetRetryDeltaTime();
                        }
                        catch(SRMInvalidRequestException ire) {
                                logger.error(ire.toString());
                        }
                        setState(State.DONE, State.DONE.toString());
                }
                catch (SRMException | SQLException | URISyntaxException | IllegalStateTransition e) {
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
                                else if (e instanceof SQLException) {
                                    status = new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR,
                                            msg);
                                    setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
                                }
                                else {
                                        if (e instanceof RuntimeException) {
                                                logger.error(e.toString(), e);
                                        }
                                        status = new TReturnStatus(TStatusCode.SRM_FAILURE,
                                                                   msg);
                                        setStatusCode(TStatusCode.SRM_FAILURE);
                                }
                                metaDataPathDetail =  new TMetaDataPathDetail();
                                metaDataPathDetail.setPath(getPath(surl));
                                metaDataPathDetail.setStatus(status);
                                setState(State.FAILED, e.toString());
                        }
                        catch(IllegalStateTransition ist) {
                                logger.error("Illegal State Transition : " +ist.getMessage());
                        }
                        finally {
                                wunlock();
                        }
                }
        }

        @Override
        protected void stateChanged(State oldState) {
                logger.debug("State changed from "+oldState+" to "+getState());
                super.stateChanged(oldState);
        }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
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

        @Override
        public long extendLifetime(long newLifetime) throws SRMException {
                long remainingLifetime = getRemainingLifetime();
                if(remainingLifetime >= newLifetime) {
                        return remainingLifetime;
                }
                long requestLifetime = getContainerRequest().extendLifetimeMillis(newLifetime);
                return requestLifetime;
        }

        public TMetaDataPathDetail getMetaDataPathDetail() {
                return metaDataPathDetail;
        }

        public final TMetaDataPathDetail getMetaDataPathDetail(URI surl,
                                                               int depth,
                                                               long offset,
                                                               long count,
                                                               int recursionDepth,
                                                               boolean longFormat)
                throws SRMException, URISyntaxException
        {
                FileMetaData fmd = getStorage().getFileMetaData(getUser(),
                                                                surl,
                                                                false);
                TMetaDataPathDetail aMetaDataPathDetail=
                        convertFileMetaDataToTMetaDataPathDetail(surl,
                                                                 fmd,
                                                                 depth,
                                                                 longFormat);
                if(!getContainerRequest().increaseResultsNumAndContinue()) {
                        return aMetaDataPathDetail;
                }
                if (fmd.isDirectory && depth< recursionDepth) {
                        if (recursionDepth==1) {
                                //
                                // for simplicity break up code into two blocks - one block
                                // works for the simple case recursionDepth=1, the other
                                // block works for recursionDepth>1
                                // there is a bit of code duplication, but code is
                                // relatively straightforward this way
                                //
                                 getMetaDataPathDetail(aMetaDataPathDetail,
                                                       offset,
                                                       count,
                                                       longFormat);
                        }
                        else {
                                getRecursiveMetaDataPathDetail(aMetaDataPathDetail,
                                                               fmd,
                                                               depth,
                                                               offset,
                                                               count,
                                                               recursionDepth,
                                                               longFormat);
                        }
                }
                return aMetaDataPathDetail;
        }

        private void getMetaDataPathDetail(TMetaDataPathDetail metaDataPathDetail,
                                           long offset,
                                           long count,
                                           boolean longFormat)
                throws SRMException, URISyntaxException
        {
                List<FileMetaData> directoryList;
                //
                // simplify things for the most common case when people perform
                // ls on directory w/o specifying recursionDepth
                //
                URI surl =
                    new URI(null, null, metaDataPathDetail.getPath(), null);
                directoryList =
                        getStorage().listDirectory(getUser(),
                                                   surl,
                                                   longFormat,
                                                   (int) offset,
                                                   (int) count);
                getContainerRequest().setCounter(offset);
                List<TMetaDataPathDetail> metadataPathDetailList =
                        new LinkedList<>();
                for (FileMetaData md : directoryList) {
                        URI subpath = new URI(null, null, md.SURL, null);
                        TMetaDataPathDetail dirMetaDataPathDetail=
                                convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                         md,
                                                                         1,
                                                                         longFormat);
                        if (!getContainerRequest().shouldSkipThisRecord()) {
                                metadataPathDetailList.add(dirMetaDataPathDetail);
                                try {
                                        if(!getContainerRequest().increaseResultsNumAndContinue()) {
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
                        getContainerRequest().incrementGlobalEntryCounter();
                }
                metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(metadataPathDetailList
                        .toArray(new TMetaDataPathDetail[metadataPathDetailList
                                .size()])));
        }

        private void getRecursiveMetaDataPathDetail(TMetaDataPathDetail metaDataPathDetail,
                                                    FileMetaData fmd,
                                                    int depth,
                                                    long offset,
                                                    long count,
                                                    int recursionDepth,
                                                    boolean longFormat)
                throws SRMException, URISyntaxException
        {
                if (!fmd.isDirectory || depth >= recursionDepth) {
                    return;
                }
                List<FileMetaData> directoryList;
                URI surl =
                        new URI(null, null, metaDataPathDetail.getPath(), null);
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
                                                           surl,
                                                           longFormat,
                                                           0,
                                                           (int) count);
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
                                                           surl,
                                                           false,
                                                           0,
                                                           Integer.MAX_VALUE);
                }
                //
                // sort list such that directories are at the end of the list after
                // sorting. The intent is to leave the recursion calls at the
                // end of the tree, so we have less chance to even get there
                //
                Collections.sort(directoryList,
                                 DIRECTORY_LAST_ORDER);
                List<TMetaDataPathDetail> metadataPathDetailList =
                        new LinkedList<>();
                for (FileMetaData md : directoryList) {
                        URI subpath = new URI(null, null, md.SURL, null);
                        TMetaDataPathDetail dirMetaDataPathDetail;
                        if (offset==0) {
                                dirMetaDataPathDetail=
                                        convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                 md,
                                                                                 depth,
                                                                                 longFormat);
                        }
                        else {
                                FileMetaData fileMetaData=md;
                                if (!getContainerRequest().shouldSkipThisRecord()) {
                                        if (longFormat) {
                                                fileMetaData = getStorage().getFileMetaData(getUser(),
                                                                                            subpath,
                                                                                            false);
                                        }
                                        dirMetaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                                       fileMetaData,
                                                                                                       depth, longFormat);
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
                        if (!getContainerRequest().shouldSkipThisRecord()) {
                                metadataPathDetailList.add(dirMetaDataPathDetail);
                                try {
                                        if(!getContainerRequest().increaseResultsNumAndContinue()) {
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
                        getContainerRequest().incrementGlobalEntryCounter();
                        if (md.isDirectory) {
                                try {
                                        getRecursiveMetaDataPathDetail(dirMetaDataPathDetail,
                                                                       md,
                                                                       depth+1,
                                                                       offset,
                                                                       count,
                                                                       recursionDepth,
                                                                       longFormat);
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
                metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(metadataPathDetailList
                        .toArray(new TMetaDataPathDetail[metadataPathDetailList
                                .size()])));
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

        private TMetaDataPathDetail
                convertFileMetaDataToTMetaDataPathDetail(final URI path,
                                                         final FileMetaData fmd,
                                                         final int depth,
                                                         final boolean verbose)
                throws SRMException {
                TMetaDataPathDetail metaDataPathDetail =
                        new TMetaDataPathDetail();
                metaDataPathDetail.setPath(getPath(path));
                metaDataPathDetail.setLifetimeAssigned(-1);
                metaDataPathDetail.setLifetimeLeft(-1);
                metaDataPathDetail.setSize(new UnsignedLong(fmd.size));
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
                        int userPerm = (fmd.permMode >> 6) & 7;
                        userPermission.setMode(maskToTPermissionMode(userPerm));
                        metaDataPathDetail.setOwnerPermission(userPermission);
                        TGroupPermission groupPermission = new TGroupPermission();
                        groupPermission.setGroupID(fmd.group);
                        int groupPerm = (fmd.permMode >> 3) & 7;
                        groupPermission.setMode(maskToTPermissionMode(groupPerm));
                        metaDataPathDetail.setGroupPermission(groupPermission);
                        metaDataPathDetail.setOtherPermission(maskToTPermissionMode(fmd.permMode&7));
                        GregorianCalendar td =
                                new GregorianCalendar();
                        td.setTimeInMillis(fmd.creationTime);
                        metaDataPathDetail.setCreatedAtTime(td);
                        td = new GregorianCalendar();
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
                                                StringBuilder spaceToken = new StringBuilder();
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

