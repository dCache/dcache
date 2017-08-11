package org.dcache.srm.request;


import com.google.common.collect.Iterables;
import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.net.URI;
import java.net.URISyntaxException;
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
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
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
        private final URI surl;
        private TMetaDataPathDetail metaDataPathDetail;
        private static final Comparator<FileMetaData> DIRECTORY_LAST_ORDER =
                (f1, f2) -> {
                        if (f1.isDirectory&&f2.isRegular) {
                            return 1;
                        }
                        if (f1.isRegular&&f2.isDirectory) {
                            return -1;
                        }
                        return 0;
                };

        public LsFileRequest(long requestId, URI surl, long lifetime)
        {
                super(requestId, lifetime);
                this.surl = surl;
        }

        public LsFileRequest(
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
                String SURL)
        {
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
        public synchronized void run() throws IllegalStateTransition
        {
            logger.trace("run");
            if (!getState().isFinal()) {
                try {
                    LsRequest parent = getContainerRequest();
                    long t0 = 0;
                    if (logger.isDebugEnabled()) {
                        t0 = System.currentTimeMillis();
                    }

                    String fileId = SRM.getSRM().getUploadFileId(surl);

                    TMetaDataPathDetail detail;

                    if (fileId != null) {
                        // [SRM 2.2, 4.4.3]
                        //
                        // SRM_FILE_BUSY
                        //
                        //     client requests for a file which there is an active
                        //     srmPrepareToPut (no srmPutDone is yet called) request for.
                        try {
                            FileMetaData fmd = getStorage().getFileMetaData(getUser(),
                                                                            surl,
                                                                            fileId);
                            detail = convertFileMetaDataToTMetaDataPathDetail(surl,
                                                                              fmd,
                                                                              parent.getLongFormat());
                        } catch (SRMInvalidPathException e) {
                            detail = new TMetaDataPathDetail();
                            detail.setType(TFileType.FILE);
                        }
                        detail.setPath(getPath(surl));
                        detail.setStatus(new TReturnStatus(TStatusCode.SRM_FILE_BUSY,
                                                           "The requested SURL is locked by an upload."));
                    } else {
                        detail = getMetaDataPathDetail(surl,
                                                       0,
                                                       parent.getOffset(),
                                                       parent.getCount(),
                                                       parent.getNumOfLevels(),
                                                       parent.getLongFormat());
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("LsFileRequest.run(), TOOK {}", (System.currentTimeMillis() - t0));
                    }
                    try {
                        getContainerRequest().resetRetryDeltaTime();
                    } catch (SRMInvalidRequestException ire) {
                        logger.error(ire.toString());
                    }
                    wlock();
                    try {
                        metaDataPathDetail = detail;
                        if (!getState().isFinal()) {
                            setState(State.DONE, State.DONE.toString());
                        }
                    } finally {
                        wunlock();
                    }
                } catch (SRMException e) {
                    fail(e.getStatusCode(), e.getMessage());
                } catch (URISyntaxException e) {
                    fail(TStatusCode.SRM_FAILURE, e.getMessage());
                } catch (DataAccessException | IllegalStateTransition e) {
                    logger.error(e.toString(), e);
                    fail(TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
                }
            }
        }

    private void fail(TStatusCode statusCode, String msg)
    {
        wlock();
        try {
            if (!getState().isFinal()) {
                metaDataPathDetail = new TMetaDataPathDetail();
                metaDataPathDetail.setPath(getPath(surl));
                metaDataPathDetail.setStatus(new TReturnStatus(statusCode, msg));
                setStatusCode(statusCode);
                setState(State.FAILED, msg);
            }
        } catch (IllegalStateTransition e) {
            logger.error("Illegal State Transition : {}", e.getMessage());
        } finally {
            wunlock();
        }
    }

    @Override
        protected void stateChanged(State oldState) {
                logger.debug("State changed from {} to {}", oldState, getState());
                super.stateChanged(oldState);
        }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
    }

    @Override
        public TReturnStatus getReturnStatus() {
                String description = getLastJobChange().getDescription();
                TStatusCode statusCode = getStatusCode();
                if(statusCode != null) {
                    return new TReturnStatus(statusCode, description);
                }
                switch (getState()) {
                case DONE:
                case READY:
                    return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
                case FAILED:
                    return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
                case CANCELED:
                    return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
                case INPROGRESS:
                case RQUEUED:
                    return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
                default:
                    return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
                }
        }

        @Override
        public long extendLifetime(long newLifetime) throws SRMException {
                long remainingLifetime = getRemainingLifetime();
                if(remainingLifetime >= newLifetime) {
                        return remainingLifetime;
                }
            return getContainerRequest().extendLifetimeMillis(newLifetime);
        }

        public TMetaDataPathDetail getMetaDataPathDetail()
                throws SRMInvalidRequestException
        {
            rlock();
            try {
                if (metaDataPathDetail != null) {
                    return metaDataPathDetail;
                }
                if (getState() == State.DONE) {
                    /* If the request has been processed yet metaDataPathDetail
                     * is null then the information is no longer known.  This
                     * can happen if the information has been delivered to the
                     * client, this Request has been garbage collected, and the
                     * request was fetched back from the database to process a
                     * StatusOfLsRequest request.
                     */
                    throw new SRMInvalidRequestException("Response no longer available.");
                }
                TMetaDataPathDetail detail =  new TMetaDataPathDetail();
                detail.setPath(getPath(surl));
                detail.setStatus(getReturnStatus());
                return detail;
            } finally {
                runlock();
            }
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
                                                                                                       longFormat);
                                }
                                else {
                                        //
                                        // skip this record - meaning count it, and request only minimal details, do not store it
                                        //
                                        dirMetaDataPathDetail=convertFileMetaDataToTMetaDataPathDetail(subpath,
                                                                                                       fileMetaData,
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
                                    String msg = e.getMessage();
                                    if (e instanceof SRMAuthorizationException) {
                                        dirMetaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, msg));
                                    }
                                    else if (e instanceof SRMInvalidPathException) {
                                        dirMetaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_INVALID_PATH, msg));
                                    }
                                    else {
                                        dirMetaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_FAILURE, msg));
                                    }
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
            return fmd.isOwner(user) && Permissions.userCanRead(permissions);
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
        public void toString(StringBuilder sb, String padding, boolean longformat) {
                sb.append(padding);
                if (padding.isEmpty()) {
                    sb.append("Ls ");
                }
                sb.append("file id:").append(getId());
                State state = getState();
                sb.append(" state:").append(state);
                if(longformat) {
                        sb.append('\n');
                        sb.append(padding).append("   SURL: ").append(getSurl()).append('\n');
                        TStatusCode status = getStatusCode();
                        if (status != null) {
                            sb.append(padding).append("   Status:").append(status).append('\n');
                        }
                        sb.append(padding).append("   History:\n");
                        sb.append(getHistory(padding + "   "));
                }
        }

        private TMetaDataPathDetail
                convertFileMetaDataToTMetaDataPathDetail(final URI path,
                                                         final FileMetaData fmd,
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
                                            arrayOfSpaceTokens.setStringArray(st, String.valueOf(fmd.spaceTokens[st]));
                                        }
                                        metaDataPathDetail.setArrayOfSpaceTokens(arrayOfSpaceTokens);
                                }
                        }
                }
                metaDataPathDetail.setStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
                return metaDataPathDetail;
        }
}

