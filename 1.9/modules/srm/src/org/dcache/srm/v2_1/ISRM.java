/**
 * ISRM.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public interface ISRM extends java.rmi.Remote {

    /**
     * lifetimeOfSpaceToReserve is not needed if requesting permanent
     * space. 
     *             
     *             SRM can provide default size and lifetime if not supplied.
     * storageSystemInfo is optional in case storage system
     *             requires additional security check.
     * 
     *             If sizeOfTotalSpaceDesired is not specified, the SRM
     *             will return its default quota.
     */
    public org.dcache.srm.v2_1.SrmReserveSpaceResponse srmReserveSpace(org.dcache.srm.v2_1.SrmReserveSpaceRequest srmReserveSpaceRequest) throws java.rmi.RemoteException;

    /**
     * forceFileRelease=false is default.  This means that
     *             the space will not be released if it has files that
     *             are still pinned in the space.  To release the space
     *             regardless of the files it contains and their status
     *             forceFileRelease=true must be specified.
     * 
     *             To be safe, a request to release a reserved space
     *             that has an on-going file transfer will return false,
     *             even forceFileRelease= true.
     * 
     *             When space is releasable and forceFileRelease=true,
     *             all the files in the space are released, even in durable
     * or permanent space. 
     * 
     *             When space is released, the files in that space are
     *             treated according to their types: If permanent, keep
     *             it. If durable, perform action at the end of lifetime.
     * If Volatile, release it at the end of lifetime.
     */
    public org.dcache.srm.v2_1.SrmReleaseSpaceResponse srmReleaseSpace(org.dcache.srm.v2_1.SrmReleaseSpaceRequest srmReleaseSpaceRequest) throws java.rmi.RemoteException;

    /**
     * Includes size and time
     * 
     *             If neither size nor lifetime are supplied in the input,
     * then
     *             return will be null.
     * 
     *             newSize is the new actual size of the space, so has
     *             to be positive.
     * 
     *             newLifetimeFromCallingTime is the new lifetime requested
     * regardless of the previous lifetime, and has to be positive.
     *              It might even be shorter than the remaining lifetime
     *             at the time of the call.
     */
    public org.dcache.srm.v2_1.SrmUpdateSpaceResponse srmUpdateSpace(org.dcache.srm.v2_1.SrmUpdateSpaceRequest srmUpdateSpaceRequest) throws java.rmi.RemoteException;

    /**
     * This function is called to reclaim the space for all
     *             released files and update space size in Durable and
     *             Permanent spaces. Files not released are not going to
     *             be removed (even if lifetime expired.)  
     * 
     *             doDynamicCompactFromNowOn=false by default, which implies
     * that only a one time compactSpace will take place.
     * 
     *             If doDynamicCompactFromNowOn=true, then the space of
     *             released files will be automatically compacted until
     *             the value of doDynamicCompactFromNowOn is set to false.
     * 
     * 
     *             When space is compacted, the files in that space do
     *             not have to be removed by the SRM.  For example, the
     *             SRM can choose to move them to volatile space.  The
     *             client will only perceive that the compacted space is
     *             now available to them.
     * 
     *             To physically force a removal of a file, the client
     *             should use srmRm.
     */
    public org.dcache.srm.v2_1.SrmCompactSpaceResponse srmCompactSpace(org.dcache.srm.v2_1.SrmCompactSpaceRequest srmCompactSpaceRequest) throws java.rmi.RemoteException;

    /**

     */
    public org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(org.dcache.srm.v2_1.SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest) throws java.rmi.RemoteException;

    /**
     * Applies to both dir and dile
     * 
     *             Either path must be supplied.
     * 
     *             If a path is pointing to a directory, then the effect
     *             is recursive for all the files in this directory.
     * 
     *             Space allocation and de-allocation maybe involved.
     */
    public org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse srmChangeFileStorageType(org.dcache.srm.v2_1.SrmChangeFileStorageTypeRequest srmChangeFileStorageTypeRequest) throws java.rmi.RemoteException;

    /**
     * If userSpaceTokenDescription is null, returns all space
     *             tokens this user owns
     * 
     *             If the user assigned the same name to multiple space
     *             reservations, he may get back multiple space tokens.
     */
    public org.dcache.srm.v2_1.SrmGetSpaceTokenResponse srmGetSpaceToken(org.dcache.srm.v2_1.SrmGetSpaceTokenRequest srmGetSpaceTokenRequest) throws java.rmi.RemoteException;

    /**
     * Applies to both dir and file
     * 
     *             Support for srmSetPermission is optional.
     * 
     *             In this version, TPermissionMode is identical to Unix
     *             permission modes.
     * 
     *             User permissions are provided in order to support dynamic
     * user-level permission assignment similar to Access Control
     *             Lists (ACLs).
     * 
     *             Permissions can be assigned to set of users and sets
     *             of groups, but only a single owner.
     * 
     *             In this version, SRMs do not provide any group operations
     * (setup, modify, remove, etc.)
     * 
     *             Groups are assumed to be setup before srmSetPermission
     * is used.
     * 
     *             If TPermissionType is ADD or CHANGE, and TPermissionMode
     * is null, then it is assumed that TPermissionMode is READ only.
     * 
     *             If TPermissionType is REMOVE, then the TPermissionMode
     * is ignored.
     */
    public org.dcache.srm.v2_1.SrmSetPermissionResponse srmSetPermission(org.dcache.srm.v2_1.SrmSetPermissionRequest srmSetPermissionRequest) throws java.rmi.RemoteException;

    /**
     * After lifeTimeOfThisAssignment time period, or when
     *             assignedUser obtained a copy of files through srmCopy(),
     * the files involved are released and space is compacted
     *             automatically, which ever is first.
     * 
     *             This function implies actual lifetime of file/space
     *             involved is extended up to the lifeTimeOfThisAssignment.
     * 
     *             The caller must be the owner of the files to be reassigned.
     * 
     *             permission is omitted because it has to be READ permission.
     * 
     *             lifeTimeOfThisAssignment is relative to the calling
     *             time. So it must be positive.
     * 
     *             If the path here is a directory, then all the files
     *             under it are included recursively.
     * 
     *             If there are any files involved that are released before
     * this function call, then these files will not be involved
     *             in reassignment, even if they are still in the space.
     * 
     *             If a compact() is called  before this function is complete,
     * then this function has priority over compact().  Compact
     *             will be done automatically as soon as files are copies
     * to the assignedUser. Whether to dynamically compact
     *             or not is an implementation choice.
     */
    public org.dcache.srm.v2_1.SrmReassignToUserResponse srmReassignToUser(org.dcache.srm.v2_1.SrmReassignToUserRequest srmReassignToUserRequest) throws java.rmi.RemoteException;

    /**
     * When checkInLocalCacheOnly=true, then SRM will only
     *             check files in its local cache. Otherwise, if a file
     *             is not in its local cache, then SRM will go to the siteURL
     * to check the user permission.
     * 
     *             If checkInLocalCacheOnly = false, SRM can choose to
     *             always check the siteURL for user permission of each
     *             file. It is also ok if SRM choose to check its local
     *             cache first, if a file exists and the user has permission,
     * return that permission. Otherwise, check the siteURL
     *             and return permission.
     */
    public org.dcache.srm.v2_1.SrmCheckPermissionResponse srmCheckPermission(org.dcache.srm.v2_1.SrmCheckPermissionRequest srmCheckPermissionRequest) throws java.rmi.RemoteException;

    /**
     * Consistent with unix, recursive creation of directories
     *             is not supported.
     * 
     *             newDiretoryPath can include paths, as long as all sub
     *             directories exist.
     */
    public org.dcache.srm.v2_1.SrmMkdirResponse srmMkdir(org.dcache.srm.v2_1.SrmMkdirRequest srmMkdirRequest) throws java.rmi.RemoteException;

    /**
     * applies to dir
     * 
     *             doRecursiveRemove is false by default.  
     * 
     *             To distinguish from srmRm(), this function is for directories
     * only.
     */
    public org.dcache.srm.v2_1.SrmRmdirResponse srmRmdir(org.dcache.srm.v2_1.SrmRmdirRequest srmRmdirRequest) throws java.rmi.RemoteException;

    /**
     * Applies to files
     * 
     *             To distinguish from srmRmDir(), this function applies
     * to files only.
     */
    public org.dcache.srm.v2_1.SrmRmResponse srmRm(org.dcache.srm.v2_1.SrmRmRequest srmRmRequest) throws java.rmi.RemoteException;

    /**
     * Applies to both dir and file
     * 
     *             fullDetailedList=false by default.
     * 
     *             For directories, only path is required to be returned.
     * 
     *             For files, path and size are required to be returned.
     * 
     *             If fullDetailedList=true, the full details are returned.
     * 
     *             For directories, path and userPermission are required
     *             to be returned.
     * 
     *             For files, path, size, userPermission, lastModificationTime,
     * typeOfThisFile, and lifetimeLeft are required to be
     *             returned, similar to unix command ls -l.
     * 
     *             If allLevelRecursive=true then file lists of all level
     * below current will be provided as well.
     * 
     *             If allLevelRecursive is "true" it dominates, i.e. ignore
     * numOfLevels.  If allLevelRecursive is "false" or missing,
     *             then do numOfLevels.  If numOfLevels is "0" (zero) or
     *             missing, assume a single level.  If both allLevelRecursive
     * and numOfLevels are missing, assume a single level.
     * 
     *             When listing for a particular type specified by "fileStorageType",
     * only the files with that type will be in the output. 
     * 
     *             Empty directories will be returned.
     * 
     *             We recommend width first in the listing.
     * 
     *             We recommend that list of directories come before list
     * of files in the return array (details).
     */
    public org.dcache.srm.v2_1.SrmLsResponse srmLs(org.dcache.srm.v2_1.SrmLsRequest srmLsRequest) throws java.rmi.RemoteException;

    /**
     * Applies to both dir and file
     * 
     *             Authorization checks need to be performed on both fromPath
     * and toPath.
     */
    public org.dcache.srm.v2_1.SrmMvResponse srmMv(org.dcache.srm.v2_1.SrmMvRequest srmMvRequest) throws java.rmi.RemoteException;

    /**
     * The userRequestDescription is a user designated name
     *             for the request.  It can be used in the getRequestID
     *             method to get back the system assigned request ID.  
     * 
     *             Only  pull mode is supported.
     * 
     *             SRM  assigns the requestToken at this time.
     * 
     *             Normally this call will be followed by srmRelease().
     * 
     *             "retryTime" means: if all the file transfer for this
     *             request are complete, then try previously failed transfers
     * for a total time period of "retryTime".
     * 
     *             In case that the retries fail, the return should include
     * an explanation of why the retries failed.
     * 
     *             This call is an asynchronous (non-blocking) call. To
     *             get subsequent status and results, separate calls should
     * be made.
     * 
     *             When the file is ready for the user, the file is implicitly
     * pinned in the cache and lifetime will be enforced. 
     * 
     *             The invocation of srmReleaseFile() is expected for finished
     * files later on.
     */
    public org.dcache.srm.v2_1.SrmPrepareToGetResponse srmPrepareToGet(org.dcache.srm.v2_1.SrmPrepareToGetRequest srmPrepareToGetRequest) throws java.rmi.RemoteException;

    /**
     * Only push mode is supported for srmPrepareToPut.
     * 
     *             StFN ("toSURLInfo" in the TPutFileRequest) has to be
     *             local. If stFN is not specified, SRM will name it automatically
     * and put it in the specified user space. This will be
     *             returned as part of the "transfer URL".
     * 
     *             srmPutDone() is expected after each file is "put" into
     * the allocated space.
     * 
     *             The lifetime of the file starts as soon as SRM get the
     * srmPutDone().  If srmPutDone() is not provided then
     *             the files in that space are subject to removal when
     *             the space lifetime expires.
     * 
     *             "retryTime" is meaningful here only when the file destination
     * is not a local disk, such as tape or MSS.
     * 
     *             In case that the retries fail, the return should include
     * an explanation of why the retires failed.
     */
    public org.dcache.srm.v2_1.SrmPrepareToPutResponse srmPrepareToPut(org.dcache.srm.v2_1.SrmPrepareToPutRequest srmPrepareToPutRequest) throws java.rmi.RemoteException;

    /**
     * Pull mode: copy from remote location to SRM. (e.g. from
     *             remote to MSS.)
     * 
     *             Push mode: copy from SRM to remote location.
     * 
     *             Always release files from source after copy is done.
     * 
     *             When removeSourceFiles=true, then SRM will  remove the
     * source files on behalf of the caller after copy is done. 
     * 
     *             In pull mode, send srmRelease() to remote location when
     * transfer is done.
     * 
     *             If in push mode, then after transfer is done, notify
     *             the caller. User can then release the file. If user
     *             releases a file being copied to another location before
     * it is done, then refuse to release.
     * 
     *             Note there is no protocol negotiation with the client
     *             for this request.
     * 
     *             "retryTime" means: if all the file transfer for this
     *             request are complete, then try previously failed transfers
     * for a total time period of "retryTime".
     * 
     *             In case that the retries fail, the return should include
     * an explanation of why the retires failed.
     * 
     *             When both fromSURL and toSURL are local, perform local
     * copy
     * 
     *             Empty directories are copied as well.
     */
    public org.dcache.srm.v2_1.SrmCopyResponse srmCopy(org.dcache.srm.v2_1.SrmCopyRequest srmCopyRequest) throws java.rmi.RemoteException;

    /**
     * If requestToken is not provided, then the SRM  will
     *              do nothing.
     * 
     *             It has the effect of a release before the file is removed.
     * 
     *             If file is not in cache, do nothing
     */
    public org.dcache.srm.v2_1.SrmRemoveFilesResponse srmRemoveFiles(org.dcache.srm.v2_1.SrmRemoveFilesRequest srmRemoveFilesRequest) throws java.rmi.RemoteException;

    /**
     * dir is ok. Will release recursively for dirs.
     * 
     *             If requestToken is not provided, then the SRM  will
     *             release all the files specified by the siteURLs owned
     *             by this user, regardless of the requestToken.
     * 
     *             If requestToken is not provided, then userID is needed.
     * It may be inferred or provide in the call.
     * 
     *             Releasing  files will be followed by compacting space,
     * if doDynamicCompactFromNowOn was set to true in a previous
     *             srmCompactSpace call.
     */
    public org.dcache.srm.v2_1.SrmReleaseFilesResponse srmReleaseFiles(org.dcache.srm.v2_1.SrmReleaseFilesRequest srmReleaseFilesRequest) throws java.rmi.RemoteException;

    /**
     * Called by user after srmPut()
     */
    public org.dcache.srm.v2_1.SrmPutDoneResponse srmPutDone(org.dcache.srm.v2_1.SrmPutDoneRequest srmPutDoneRequest) throws java.rmi.RemoteException;

    /**
     * Abort all files in this request regardless of the state.
     *             Expired files are released.
     */
    public org.dcache.srm.v2_1.SrmAbortRequestResponse srmAbortRequest(org.dcache.srm.v2_1.SrmAbortRequestRequest srmAbortRequestRequest) throws java.rmi.RemoteException;

    /**
     * Abort all files in this call  regardless of the state
     */
    public org.dcache.srm.v2_1.SrmAbortFilesResponse srmAbortFiles(org.dcache.srm.v2_1.SrmAbortFilesRequest srmAbortFilesRequest) throws java.rmi.RemoteException;

    /**
     * Suspend all files in this request  until srmResumeRequest
     *             is issued
     */
    public org.dcache.srm.v2_1.SrmSuspendRequestResponse srmSuspendRequest(org.dcache.srm.v2_1.SrmSuspendRequestRequest srmSuspendRequestRequest) throws java.rmi.RemoteException;

    /**
     * Resume  suspended  files in this request
     */
    public org.dcache.srm.v2_1.SrmResumeRequestResponse srmResumeRequest(org.dcache.srm.v2_1.SrmResumeRequestRequest srmResumeRequestRequest) throws java.rmi.RemoteException;

    /**
     * If arrayOfFromSURLs is not provided, returns status
     *             for all the file requests in this request.
     */
    public org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse srmStatusOfGetRequest(org.dcache.srm.v2_1.SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest) throws java.rmi.RemoteException;

    /**
     * If arrayOfToSURLs is not provided, returns status for
     *             all the file requests in this request.
     */
    public org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse srmStatusOfPutRequest(org.dcache.srm.v2_1.SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest) throws java.rmi.RemoteException;

    /**
     * If arrayOfFromSURLs and/or arrayOfToSURLs are not provided,
     * return status for all the file requests in this request.
     */
    public org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(org.dcache.srm.v2_1.SrmStatusOfCopyRequestRequest srmStatusOfCopyRequestRequest) throws java.rmi.RemoteException;

    /**

     */
    public org.dcache.srm.v2_1.SrmGetRequestSummaryResponse srmGetRequestSummary(org.dcache.srm.v2_1.SrmGetRequestSummaryRequest srmGetRequestSummaryRequest) throws java.rmi.RemoteException;

    /**
     * newLifeTime is relative to the calling time. Lifetime
     *             will be set from the calling time for the specified period.
     * 
     *             The number of lifetime extensions maybe limited by SRM
     * according to its policies.
     * 
     *             IsExtended = false if SRM refuse to do it. (set newTimeExtended
     * = 0 in this case.)
     * 
     *             If original lifetime is longer than the requested one,
     * then the requested one will be assigned.
     * 
     *             If newLifeTime is not specified, the SRM can use its
     *             default to assign the newLifeTime.
     */
    public org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(org.dcache.srm.v2_1.SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest) throws java.rmi.RemoteException;

    /**
     * If userRequestDescription is null, returns all requests
     *             this user has.
     * 
     *             If the user assigned the same name to multiple requests,
     * he may get back multiple request IDs each with the time
     *             the request was made.
     */
    public org.dcache.srm.v2_1.SrmGetRequestIDResponse srmGetRequestID(org.dcache.srm.v2_1.SrmGetRequestIDRequest srmGetRequestIDRequest) throws java.rmi.RemoteException;
}
