package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.ArrayOfTSURLPermissionReturn;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TStatusCode;

public class SrmCheckPermission
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmCheckPermission.class);
    private final AbstractStorageElement storage;
    private final SrmCheckPermissionRequest request;
    private final SRMUser user;
    private SrmCheckPermissionResponse response;

    public SrmCheckPermission(SRMUser user,
                              RequestCredential credential,
                              SrmCheckPermissionRequest request,
                              AbstractStorageElement storage,
                              SRM srm,
                              String client_host)
    {
        this.request = request;
        this.user = user;
        this.storage = storage;
    }

    public SrmCheckPermissionResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmCheckPermission();
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                return getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                return getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmCheckPermissionResponse srmCheckPermission()
            throws SRMInternalErrorException, SRMInvalidRequestException
    {
        org.apache.axis.types.URI[] surls = request.getArrayOfSURLs().getUrlArray();
        if (surls == null || surls.length == 0) {
            throw new SRMInvalidRequestException("arrayOfSURLs is empty");
        }
        int length = surls.length;
        TSURLPermissionReturn permissions[] = new TSURLPermissionReturn[length];
        boolean hasSuccess = false;
        boolean hasFailure = false;
        for (int i = 0; i < length; i++) {
            TReturnStatus returnStatus;
            TPermissionMode pm = null;
            try {
                FileMetaData fmd = storage.getFileMetaData(user, URI.create(surls[i].toString()), false);
                int mode = fmd.permMode;
                if (fmd.isOwner(user)) {
                    pm = PermissionMaskToTPermissionMode.maskToTPermissionMode(((mode >> 6) & 0x7));
                } else if (fmd.isGroupMember(user)) {
                    pm = PermissionMaskToTPermissionMode.maskToTPermissionMode(((mode >> 3) & 0x7));
                } else {
                    pm = PermissionMaskToTPermissionMode.maskToTPermissionMode((mode & 0x7));
                }
                returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
                hasSuccess = true;
            } catch (SRMInternalErrorException e) {
                throw e;
            } catch (SRMInvalidPathException e) {
                returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
                hasFailure = true;
            } catch (SRMAuthorizationException e) {
                returnStatus = new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
                hasFailure = true;
            } catch (SRMException e) {
                LOGGER.warn(e.toString());
                returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
                hasFailure = true;
            }

            permissions[i] = new TSURLPermissionReturn(surls[i], returnStatus, pm);
        }
        return new SrmCheckPermissionResponse(
                ReturnStatuses.getSummaryReturnStatus(hasFailure, hasSuccess),
                new ArrayOfTSURLPermissionReturn(permissions));
    }

    public static final SrmCheckPermissionResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmCheckPermissionResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        TReturnStatus status = new TReturnStatus(statusCode, error);
        SrmCheckPermissionResponse response = new SrmCheckPermissionResponse();
        response.setReturnStatus(status);
        return response;
    }
}
