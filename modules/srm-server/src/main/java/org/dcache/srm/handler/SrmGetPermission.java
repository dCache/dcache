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
import org.dcache.srm.v2_2.ArrayOfTGroupPermission;
import org.dcache.srm.v2_2.ArrayOfTPermissionReturn;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionReturn;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

public class SrmGetPermission
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmGetPermission.class);
    private final AbstractStorageElement storage;
    private final SrmGetPermissionRequest request;
    private final SRMUser user;
    private SrmGetPermissionResponse response;

    public SrmGetPermission(SRMUser user,
                            RequestCredential credential,
                            SrmGetPermissionRequest request,
                            AbstractStorageElement storage,
                            SRM srm,
                            String client_host)
    {
        this.request = request;
        this.user = user;
        this.storage = storage;
    }

    public SrmGetPermissionResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmGetPermission();
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                return getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                return getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmGetPermissionResponse srmGetPermission()
            throws SRMInvalidRequestException, SRMInternalErrorException
    {
        org.apache.axis.types.URI[] surls = request.getArrayOfSURLs().getUrlArray();
        if (surls == null || surls.length == 0) {
            throw new SRMInvalidRequestException("arrayOfSURLs is empty");
        }
        int length = surls.length;
        TPermissionReturn permissionsArray[] = new TPermissionReturn[length];
        boolean hasFailure = false;
        boolean hasSuccess = false;
        for (int i = 0; i < length; i++) {
            TPermissionReturn p = new TPermissionReturn();
            TReturnStatus returnStatus;
            try {
                FileMetaData fmd = storage.getFileMetaData(user, URI.create(surls[i].toString()), false);
                copyPermissions(fmd, p);
                returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
                hasSuccess = true;
            } catch (SRMInternalErrorException e) {
                throw e;
            } catch (SRMAuthorizationException e) {
                returnStatus = new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
                hasFailure = true;
            } catch (SRMInvalidPathException e) {
                returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
                hasFailure = true;
            } catch (SRMException e) {
                LOGGER.warn(e.toString());
                returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
                hasFailure = true;
            }
            p.setSurl(surls[i]);
            p.setStatus(returnStatus);
            permissionsArray[i] = p;
        }
        return new SrmGetPermissionResponse(
                getSummaryReturnStatus(hasFailure, hasSuccess), new ArrayOfTPermissionReturn(permissionsArray));
    }

    private static void copyPermissions(FileMetaData fmd, TPermissionReturn p)
    {
        String owner = fmd.owner;
        String group = fmd.group;
        int permissions = fmd.permMode;
        TPermissionMode upm = PermissionMaskToTPermissionMode.maskToTPermissionMode(((permissions >> 6) & 0x7));
        TPermissionMode gpm = PermissionMaskToTPermissionMode.maskToTPermissionMode(((permissions >> 3) & 0x7));
        TPermissionMode opm = PermissionMaskToTPermissionMode.maskToTPermissionMode((permissions & 0x7));

        TGroupPermission[] groupPermissionArray = new TGroupPermission[] { new TGroupPermission(group, gpm) };

        p.setArrayOfGroupPermissions(new ArrayOfTGroupPermission(groupPermissionArray));
        p.setOwnerPermission(upm);
        p.setOtherPermission(opm);
        p.setOwner(owner);
    }

    public static final SrmGetPermissionResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmGetPermissionResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmGetPermissionResponse response = new SrmGetPermissionResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
