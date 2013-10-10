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
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.ArrayOfTGroupPermission;
import org.dcache.srm.v2_2.ArrayOfTUserPermission;
import org.dcache.srm.v2_2.SrmSetPermissionRequest;
import org.dcache.srm.v2_2.SrmSetPermissionResponse;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SrmSetPermission
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmSetPermission.class);

    private static final String ACL_NOT_SUPPORTED = "ACLs are not supported by the dCache SRM";

    private final AbstractStorageElement storage;
    private final SrmSetPermissionRequest request;
    private final SRMUser user;
    private SrmSetPermissionResponse response;

    public SrmSetPermission(SRMUser user,
                            RequestCredential credential,
                            SrmSetPermissionRequest request,
                            AbstractStorageElement storage,
                            SRM srm,
                            String client_host)
    {
        this.request = request;
        this.user = user;
        this.storage = storage;
    }

    public SrmSetPermissionResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmSetPermission();
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMAuthorizationException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_AUTHORIZATION_FAILURE);
            } catch (SRMInvalidPathException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_PATH);
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage());
            }
        }
        return response;
    }

    private SrmSetPermissionResponse srmSetPermission()
            throws SRMException
    {
        URI surl = URI.create(request.getSURL().toString());

        FileMetaData fmd = storage.getFileMetaData(user, surl, false);

        TPermissionType permissionType = request.getPermissionType();
        if (permissionType == TPermissionType.REMOVE) {
            /* [ SRM 2.2, 3.1.2 ]
             *
             * h) If TPermissionType is REMOVE, then the TPermissionMode must be ignored.
             *
             * We interpret this requirement to apply to user and group ACLs only. Since
             * we don't support these, we don't support REMOVE.
             */
            return getFailedResponse(ACL_NOT_SUPPORTED, TStatusCode.SRM_NOT_SUPPORTED);
        }

        TPermissionMode ownerMode = request.getOwnerPermission();
        TPermissionMode otherMode = request.getOtherPermission();
        TPermissionMode groupMode = null;

        ArrayOfTUserPermission userPermissions = request.getArrayOfUserPermissions();
        if (userPermissions != null) {
            return getFailedResponse(ACL_NOT_SUPPORTED, TStatusCode.SRM_NOT_SUPPORTED);
        }

        ArrayOfTGroupPermission groupPermissions = request.getArrayOfGroupPermissions();
        if (groupPermissions != null && groupPermissions.getGroupPermissionArray() != null) {
            switch (groupPermissions.getGroupPermissionArray().length) {
            case 0:
                break;
            case 1:
                TGroupPermission permission = groupPermissions.getGroupPermissionArray()[0];
                String group = permission.getGroupID();
                if (!group.equals("-") && !group.equals(fmd.group)) {
                    /* The dash is a special dCache convention used by our own SRM client to
                     * indicate that the POSIX group permissions should be updated.
                     */
                    return getFailedResponse(ACL_NOT_SUPPORTED, TStatusCode.SRM_NOT_SUPPORTED);
                }
                groupMode = permission.getMode();
                break;
            default:
                return getFailedResponse(ACL_NOT_SUPPORTED, TStatusCode.SRM_NOT_SUPPORTED);
            }
        }

        fmd.permMode = toNewPermissions(fmd.permMode, permissionType, ownerMode, groupMode, otherMode);
        storage.setFileMetaData(user, fmd);

        return new SrmSetPermissionResponse(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
    }

    private static int toNewPermissions(int permissions, TPermissionType permissionType,
                                        TPermissionMode ownerPermission, TPermissionMode groupPermission,
                                        TPermissionMode otherPermission)
    {
        int iowner = (permissions >> 6) & 0x7;
        int igroup = (permissions >> 3) & 0x7;
        int iother = permissions & 0x7;

        int requestOwner = toMode(ownerPermission, iowner);
        int requestGroup = toMode(groupPermission, igroup);
        int requestOther = toMode(otherPermission, iother);

        if (permissionType == TPermissionType.CHANGE) {
            iowner = requestOwner;
            igroup = requestGroup;
            iother = requestOther;
        } else if (permissionType == TPermissionType.ADD) {
            iowner |= requestOwner;
            igroup |= requestGroup;
            iother |= requestOther;
        }
        return ((iowner << 6) | (igroup << 3)) | iother;
    }

    private static int toMode(TPermissionMode newPermissions, int existingMode)
    {
        /* [ SRM 2.2, 3.1.2 ]
         *
         * g) If TPermissionType is ADD or CHANGE, and TPermissionMode is null, then it
         *    must be assumed that TPermissionMode is READ only.
         *
         * We interpret this requirement to apply to the user and group ACLs only. Since
         * we don't support those, we let an absent TPermissionMode imply that the
         * permission should not be changed.
         */
        return (newPermissions == null) ? existingMode
                : PermissionMaskToTPermissionMode.permissionModetoMask(newPermissions);
    }


    public static final SrmSetPermissionResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmSetPermissionResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmSetPermissionResponse response = new SrmSetPermissionResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
