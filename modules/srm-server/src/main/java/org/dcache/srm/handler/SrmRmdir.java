package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMNonEmptyDirectoryException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmRmdir
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmRmdir.class);

    private final AbstractStorageElement storage;
    private final SrmRmdirRequest request;
    private final SRMUser user;
    private final SRM srm;
    private SrmRmdirResponse response;

    public SrmRmdir(SRMUser user,
                    SrmRmdirRequest request,
                    AbstractStorageElement storage,
                    SRM srm,
                    String clientHost)
    {
        this.srm = srm;
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
    }

    public SrmRmdirResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmRmdir();
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMAuthorizationException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_AUTHORIZATION_FAILURE);
            } catch (SRMInvalidPathException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_PATH);
            } catch (SRMNonEmptyDirectoryException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_NON_EMPTY_DIRECTORY);
            } catch (SRMException e) {
                response = getFailedResponse(e.toString());
            }
        }
        return response;
    }

    private SrmRmdirResponse srmRmdir()
            throws SRMException
    {
        URI surl = URI.create(request.getSURL().toString());

        /* If surl is a prefix to any active upload, then we report the directory as
         * non-empty. This is not strictly required by the SRM spec, however S2 tests
         * (usecase.RmdirBeingPutInto) check for this behaviour.
         */
        srm.checkRemoveDirectory(surl);

        storage.removeDirectory(user, surl, request.getRecursive() != null && request.getRecursive());
        return new SrmRmdirResponse(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
    }

    public static final SrmRmdirResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmRmdirResponse getFailedResponse(String error,
                                                           TStatusCode statusCode)
    {
        SrmRmdirResponse response = new SrmRmdirResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
