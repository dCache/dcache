package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmMkdir
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmMkdir.class.getName());
    private final AbstractStorageElement storage;
    private final SrmMkdirRequest request;
    private final SRMUser user;
    SrmMkdirResponse response;

    public SrmMkdir(SRMUser user,
                    SrmMkdirRequest request,
                    AbstractStorageElement storage,
                    SRM srm,
                    String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
    }

    public SrmMkdirResponse getResponse()
    {
        if (response == null) {
            response = srmMkdir();
        }
        return response;
    }

    private SrmMkdirResponse srmMkdir()
    {
        TReturnStatus returnStatus;
        try {
            storage.createDirectory(user, URI.create(request.getSURL().toString()));
            returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } catch (SRMInternalErrorException e) {
            LOGGER.error(e.getMessage());
            returnStatus = new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
        } catch (SRMDuplicationException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR, e.getMessage());
        } catch (SRMAuthorizationException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
        } catch (SRMInvalidPathException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
        } catch (SRMException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        }
        return new SrmMkdirResponse(returnStatus);
    }

    public static final SrmMkdirResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmMkdirResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmMkdirResponse response = new SrmMkdirResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
