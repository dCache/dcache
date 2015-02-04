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
import org.dcache.srm.v2_2.SrmMvRequest;
import org.dcache.srm.v2_2.SrmMvResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmMv
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmMv.class);

    private final AbstractStorageElement storage;
    private final SrmMvRequest request;
    private final SRMUser user;
    private final SRM srm;
    private SrmMvResponse response;

    public SrmMv(SRMUser user,
                 SrmMvRequest request,
                 AbstractStorageElement storage,
                 SRM srm,
                 String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
        this.srm = checkNotNull(srm);
    }

    public SrmMvResponse getResponse()
    {
        if (response == null) {
            response = srmMv();
        }
        return response;
    }

    private SrmMvResponse srmMv()
    {
        TReturnStatus returnStatus;
        try {
            URI to_surl = URI.create(request.getToSURL().toString());
            URI from_surl = URI.create(request.getFromSURL().toString());
            // [SRM 2.2, 4.6.3]     SRM_INVALID_PATH: status of fromSURL is SRM_FILE_BUSY.
            // [SRM 2.2, 4.6.2, c)] srmMv must fail on SURL that its status is SRM_FILE_BUSY,
            //                      and SRM_FILE_BUSY must be returned.
            // [SRM 2.2, 4.6.2, e)] When moving an SURL to already existing SURL,
            //                      SRM_DUPLICATION_ERROR must be returned.
            //
            // The SRM spec is somewhat inconsistent on what the correct return code should be.
            // Instead we use SRM_DUPLICATION_ERROR if the target SURL is busy (consistent with
            // how this situation is handled in srmPrepareToPut) and SRM_FILE_BUSY if the source
            // SURL is busy.
            if (srm.isFileBusy(from_surl)) {
                returnStatus = new TReturnStatus(TStatusCode.SRM_FILE_BUSY,
                        "The source SURL is being used by another client.");
            } else if (srm.isFileBusy(to_surl)) {
                returnStatus = new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR,
                        "The target SURL is being used by another client.");
            } else {
                storage.moveEntry(user, from_surl, to_surl);
                returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            }
        } catch (SRMDuplicationException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR, e.getMessage());
        } catch (SRMInternalErrorException e) {
            LOGGER.error(e.toString());
            returnStatus = new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
        } catch (SRMInvalidPathException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
        } catch (SRMAuthorizationException e) {
            LOGGER.warn(e.toString());
            returnStatus = new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
        } catch (SRMException e) {
            LOGGER.error(e.toString());
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        }
        return new SrmMvResponse(returnStatus);
    }

    public static final SrmMvResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmMvResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmMvResponse response = new SrmMvResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
