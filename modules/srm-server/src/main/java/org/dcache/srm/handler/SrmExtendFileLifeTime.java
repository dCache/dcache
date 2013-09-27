package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfTSURLLifetimeReturnStatus;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLLifetimeReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SrmExtendFileLifeTime
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmExtendFileLifeTime.class);

    private final AbstractStorageElement storage;
    private final SrmExtendFileLifeTimeRequest request;
    private final SRMUser user;
    private final Configuration configuration;
    private SrmExtendFileLifeTimeResponse response;

    public SrmExtendFileLifeTime(SRMUser user,
                                 RequestCredential credential,
                                 SrmExtendFileLifeTimeRequest request,
                                 AbstractStorageElement storage,
                                 SRM srm,
                                 String clientHost)
    {
        this.request = request;
        this.user = user;
        this.storage = storage;
        this.configuration = srm.getConfiguration();
    }

    public SrmExtendFileLifeTimeResponse getResponse()
    {
        if (response == null) {
            try {
                response = extendFileLifeTime();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            }
        }
        return response;
    }

    private SrmExtendFileLifeTimeResponse extendFileLifeTime()
            throws SRMInvalidRequestException, SRMInternalErrorException
    {
        org.apache.axis.types.URI[] surls = request.getArrayOfSURLs().getUrlArray();
        if (surls == null) {
            throw new SRMInvalidRequestException("arrayOfSURLs is required");
        }
        /* [ SRM 2.2, 5.16.2 ]
         *
         * a) srmExtendFileLifeTime allows to change only one type of lifetime at a
         *    time (either SURL lifetime by the newFileLifetime or pin lifetime by
         *    the newPinLifetime), depending on the presence or absence of the request
         *    token. Either newFileLifetime or newPinLifetime must be provided. When
         *    both newFileLifetime and newPinLifetime are provided in the same request,
         *    the request must be invalid, and SRM_INVALID_REQUEST must be returned.
         */
        String token = request.getRequestToken();
        Integer newFileLifetime = request.getNewFileLifeTime();
        Integer newPinLifetime = request.getNewPinLifeTime();
        if (newFileLifetime != null && newPinLifetime != null) {
            throw new SRMInvalidRequestException("newFileLifetime and newPinLifetime must not be present at the same time");
        }
        if (token == null) {
            if (newFileLifetime == null) {
                throw new SRMInvalidRequestException("requestToken and newFileLifetime must not be absent at the same time");
            }
            return extendSurlLifeTime(surls, newFileLifetime);
        } else {
            if (newPinLifetime == null) {
                throw new SRMInvalidRequestException("newPinLifetime is required when requestToken is present");
            }
            return extendTurlOrPinLifeTime(token, surls, newPinLifetime);
        }
    }

    private SrmExtendFileLifeTimeResponse extendSurlLifeTime(org.apache.axis.types.URI[] surls,
                                                             int newFileLifetime)
            throws SRMInternalErrorException
    {
        long newLifetimeInMillis = toMillis(newFileLifetime, Long.MAX_VALUE);
        int len = surls.length;
        TSURLLifetimeReturnStatus[] surlStatus = new TSURLLifetimeReturnStatus[len];
        for (int i = 0; i < len; ++i) {
            surlStatus[i] = extendSurlLifeTime(surls[i], newLifetimeInMillis);
        }
        return new SrmExtendFileLifeTimeResponse(
                getSummaryReturnStatus(surlStatus),
                new ArrayOfTSURLLifetimeReturnStatus(surlStatus));
    }

    private TSURLLifetimeReturnStatus extendSurlLifeTime(org.apache.axis.types.URI surl,
                                                         long newLifetimeInMillis)
            throws SRMInternalErrorException
    {
        TSURLLifetimeReturnStatus status = new TSURLLifetimeReturnStatus();
        status.setSurl(surl);
        TReturnStatus returnStatus;
        try {
            long lifetimeLeftInMillis =
                    storage.srmExtendSurlLifetime(user, URI.create(surl.toString()), newLifetimeInMillis);
            status.setFileLifetime(toSeconds(lifetimeLeftInMillis));
            returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } catch (SRMInternalErrorException e) {
            throw new SRMInternalErrorException("File lifetime extension failed for SURL " +
                    surl + ": " + e.getMessage(), e);
        } catch (SRMException e) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, e.toString());
        }
        status.setStatus(returnStatus);
        return status;
    }

    private long getMaxLifetime(ContainerRequest<?> containerRequest)
    {
        long configMaximumLifetime;
        if (containerRequest instanceof CopyRequest) {
            configMaximumLifetime = configuration.getCopyLifetime();
        } else if (containerRequest instanceof PutRequest) {
            configMaximumLifetime = configuration.getPutLifetime();
        } else if (containerRequest instanceof BringOnlineRequest) {
            configMaximumLifetime = configuration.getBringOnlineLifetime();
        } else {
            configMaximumLifetime = configuration.getGetLifetime();
        }
        return configMaximumLifetime;
    }

    private SrmExtendFileLifeTimeResponse extendTurlOrPinLifeTime(
            String requestToken, org.apache.axis.types.URI[] surls, int newLifetime)
            throws SRMInvalidRequestException, SRMInternalErrorException
    {
        ContainerRequest<?> containerRequest =
                Request.getRequest(requestToken, ContainerRequest.class);
        try (JDC ignored = containerRequest.applyJdc()) {
            long maxLifetime = getMaxLifetime(containerRequest);
            long newLifetimeInMillis = toMillis(newLifetime, maxLifetime);
            int len = surls.length;
            TSURLLifetimeReturnStatus[] surlStatus = new TSURLLifetimeReturnStatus[len];
            for (int i = 0; i < len; ++i) {
                surlStatus[i] = extendTurlOrPinLifeTime(containerRequest, surls[i], newLifetimeInMillis);
            }
            return new SrmExtendFileLifeTimeResponse(
                    getSummaryReturnStatus(surlStatus),
                    new ArrayOfTSURLLifetimeReturnStatus(surlStatus));
        }
    }

    private TSURLLifetimeReturnStatus extendTurlOrPinLifeTime(
            ContainerRequest<?> request, org.apache.axis.types.URI surl, long newLifetimeInMillis)
            throws SRMInternalErrorException
    {
        TSURLLifetimeReturnStatus status = new TSURLLifetimeReturnStatus();
        status.setSurl(surl);
        TReturnStatus returnStatus;
        try {
            FileRequest<?> fileRequest =
                    request.getFileRequestBySurl(URI.create(surl.toString()));
            long lifetimeLeftMillis =
                    fileRequest.extendLifetime(newLifetimeInMillis);
            status.setPinLifetime(toSeconds(lifetimeLeftMillis));
            returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } catch (SRMFileRequestNotFoundException e) {
            returnStatus = new TReturnStatus(
                    TStatusCode.SRM_INVALID_PATH, "SURL does match any existing file request associated with the request token");
        } catch (SRMInvalidRequestException e) {
            returnStatus = new TReturnStatus(
                    TStatusCode.SRM_INVALID_REQUEST, "TURL is no longer valid and cannot be extended");
        } catch (SRMReleasedException e) {
            returnStatus = new TReturnStatus(
                    TStatusCode.SRM_RELEASED, "TURL has been released and cannot be extended");
        } catch (SRMAbortedException e) {
            returnStatus = new TReturnStatus(
                    TStatusCode.SRM_ABORTED, "TURL has been aborted and cannot be extended");
        } catch (SRMInternalErrorException e) {
            throw new SRMInternalErrorException("File lifetime extension failed for request " + request.getId()
                    + " with SURL " + surl + ": " + e.getMessage(), e);
        } catch (SRMException e) {
            LOGGER.warn("File lifetime extension failed for request {} with SURL {}: {}",
                    request.getId(), surl, e.getMessage());
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE,
                    "TURL for request " + request.getId() + " with SURL " + surl + " cannot be extended: " + e.getMessage());
        }
        status.setStatus(returnStatus);
        return status;
    }

    private static TReturnStatus getSummaryReturnStatus(TSURLLifetimeReturnStatus[] surlStatus)
    {
        boolean hasFailure = false;
        boolean hasSuccess = false;
        for (TSURLLifetimeReturnStatus returnStatus : surlStatus) {
            if (returnStatus.getStatus().getStatusCode() == TStatusCode.SRM_SUCCESS) {
                hasSuccess = true;
            } else {
                hasFailure = true;
            }
        }
        return ReturnStatuses.getSummaryReturnStatus(hasFailure, hasSuccess);
    }

    private static long toMillis(int seconds, long max)
    {
        /* [ SRM 2.2, 1.20 ]
         *
         * o A negative value (-1) indicates “infinite (indefinite)” time.
         */
        return (seconds >= 0) ? Math.min(SECONDS.toMillis(seconds), max) : -1;
    }

    private static int toSeconds(long millis)
    {
        /* [ SRM 2.2, 1.20 ]
         *
         * o A negative value (-1) indicates “infinite (indefinite)” time.
         */
        return (millis >= 0) ? (int) MILLISECONDS.toSeconds(millis) : -1;
    }

    public static final SrmExtendFileLifeTimeResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmExtendFileLifeTimeResponse getFailedResponse(
            String error, TStatusCode statusCode)
    {
        SrmExtendFileLifeTimeResponse response = new SrmExtendFileLifeTimeResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
