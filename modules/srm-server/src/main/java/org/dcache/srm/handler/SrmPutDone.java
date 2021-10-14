package org.dcache.srm.handler;

import static java.util.Objects.requireNonNull;

import org.apache.axis.types.URI;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMRequestTimedOutException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SrmPutDone {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(SrmPutDone.class);

    private final SrmPutDoneRequest request;
    private final SRMUser user;
    private SrmPutDoneResponse response;

    public SrmPutDone(SRMUser user,
          SrmPutDoneRequest request,
          AbstractStorageElement storage,
          SRM srm,
          String clientHost) {
        this.request = requireNonNull(request);
        this.user = requireNonNull(user);
    }

    public SrmPutDoneResponse getResponse() {
        if (response == null) {
            try {
                response = srmPutDone();
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage(), e.getStatusCode());
            }
        }
        return response;
    }

    private SrmPutDoneResponse srmPutDone()
          throws SRMInvalidRequestException, SRMRequestTimedOutException, SRMAbortedException,
          SRMInternalErrorException, SRMAuthorizationException {
        URI[] surls = getSurls(request);
        PutRequest putRequest = Request.getRequest(request.getRequestToken(), PutRequest.class);
        try (JDC ignored = putRequest.applyJdc()) {
            putRequest.wlock();
            try {
                if (!user.hasAccessTo(putRequest)) {
                    throw new SRMAuthorizationException(
                          "User is not the owner of request " + request.getRequestToken() + ".");
                }

                switch (putRequest.getState()) {
                    case FAILED:
                        if (putRequest.getStatusCode() == TStatusCode.SRM_REQUEST_TIMED_OUT) {
                            throw new SRMRequestTimedOutException("Total request time exceeded");
                        }
                        break;
                    case CANCELED:
                        throw new SRMAbortedException("Request has been aborted.");
                }

                TSURLReturnStatus[] returnStatuses = new TSURLReturnStatus[surls.length];
                for (int i = 0; i < surls.length; ++i) {
                    if (surls[i] == null) {
                        throw new SRMInvalidRequestException("SiteURLs[" + (i + 1) + "] is null.");
                    }
                    TReturnStatus returnStatus;
                    try {
                        PutFileRequest fileRequest = putRequest
                              .getFileRequestBySurl(java.net.URI.create(surls[i].toString()));
                        try (JDC ignore = fileRequest.applyJdc()) {
                            returnStatus = fileRequest.done(this.user);
                        }
                    } catch (SRMFileRequestNotFoundException e) {
                        returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH,
                              "File does not exist.");
                    }
                    returnStatuses[i] = new TSURLReturnStatus(surls[i], returnStatus);
                }

                putRequest.updateStatus();

                return new SrmPutDoneResponse(
                      ReturnStatuses.getSummaryReturnStatus(returnStatuses),
                      new ArrayOfTSURLReturnStatus(returnStatuses));
            } finally {
                putRequest.wunlock();
            }
        }
    }

    private static URI[] getSurls(SrmPutDoneRequest request) throws SRMInvalidRequestException {
        ArrayOfAnyURI arrayOfSURLs = request.getArrayOfSURLs();
        if (arrayOfSURLs == null || arrayOfSURLs.getUrlArray().length == 0) {
            throw new SRMInvalidRequestException("arrayOfSURLs is empty.");
        }
        return arrayOfSURLs.getUrlArray();
    }

    public static final SrmPutDoneResponse getFailedResponse(String error) {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmPutDoneResponse getFailedResponse(String error, TStatusCode statusCode) {
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();
        srmPutDoneResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPutDoneResponse;
    }
}
