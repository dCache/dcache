package org.dcache.srm.handler;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SrmAbortRequest
{
    private final SrmAbortRequestRequest request;
    private SrmAbortRequestResponse response;

    public SrmAbortRequest(
            SRMUser user,
            RequestCredential credential,
            SrmAbortRequestRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String clientHost)
    {
        this.request = request;
    }

    public SrmAbortRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = abortRequest();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmAbortRequestResponse abortRequest()
            throws SRMInvalidRequestException
    {
        Request requestToAbort = Request.getRequest(this.request.getRequestToken(), Request.class);
        try (JDC ignored = requestToAbort.applyJdc()) {
            return new SrmAbortRequestResponse(requestToAbort.abort());
        }
    }

    public static final SrmAbortRequestResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmAbortRequestResponse getFailedResponse(String error,
                                                                  TStatusCode statusCode)
    {
        return new SrmAbortRequestResponse(new TReturnStatus(statusCode, error));
    }
}
