package org.dcache.srm.handler;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmAbortRequest
{
    private final SrmAbortRequestRequest request;
    private final SRMUser user;
    private SrmAbortRequestResponse response;

    public SrmAbortRequest(
            SRMUser user,
            RequestCredential credential,
            SrmAbortRequestRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String clientHost)
    {
        this.user = user;
        this.request = checkNotNull(request);
    }

    public SrmAbortRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = abortRequest();
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage(), e.getStatusCode());
            }
        }
        return response;
    }

    private SrmAbortRequestResponse abortRequest()
            throws SRMInvalidRequestException, SRMAuthorizationException
    {
        Request requestToAbort = Request.getRequest(request.getRequestToken(), Request.class);
        try (JDC ignored = requestToAbort.applyJdc()) {
            if (!user.hasAccessTo(requestToAbort)) {
                throw new SRMAuthorizationException("User is not the owner of request " + request.getRequestToken() + ".");
            }
            return new SrmAbortRequestResponse(requestToAbort.abort("Request aborted by client."));
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
