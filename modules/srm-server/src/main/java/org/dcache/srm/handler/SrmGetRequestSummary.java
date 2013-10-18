package org.dcache.srm.handler;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfTRequestSummary;
import org.dcache.srm.v2_2.SrmGetRequestSummaryRequest;
import org.dcache.srm.v2_2.SrmGetRequestSummaryResponse;
import org.dcache.srm.v2_2.TRequestSummary;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

public class SrmGetRequestSummary
{
    private final SrmGetRequestSummaryRequest request;
    private SrmGetRequestSummaryResponse response;

    public SrmGetRequestSummary(
            SRMUser user,
            RequestCredential credential,
            SrmGetRequestSummaryRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String client_host)
    {
        this.request = request;
    }

    public SrmGetRequestSummaryResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmGetRequestSummary();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmGetRequestSummaryResponse srmGetRequestSummary() throws SRMInvalidRequestException
    {
        String[] requestTokens = request.getArrayOfRequestTokens().getStringArray();
        if (requestTokens == null || requestTokens.length == 0) {
            throw new SRMInvalidRequestException("arrayOfRequestTokens is empty");
        }
        TRequestSummary[] requestSummaries = new TRequestSummary[requestTokens.length];
        for (int i = 0; i < requestTokens.length; ++i) {
            String requestToken = requestTokens[i];
            TRequestSummary summary;
            try {
                ContainerRequest<?> request = Request.getRequest(requestToken, ContainerRequest.class);
                try (JDC ignored = request.applyJdc()) {
                    summary = request.getRequestSummary();
                }
            } catch (SRMInvalidRequestException e) {
                summary = new TRequestSummary();
                summary.setRequestToken(requestToken);
                summary.setStatus(new TReturnStatus(TStatusCode.SRM_INVALID_REQUEST, e.getMessage()));
            }
            requestSummaries[i] = summary;
        }
        return new SrmGetRequestSummaryResponse(
                getSummaryReturnStatus(requestSummaries),
                new ArrayOfTRequestSummary(requestSummaries));
    }

    public static final SrmGetRequestSummaryResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmGetRequestSummaryResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        TReturnStatus status = new TReturnStatus(statusCode, error);
        SrmGetRequestSummaryResponse srmGetRequestSummaryResponse = new SrmGetRequestSummaryResponse();
        srmGetRequestSummaryResponse.setReturnStatus(status);
        return srmGetRequestSummaryResponse;
    }
}
