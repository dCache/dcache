package org.dcache.srm.handler;

import org.apache.axis.types.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmStatusOfCopyRequest
{
    private final SrmStatusOfCopyRequestRequest request;
    private SrmStatusOfCopyRequestResponse response;

    public SrmStatusOfCopyRequest(SRMUser user,
                                  RequestCredential credential,
                                  SrmStatusOfCopyRequestRequest request,
                                  AbstractStorageElement storage,
                                  SRM srm,
                                  String clientHost)
    {
        this.request = checkNotNull(request);
    }

    public SrmStatusOfCopyRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmStatusOfCopyRequest();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest()
            throws SRMInvalidRequestException
    {
        CopyRequest copyRequest = Request.getRequest(request.getRequestToken(), CopyRequest.class);
        try (JDC ignored = copyRequest.applyJdc()) {
            if (request.getArrayOfSourceSURLs() == null || request.getArrayOfTargetSURLs() == null) {
                return copyRequest.getSrmStatusOfCopyRequest();
            }

            URI[] fromsurls = request.getArrayOfSourceSURLs().getUrlArray();
            URI[] tosurls = request.getArrayOfTargetSURLs().getUrlArray();
            if (fromsurls.length == 0 || tosurls.length == 0) {
                return copyRequest.getSrmStatusOfCopyRequest();
            }
            if (tosurls.length != fromsurls.length) {
                throw new SRMInvalidRequestException("Length of arrayOfSourceSURLs and arrayOfTargetSURLs differ.");
            }
            return copyRequest.getSrmStatusOfCopyRequest(fromsurls, tosurls);
        }
    }

    public static final SrmStatusOfCopyRequestResponse getFailedResponse(String text)
    {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmStatusOfCopyRequestResponse getFailedResponse(String text, TStatusCode statusCode)
    {
        SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}
