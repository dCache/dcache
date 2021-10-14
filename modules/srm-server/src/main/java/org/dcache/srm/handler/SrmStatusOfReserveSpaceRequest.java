package org.dcache.srm.handler;

import static java.util.Objects.requireNonNull;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SrmStatusOfReserveSpaceRequest {

    private final SrmStatusOfReserveSpaceRequestRequest request;
    private SrmStatusOfReserveSpaceRequestResponse response;

    public SrmStatusOfReserveSpaceRequest(SRMUser user,
          SrmStatusOfReserveSpaceRequestRequest request,
          AbstractStorageElement storage,
          SRM srm,
          String clientHost) {
        this.request = requireNonNull(request);
    }

    public SrmStatusOfReserveSpaceRequestResponse getResponse() {
        if (response == null) {
            try {
                response = reserveSpaceStatus();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmStatusOfReserveSpaceRequestResponse reserveSpaceStatus()
          throws SRMInvalidRequestException {
        String requestToken = request.getRequestToken();
        if (requestToken == null) {
            throw new SRMInvalidRequestException("Missing requestToken");
        }
        ReserveSpaceRequest request = ReserveSpaceRequest.getRequest(requestToken);
        try (JDC ignored = request.applyJdc()) {
            return request.getSrmStatusOfReserveSpaceRequestResponse();
        }
    }

    public static final SrmStatusOfReserveSpaceRequestResponse getFailedResponse(String text) {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmStatusOfReserveSpaceRequestResponse getFailedResponse(String text,
          TStatusCode statusCode) {
        SrmStatusOfReserveSpaceRequestResponse response = new SrmStatusOfReserveSpaceRequestResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}
