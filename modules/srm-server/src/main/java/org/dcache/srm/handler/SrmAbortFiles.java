package org.dcache.srm.handler;

import static java.util.Objects.requireNonNull;
import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

import java.net.URI;
import java.net.URISyntaxException;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SrmAbortFiles {

    private final SrmAbortFilesRequest request;
    private final SRMUser user;
    private SrmAbortFilesResponse response;

    public SrmAbortFiles(
          SRMUser user,
          SrmAbortFilesRequest request,
          AbstractStorageElement storage,
          SRM srm,
          String clientHost) {
        this.user = user;
        this.request = requireNonNull(request);
    }

    public SrmAbortFilesResponse getResponse() {
        if (response == null) {
            try {
                response = abortFiles();
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage(), e.getStatusCode());
            }
        }
        return response;
    }

    private SrmAbortFilesResponse abortFiles()
          throws SRMInvalidRequestException, SRMAuthorizationException {
        ContainerRequest<?> requestToAbort =
              Request.getRequest(this.request.getRequestToken(), ContainerRequest.class);
        try (JDC ignored = requestToAbort.applyJdc()) {
            if (!user.hasAccessTo(requestToAbort)) {
                throw new SRMAuthorizationException(
                      "User is not the owner of request " + request.getRequestToken() + ".");
            }

            org.apache.axis.types.URI[] surls = getSurls();

            TSURLReturnStatus[] surlReturnStatusArray = new TSURLReturnStatus[surls.length];
            for (int i = 0; i < surls.length; i++) {
                TReturnStatus returnStatus = abortSurl(requestToAbort, surls[i]);
                surlReturnStatusArray[i] = new TSURLReturnStatus(surls[i], returnStatus);
            }

            requestToAbort.updateStatus();

            return new SrmAbortFilesResponse(
                  getSummaryReturnStatus(surlReturnStatusArray),
                  new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        }
    }

    private TReturnStatus abortSurl(ContainerRequest<?> request, org.apache.axis.types.URI surl) {
        try {
            request.getFileRequestBySurl(new URI(surl.toString()))
                  .abort("File request aborted by client.");
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } catch (SRMFileRequestNotFoundException | URISyntaxException e) {
            return new TReturnStatus(TStatusCode.SRM_INVALID_PATH,
                  "SURL does match any existing file request associated with the request token");
        } catch (SRMException | IllegalStateTransition e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        }
    }

    private org.apache.axis.types.URI[] getSurls() throws SRMInvalidRequestException {
        ArrayOfAnyURI arrayOfSURLs = request.getArrayOfSURLs();
        if (arrayOfSURLs == null || arrayOfSURLs.getUrlArray().length == 0) {
            throw new SRMInvalidRequestException("Request contains no SURL");
        }
        return arrayOfSURLs.getUrlArray();
    }

    public static SrmAbortFilesResponse getFailedResponse(String error) {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static SrmAbortFilesResponse getFailedResponse(String error,
          TStatusCode statusCode) {
        SrmAbortFilesResponse srmAbortFilesResponse = new SrmAbortFilesResponse();
        srmAbortFilesResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmAbortFilesResponse;
    }
}
