package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn;
import org.dcache.srm.v2_2.SrmGetRequestTokensRequest;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.TRequestTokenReturn;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmGetRequestTokens
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmGetRequestTokens.class);

    private final AbstractStorageElement storage;
    private final SrmGetRequestTokensRequest request;
    private final SRMUser user;
    private SrmGetRequestTokensResponse response;

    public SrmGetRequestTokens(SRMUser user,
                               RequestCredential credential,
                               SrmGetRequestTokensRequest request,
                               AbstractStorageElement storage,
                               SRM srm,
                               String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
    }

    public SrmGetRequestTokensResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmGetRequestTokens();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_FAILURE);
            }
        }
        return response;
    }

    private SrmGetRequestTokensResponse srmGetRequestTokens()
            throws SRMException
    {
        String description = request.getUserRequestDescription();
        String[] requestTokens = storage.srmGetRequestTokens(user, description);
        if (requestTokens.length == 0) {
            throw new SRMInvalidRequestException("No such requests");
        }
        TRequestTokenReturn[] requestTokenReturns =
                new TRequestTokenReturn[requestTokens.length];
        for (int i = 0; i < requestTokens.length; ++i) {
            requestTokenReturns[i] = new TRequestTokenReturn(requestTokens[i], null);
        }
        return new SrmGetRequestTokensResponse(
                new TReturnStatus(TStatusCode.SRM_SUCCESS, null),
                new ArrayOfTRequestTokenReturn(requestTokenReturns));
    }

    public static final SrmGetRequestTokensResponse getFailedResponse(String text)
    {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmGetRequestTokensResponse getFailedResponse(String text, TStatusCode statusCode)
    {
        SrmGetRequestTokensResponse response = new SrmGetRequestTokensResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}
