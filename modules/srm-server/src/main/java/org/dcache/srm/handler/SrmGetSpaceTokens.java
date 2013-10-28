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
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.SrmGetSpaceTokensRequest;
import org.dcache.srm.v2_2.SrmGetSpaceTokensResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmGetSpaceTokens
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmGetSpaceTokens.class);

    private final AbstractStorageElement storage;
    private final SrmGetSpaceTokensRequest request;
    private final SRMUser user;
    private SrmGetSpaceTokensResponse response;

    public SrmGetSpaceTokens(SRMUser user,
                             RequestCredential credential,
                             SrmGetSpaceTokensRequest request,
                             AbstractStorageElement storage,
                             SRM srm,
                             String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
    }

    public SrmGetSpaceTokensResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmGetSpaceTokens();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMException e) {
                response = getFailedResponse(e.toString());
            }
        }
        return response;
    }

    private SrmGetSpaceTokensResponse srmGetSpaceTokens()
            throws SRMException
    {
        String description = request.getUserSpaceTokenDescription();
        String[] spaceTokens = storage.srmGetSpaceTokens(user, description);
        if (spaceTokens.length == 0) {
            throw new SRMInvalidRequestException("No such space tokens");
        }
        return new SrmGetSpaceTokensResponse(
                new TReturnStatus(TStatusCode.SRM_SUCCESS, null),
                new ArrayOfString(spaceTokens));
    }

    public static final SrmGetSpaceTokensResponse getFailedResponse(String text)
    {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmGetSpaceTokensResponse getFailedResponse(String text, TStatusCode statusCode)
    {
        SrmGetSpaceTokensResponse response = new SrmGetSpaceTokensResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}
