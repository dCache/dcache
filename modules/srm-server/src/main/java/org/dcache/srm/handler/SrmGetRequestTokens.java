package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.util.Set;

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

    private final SRM srm;
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
        this.srm = checkNotNull(srm);
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

    private String[] getRequestTokens(SRMUser user,String description)
            throws SRMException
    {
        try {
            Set<Long> tokens = srm.getBringOnlineRequestIds(user,
                                                            description);
            tokens.addAll(srm.getGetRequestIds(user,
                                               description));
            tokens.addAll(srm.getPutRequestIds(user,
                                               description));
            tokens.addAll(srm.getCopyRequestIds(user,
                                                description));
            tokens.addAll(srm.getLsRequestIds(user,
                                              description));
            Long[] tokenLongs = tokens
                    .toArray(new Long[tokens.size()]);
            String[] tokenStrings = new String[tokenLongs.length];
            for (int i = 0; i < tokenLongs.length; ++i) {
                tokenStrings[i] = tokenLongs[i].toString();
            }
            return tokenStrings;
        } catch (DataAccessException e) {
            throw new SRMInternalErrorException("Database failure", e);
        }
    }

    private SrmGetRequestTokensResponse srmGetRequestTokens()
            throws SRMException
    {
        String description = request.getUserRequestDescription();
        String[] requestTokens = getRequestTokens(user, description);
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
