//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmCopy
 *
 * Created on 10/05
 */

package org.dcache.srm.handler;

import org.apache.axis.types.URI.MalformedURIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn;
import org.dcache.srm.v2_2.SrmGetRequestTokensRequest;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.TRequestTokenReturn;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  timur
 */

public class SrmGetRequestTokens {
    private static Logger logger =
            LoggerFactory.getLogger(SrmGetRequestTokens.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmGetRequestTokensRequest  request;
    SrmGetRequestTokensResponse response;
    SRMUser             user;
    RequestCredential       credential;
    Configuration           configuration;

    public SrmGetRequestTokens(SRMUser user,
            RequestCredential credential,
            SrmGetRequestTokensRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {

        if (request == null) {
            throw new NullPointerException("request is null");
        }
        this.request    = request;
        this.user       = user;
        this.credential = credential;
        if (storage == null) {
            throw new NullPointerException("storage is null");
        }
        this.storage = storage;
        this.configuration = srm.getConfiguration();
        if (configuration == null) {
            throw new NullPointerException("configuration is null");
        }
    }

    public SrmGetRequestTokensResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmGetRequestTokens();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme.toString());
            response = getFailedResponse(srme.toString());
        }
        return response;
    }

    public static final SrmGetRequestTokensResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmGetRequestTokensResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }

        SrmGetRequestTokensResponse response = new SrmGetRequestTokensResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
    /**
     * implementation of srm SrmGetSpaceMetaData
     */

    public SrmGetRequestTokensResponse srmGetRequestTokens()
        throws SRMException, MalformedURIException {
        if(request==null) {
            return getFailedResponse(
                    "srmGetRequestTokens: null request passed to SrmGetRequestTokens",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        String description = request.getUserRequestDescription();

        String[] requestTokens = storage.srmGetRequestTokens(user,description);
        if(requestTokens.length >0) {
            TRequestTokenReturn[] requestTokenReturns =
                    new TRequestTokenReturn[requestTokens.length];
            for(int i =0; i <requestTokens.length; ++i) {
                requestTokenReturns[i] =
                        new TRequestTokenReturn(requestTokens[i],null);
            }
            SrmGetRequestTokensResponse response =
                    new SrmGetRequestTokensResponse(
                    new TReturnStatus(TStatusCode.SRM_SUCCESS,"OK"),
                    new ArrayOfTRequestTokenReturn(requestTokenReturns));

            return response;
        } else {
               return getFailedResponse("userRequestDescription does not refer to any existing known requests",
               TStatusCode.SRM_INVALID_REQUEST);

        }

   }


}
