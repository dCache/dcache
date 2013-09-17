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
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.SrmGetSpaceTokensRequest;
import org.dcache.srm.v2_2.SrmGetSpaceTokensResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */

public class SrmGetSpaceTokens {
    private static Logger logger =
            LoggerFactory.getLogger(SrmGetSpaceTokens.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmGetSpaceTokensRequest  request;
    SrmGetSpaceTokensResponse response;
    SRMUser             user;
    RequestCredential       credential;
    Configuration           configuration;

    public SrmGetSpaceTokens(SRMUser user,
            RequestCredential credential,
            SrmGetSpaceTokensRequest request,
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

    public SrmGetSpaceTokensResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmGetSpaceTokens();
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

    public static final SrmGetSpaceTokensResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmGetSpaceTokensResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }

        SrmGetSpaceTokensResponse response = new SrmGetSpaceTokensResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
    /**
     * implementation of srm SrmGetSpaceMetaData
     */

    public SrmGetSpaceTokensResponse srmGetSpaceTokens()
        throws SRMException,MalformedURIException {
        if(request==null) {
            return getFailedResponse(
                    "srmGetSpaceTokens: null request passed to SrmGetSpaceTokens",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        String description = request.getUserSpaceTokenDescription();
        String[] spaceTokens = storage.srmGetSpaceTokens(user,description);

        if(spaceTokens == null || spaceTokens.length == 0) {
            return getFailedResponse(
                    "the space token description provided does not refer to any existing space",
                    TStatusCode.SRM_INVALID_REQUEST);

        }
        SrmGetSpaceTokensResponse response =
                new SrmGetSpaceTokensResponse(
                new TReturnStatus(TStatusCode.SRM_SUCCESS,"OK"),
                new ArrayOfString(spaceTokens));

        return response;
   }


}
