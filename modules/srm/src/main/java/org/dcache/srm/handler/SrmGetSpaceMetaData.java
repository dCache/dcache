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

import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Scheduler;
import org.apache.axis.types.URI;
import org.dcache.srm.SRMProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.axis.types.URI.MalformedURIException;

/**
 *
 * @author  timur
 */

public class SrmGetSpaceMetaData {
    private static Logger logger = 
            LoggerFactory.getLogger(SrmGetSpaceMetaData.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmGetSpaceMetaDataRequest  request;
    SrmGetSpaceMetaDataResponse response;
    SRMUser             user;
    RequestCredential       credential;
    Configuration           configuration;
    
    public SrmGetSpaceMetaData(SRMUser user,
            RequestCredential credential,
            SrmGetSpaceMetaDataRequest request,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
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
    
    public SrmGetSpaceMetaDataResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmGetSpaceMetaData();
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
    
    public static final SrmGetSpaceMetaDataResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }
    
    public static final SrmGetSpaceMetaDataResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }
        
        SrmGetSpaceMetaDataResponse response = new SrmGetSpaceMetaDataResponse();
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(text);
        response.setReturnStatus(status);
        return response;
    }
    /**
     * implementation of srm SrmGetSpaceMetaData
     */
    
    public SrmGetSpaceMetaDataResponse srmGetSpaceMetaData() 
        throws SRMException,MalformedURIException {
        if(request==null) {
            return getFailedResponse(
                    "srmGetSpaceMetaData: null request passed to SrmGetSpaceMetaData",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        String[] spaceTokens = request.getArrayOfSpaceTokens().getStringArray();
        if(spaceTokens == null) {
            return getFailedResponse(
                    "srmGetSpaceMetaData: null array of tokens passed to SrmGetSpaceMetaData",
                    TStatusCode.SRM_INVALID_REQUEST);
        }

        TMetaDataSpace[] array = storage.srmGetSpaceMetaData(user,spaceTokens);
        SrmGetSpaceMetaDataResponse response =
                new SrmGetSpaceMetaDataResponse(
                new TReturnStatus(TStatusCode.SRM_SUCCESS,"OK"),
                new ArrayOfTMetaDataSpace(array));

        return response;
   }
    
    
}
