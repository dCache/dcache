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

/**
 *
 * @author  timur
 */

public class SrmGetSpaceMetaData {
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
    
    
    private void say(String txt) {
        if(storage!=null) {
            storage.log("SrmGetSpaceMetaData "+txt);
        }
    }
    
    private void esay(String txt) {
        if(storage!=null) {
            storage.elog("SrmGetSpaceMetaData "+txt);
        }
    }
    
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmGetSpaceMetaData exception : ");
            storage.elog(t);
        }
    }
    
    public SrmGetSpaceMetaDataResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmGetSpaceMetaData();
        } catch(Exception e) {
            storage.elog(e);
            response = getFailedResponse("Exception : "+e.toString());
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
        TReturnStatus returnStatus    = new TReturnStatus();
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
        throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        try {
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
       catch (Exception e) {
           esay(e);
           return getFailedResponse(e.toString(),
                   TStatusCode.SRM_INTERNAL_ERROR);
       }
   }
    
    
}
