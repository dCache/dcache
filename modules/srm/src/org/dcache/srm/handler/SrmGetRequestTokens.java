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
import org.dcache.srm.request.RequestUser;
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

public class SrmGetRequestTokens {
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmGetRequestTokensRequest  request;
    SrmGetRequestTokensResponse response;
    RequestUser             user;
    RequestCredential       credential;
    Configuration           configuration;
    
    public SrmGetRequestTokens(RequestUser user,
            RequestCredential credential,
            SrmGetRequestTokensRequest request,
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
            storage.log("SrmGetRequestTokens "+" "+txt);
        }
    }
    
    private void esay(String txt) {
        if(storage!=null) {
            storage.elog("SrmGetRequestTokens "+" "+txt);
        }
    }
    
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmGetRequestTokens exception : ");
            storage.elog(t);
        }
    }
    
    public SrmGetRequestTokensResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmGetRequestTokens();
        } catch(Exception e) {
            storage.elog(e);
            response = getFailedResponse("Exception : "+e.toString());
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
    
    public SrmGetRequestTokensResponse srmGetRequestTokens() 
        throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        try {
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
       catch (Exception e) {
           esay(e);
           return getFailedResponse(e.toString(),
                   TStatusCode.SRM_INTERNAL_ERROR);
       }
   }
    
    
}
