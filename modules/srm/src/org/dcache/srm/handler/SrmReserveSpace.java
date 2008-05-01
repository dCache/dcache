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
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.request.sql.ReserveSpaceRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Scheduler;
import org.apache.axis.types.URI;
import org.dcache.srm.SRMProtocol;

/**
 *
 * @author  litvinse
 */

public class SrmReserveSpace {
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmReserveSpaceRequest  request;
    SrmReserveSpaceResponse response;
    RequestUser             user;
    Scheduler               scheduler;
    RequestCredential       credential;
    Configuration           configuration;
    ReserveSpaceRequestStorage reserverSpaceRequestStorage;
    private String client_host;
    
    public SrmReserveSpace(RequestUser user,
            RequestCredential credential,
            SrmReserveSpaceRequest request,
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
        this.scheduler = srm.getReserveSpaceScheduler();
        if (scheduler == null) {
            throw new NullPointerException("scheduler is null");
        }
        this.configuration = srm.getConfiguration();
        if (configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        this.client_host = client_host;
        reserverSpaceRequestStorage = srm.getReserveSpaceRequestStorage();
    }
    
    
    private void say(String txt) {
        if(storage!=null) {
            storage.log("SrmReserveSpace "+txt);
        }
    }
    
    private void esay(String txt) {
        if(storage!=null) {
            storage.elog("SrmReserveSpace "+txt);
        }
    }
    
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmReserveSpace exception : ");
            storage.elog(t);
        }
    }
    
    public SrmReserveSpaceResponse getResponse() {
        if(response != null ) return response;
        try {
            response = reserveSpace();
        } catch(Exception e) {
            storage.elog(e);
            response = getFailedResponse("Exception : "+e.toString());
        }
        return response;
    }
    
    public static final SrmReserveSpaceResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }
    
    public static final SrmReserveSpaceResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }
        
        SrmReserveSpaceResponse response = new SrmReserveSpaceResponse();
        TReturnStatus returnStatus    = new TReturnStatus();
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(text);
        response.setReturnStatus(status);
        return response;
    }
    /**
     * implementation of srm reserve space
     */
    
    public SrmReserveSpaceResponse reserveSpace() 
        throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        if(request==null) {
            return getFailedResponse("srmReserveSpace: null request passed to SrmReserveSpace",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        long sizeInBytes = request.getDesiredSizeOfGuaranteedSpace().longValue();
        TRetentionPolicy retentionPolicy = null;
        TAccessLatency accessLatency = null;
        TRetentionPolicyInfo retentionPolicyInfo = request.getRetentionPolicyInfo();
        if(retentionPolicyInfo == null) {
            return getFailedResponse("srmReserveSpace: retentionPolicyInfo == null",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        retentionPolicy = retentionPolicyInfo.getRetentionPolicy();
        if(retentionPolicy == null) {
            return getFailedResponse("srmReserveSpace: retentionPolicy == null",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        
        accessLatency = retentionPolicyInfo.getAccessLatency();
        String description = request.getUserSpaceTokenDescription();
        long lifetimeInSeconds;
        if ( request.getDesiredLifetimeOfReservedSpace() != null ) { 
             if ( request.getDesiredLifetimeOfReservedSpace().intValue() ==-1 || 
                 request.getDesiredLifetimeOfReservedSpace().intValue() >0 ) {
                lifetimeInSeconds = request.getDesiredLifetimeOfReservedSpace().intValue();
             } else {
                    return getFailedResponse("srmReserveSpace: "+
                        "DesiredLifetimeOfReservedSpace() == "+
                        request.getDesiredLifetimeOfReservedSpace().intValue(),
                        TStatusCode.SRM_INVALID_REQUEST);
             }
        } else {
            lifetimeInSeconds = configuration.getDefaultSpaceLifetime();
        }
        //make reserve request lifetime no longer than 24 hours
        //request lifetime is different from space reservation lifetime
        long requestLifetime;
        if(lifetimeInSeconds == -1 || lifetimeInSeconds >24*60*60  ) {
            requestLifetime = 24*60*60;
        } else {
            requestLifetime = lifetimeInSeconds;
        }
       try {
           ReserveSpaceRequest reserveRequest  = 
               new ReserveSpaceRequest(
                credential.getId(),
                user.getId(),
                configuration,
                requestLifetime*1000L,
                reserverSpaceRequestStorage,
                3,
                sizeInBytes,
                lifetimeInSeconds == -1? -1L: lifetimeInSeconds*1000L,
                retentionPolicy,
                accessLatency,
                description,
               client_host);
           
            reserverSpaceRequestStorage.saveJob(reserveRequest,true);
            reserveRequest.schedule(scheduler);
         return reserveRequest.getSrmReserveSpaceResponse();
       }
       catch (Exception e) {
           esay(e);
           return getFailedResponse(e.toString(),
                   TStatusCode.SRM_INTERNAL_ERROR);
       }
   }
    
    
}
