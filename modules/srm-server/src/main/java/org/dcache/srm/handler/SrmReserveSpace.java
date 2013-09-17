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
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmReserveSpaceRequest;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  litvinse
 */

public class SrmReserveSpace {
    private static Logger logger =
            LoggerFactory.getLogger(SrmReserveSpace.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmReserveSpaceRequest  request;
    SrmReserveSpaceResponse response;
    SRMUser             user;
    RequestCredential       credential;
    Configuration           configuration;
    private String client_host;

    public SrmReserveSpace(SRMUser user,
            RequestCredential credential,
            SrmReserveSpaceRequest request,
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
        this.client_host = client_host;
    }

    public SrmReserveSpaceResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = reserveSpace();
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

    public static final SrmReserveSpaceResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmReserveSpaceResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }

        SrmReserveSpaceResponse response = new SrmReserveSpaceResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
    /**
     * implementation of srm reserve space
     */

    public SrmReserveSpaceResponse reserveSpace()
        throws SRMException,MalformedURIException {
        if(request==null) {
            return getFailedResponse("srmReserveSpace: null request passed to SrmReserveSpace",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        long sizeInBytes = request.getDesiredSizeOfGuaranteedSpace().longValue();
        TRetentionPolicy retentionPolicy;
        TAccessLatency accessLatency;
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
             if (request.getDesiredLifetimeOfReservedSpace() ==-1 ||
                     request.getDesiredLifetimeOfReservedSpace() >0 ) {
                lifetimeInSeconds = request.getDesiredLifetimeOfReservedSpace();
             } else {
                    return getFailedResponse("srmReserveSpace: "+
                        "DesiredLifetimeOfReservedSpace() == "+
                            request.getDesiredLifetimeOfReservedSpace(),
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
                user,
                requestLifetime*1000L,
                3,
                sizeInBytes,
                lifetimeInSeconds == -1? -1L: lifetimeInSeconds*1000L,
                retentionPolicy,
                accessLatency,
                description,
               client_host);
           reserveRequest.applyJdc();
            reserveRequest.schedule();
         return reserveRequest.getSrmReserveSpaceResponse();
       }
       catch (Exception e) {
           logger.warn(e.toString());
           return getFailedResponse(e.toString(),
                   TStatusCode.SRM_INTERNAL_ERROR);
       }
   }


}
