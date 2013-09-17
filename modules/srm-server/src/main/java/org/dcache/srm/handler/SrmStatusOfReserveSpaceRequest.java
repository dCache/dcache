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
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  litvinse
 */

public class SrmStatusOfReserveSpaceRequest {
    private static Logger logger =
            LoggerFactory.getLogger(SrmStatusOfReserveSpaceRequest.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmStatusOfReserveSpaceRequestRequest  request;
    SrmStatusOfReserveSpaceRequestResponse response;
    SRMUser             user;
    RequestCredential       credential;
    Configuration           configuration;

    public SrmStatusOfReserveSpaceRequest(SRMUser user,
            RequestCredential credential,
            SrmStatusOfReserveSpaceRequestRequest request,
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

    public SrmStatusOfReserveSpaceRequestResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = reserveSpaceStatus();
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

    public static final SrmStatusOfReserveSpaceRequestResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmStatusOfReserveSpaceRequestResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }

        SrmStatusOfReserveSpaceRequestResponse response = new SrmStatusOfReserveSpaceRequestResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
    /**
     * implementation of srm getStatusOfReserveSpaceRequest
     */

    public SrmStatusOfReserveSpaceRequestResponse reserveSpaceStatus()
        throws SRMException, MalformedURIException {
        try {
            if(request==null) {
                return getFailedResponse(
                        "srmStatusOfReserveSpaceRequest: null request passed to SrmStatusOfReserveSpaceRequest",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            String requestIdStr = request.getRequestToken();
            if(requestIdStr == null) {
                return getFailedResponse(
                        "srmStatusOfReserveSpaceRequest: null token passed to SrmStatusOfReserveSpaceRequest",
                        TStatusCode.SRM_INVALID_REQUEST);
            }

            Long requestId;
            try {
                requestId = new Long(requestIdStr);
            }
            catch (Exception e) {
                return getFailedResponse(
                        "srmStatusOfReserveSpaceRequest: invalid token="+
                        requestIdStr+"passed to SrmStatusOfReserveSpaceRequest",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            ReserveSpaceRequest request = ReserveSpaceRequest.getRequest(requestId);
            request.applyJdc();

            SrmStatusOfReserveSpaceRequestResponse resp = request.getSrmStatusOfReserveSpaceRequestResponse();
            return resp;
       }
       catch (Exception e) {
           return getFailedResponse(e.toString(),
                   TStatusCode.SRM_INTERNAL_ERROR);
       }
   }


}
