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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.ArrayOfTSupportedTransferProtocol;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSupportedTransferProtocol;

/**
 *
 * @author  litvinse
 */

public class SrmGetTransferProtocols {
   private static Logger logger =
           LoggerFactory.getLogger(SrmGetTransferProtocols.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SRMUser            user;
    Scheduler              scheduler;
    RequestCredential      credential;
    Configuration          configuration;
    SrmGetTransferProtocolsRequest request;
    SrmGetTransferProtocolsResponse        response;
    SRM srm;

    public SrmGetTransferProtocols(SRMUser user,
            RequestCredential credential,
            SrmGetTransferProtocolsRequest request,
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
        this.srm = srm;
        this.scheduler = srm.getCopyRequestScheduler();
        if (scheduler == null) {
            throw new NullPointerException("scheduler is null");
        }
        this.configuration = srm.getConfiguration();
        if (configuration == null) {
            throw new NullPointerException("configuration is null");
        }
    }

    public SrmGetTransferProtocolsResponse getResponse() {
        if(response != null ) {
            return response;
        }
        response = new SrmGetTransferProtocolsResponse();
        String[] protocols;
        try {

         protocols = srm.getProtocols(user,credential);
      } catch(Exception e) {
         logger.warn(e.toString());
         return getFailedResponse("SrmGetTransferProtocols failed: "+e,
                 TStatusCode.SRM_INTERNAL_ERROR);
      }

        TSupportedTransferProtocol[] arrayOfProtocols =
                new TSupportedTransferProtocol[protocols.length];
        for(int i =0 ; i<protocols.length; ++i) {
            arrayOfProtocols[i] = new TSupportedTransferProtocol(protocols[i],null);
        }
        ArrayOfTSupportedTransferProtocol protocolArray =
                new ArrayOfTSupportedTransferProtocol();

        protocolArray.setProtocolArray(arrayOfProtocols);
        response.setProtocolInfo(protocolArray);
        response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS,
                "success"));
        return response;
    }

    public static final SrmGetTransferProtocolsResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmGetTransferProtocolsResponse getFailedResponse(String error,
            TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }
        SrmGetTransferProtocolsResponse response = new SrmGetTransferProtocolsResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
