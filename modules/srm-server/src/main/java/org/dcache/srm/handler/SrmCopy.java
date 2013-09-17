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

import org.apache.axis.types.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMProtocol;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmCopyRequest;
import org.dcache.srm.v2_2.SrmCopyResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TCopyFileRequest;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  litvinse
 */

public class SrmCopy {
    private static Logger logger =
            LoggerFactory.getLogger(SrmCopy.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmCopyRequest         request;
    SrmCopyResponse        response;
    SRMUser            user;
    RequestCredential      credential;
    Configuration          configuration;
    private String client_host;

    public SrmCopy(SRMUser user,
            RequestCredential credential,
            SrmCopyRequest request,
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

    public SrmCopyResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmCopy();
        } catch(Exception e) {
            logger.error(e.toString());
            response = getFailedResponse("Exception : "+e.toString());
        }
        return response;
    }

    public static final SrmCopyResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmCopyResponse getFailedResponse(String text, TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode = TStatusCode.SRM_FAILURE;
        }
        SrmCopyResponse response = new SrmCopyResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
    /**
     * implementation of srm copy
     */

    public SrmCopyResponse srmCopy() throws SRMException,URI.MalformedURIException {
        if(request==null) {
            return getFailedResponse("SrmCopy: null request passed to SrmCopy",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        if(request.getArrayOfFileRequests() == null) {
            return getFailedResponse("SrmCopy: ArrayOfFileRequests is null",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        TCopyFileRequest[] arrayOfFileRequests  =
                request.getArrayOfFileRequests().getRequestArray();
        if (arrayOfFileRequests==null) {
            return getFailedResponse("SrmCopy: null array of file requests",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        if (arrayOfFileRequests.length==0) {
            return getFailedResponse("SrmCopy: empty array of file requests",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        String from_urls[] = new String[arrayOfFileRequests.length];
        String to_urls[]   = new String[arrayOfFileRequests.length];
        long lifetimeInSeconds = 0;
        if( request.getDesiredTotalRequestTime() != null ) {
            long reqLifetime = (long)request.getDesiredTotalRequestTime().intValue();
            if(  lifetimeInSeconds < reqLifetime) {
                lifetimeInSeconds = reqLifetime;
            }
        }

        long lifetime =
                lifetimeInSeconds>0
                ?lifetimeInSeconds*1000>configuration.getCopyLifetime()
                ?configuration.getCopyLifetime()
                :lifetimeInSeconds*1000
                :configuration.getCopyLifetime();
        String spaceToken = request.getTargetSpaceToken();
        for (int i=0; i<arrayOfFileRequests.length; i++) {
            URI fromSURL = arrayOfFileRequests[i].getSourceSURL();
            URI toSURL   = arrayOfFileRequests[i].getTargetSURL();
            from_urls[i]  = fromSURL.toString();
            to_urls[i]    = toSURL.toString();
        }
        TRetentionPolicy targetRetentionPolicy=null;
        TAccessLatency targetAccessLatency=null;
        if (request.getTargetFileRetentionPolicyInfo() !=null) {
            targetRetentionPolicy = request.getTargetFileRetentionPolicyInfo().getRetentionPolicy();
            targetAccessLatency = request.getTargetFileRetentionPolicyInfo().getAccessLatency();
        }
        TOverwriteMode overwriteMode = request.getOverwriteOption();
        if( overwriteMode != null &&
            overwriteMode.equals(TOverwriteMode.WHEN_FILES_ARE_DIFFERENT)) {
            getFailedResponse(
                    "Overwrite Mode WHEN_FILES_ARE_DIFFERENT is not supported",
                    TStatusCode.SRM_NOT_SUPPORTED);
        }

        try {
            CopyRequest r = new CopyRequest(
                    user,
                    credential.getId(),
                    from_urls,
                    to_urls,
                    spaceToken,
                    lifetime,
                    configuration.getCopyRetryTimeout(),
                    configuration.getCopyMaxNumOfRetries(),
                    SRMProtocol.V2_1,
                    request.getTargetFileStorageType(),
                    targetRetentionPolicy,
                    targetAccessLatency,
                    request.getUserRequestDescription(),
                    client_host,
                    overwriteMode);
            r.applyJdc();
            if (request.getSourceStorageSystemInfo()!=null) {
                if ( request.getSourceStorageSystemInfo().getExtraInfoArray()!=null) {
                    if (request.getSourceStorageSystemInfo().getExtraInfoArray().length>0) {
                        for (int i=0;i<request.getSourceStorageSystemInfo().getExtraInfoArray().length;i++) {
                            TExtraInfo extraInfo = request.getSourceStorageSystemInfo().getExtraInfoArray()[i];
                            if (extraInfo.getKey().equals("priority")) {
                                int priority = Integer.parseInt(extraInfo.getValue());
                                r.setPriority(priority);
                            }
                        }
                    }
                }
            }
            r.schedule();
            response = ((CopyRequest)r).getSrmCopyResponse();
            return response;
        } catch(Exception e) {
            logger.error(e.toString());
            return getFailedResponse("copy request generated error : "+e.toString(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        }
    }
}
