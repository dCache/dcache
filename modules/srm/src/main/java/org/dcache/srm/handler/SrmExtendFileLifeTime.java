//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmExtendFileLifeTime
 *
 * Created on 10/05
 */

package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.ArrayOfTSURLLifetimeReturnStatus;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLLifetimeReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  litvinse
 */

public class SrmExtendFileLifeTime {
    private static Logger logger =
            LoggerFactory.getLogger(SrmExtendFileLifeTime.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmExtendFileLifeTimeRequest           request;
    SrmExtendFileLifeTimeResponse          response;
    SRMUser            user;
    Configuration configuration;

    public SrmExtendFileLifeTime(SRMUser user,
            RequestCredential credential,
            SrmExtendFileLifeTimeRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String client_host ) {
        this.request = request;
        this.user = user;
        this.storage = storage;
        this.configuration = srm.getConfiguration();
    }

    public SrmExtendFileLifeTimeResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmExtendFileLifeTime();
        } catch(URISyntaxException e) {
            logger.debug(" malformed uri : "+e.getMessage());
            response = getFailedResponse(" malformed uri : "+e.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMInvalidRequestException e) {
            logger.debug(e.toString());
            response = getFailedResponse(e.toString(), TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme.toString());
            response = getFailedResponse(srme.toString());
        }
        return response;
    }

    public final SrmExtendFileLifeTimeResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public final  SrmExtendFileLifeTimeResponse
            getFailedResponse(String error,TStatusCode statusCode) {
        logger.debug("getFailedResponse("+error+","+statusCode+")");
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmExtendFileLifeTimeResponse response = new SrmExtendFileLifeTimeResponse();
        response.setReturnStatus(status);
        return response;
    }


    org.apache.axis.types.URI surls[];
    Integer newFileLifetime ;
    Integer newPinLifetime ;
    String token ;
    long requestId;
    Job job;
    /**
     * implementation of srm expend file life time
     */
    public SrmExtendFileLifeTimeResponse srmExtendSURLLifeTime()
        throws URISyntaxException
    {
        int failuresCount = 0;
        int len = surls.length;
        TSURLLifetimeReturnStatus surlStatus[] =
                new TSURLLifetimeReturnStatus[len];
        for(int i = 0; i<len; ++i) {

            surlStatus[i] = new TSURLLifetimeReturnStatus();
            surlStatus[i].setSurl(surls[i]);

            URI surl = new URI(surls[i].toString());
            try{
                int lifetimeLeft =
                    storage.srmExtendSurlLifetime(user,surl,newFileLifetime);
                surlStatus[i].setFileLifetime(lifetimeLeft);
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_SUCCESS,"ok"));
            } catch(SRMException e) {
                failuresCount++;
                String error = "surl="+surls[i] +"lifetime can't extended:"+e;
                if(logger.isDebugEnabled()) {
                    logger.debug(error, e);
                } else {
                    logger.warn(error);
                }
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_FAILURE,e.toString()));
            }
        }
        SrmExtendFileLifeTimeResponse response = new SrmExtendFileLifeTimeResponse();
        response.setArrayOfFileStatuses(
                new ArrayOfTSURLLifetimeReturnStatus(surlStatus));
        TReturnStatus returnStatus  = new TReturnStatus();
        returnStatus.setStatusCode(
                failuresCount==0 ?
                    TStatusCode.SRM_SUCCESS:
                    failuresCount == len ?
                        TStatusCode.SRM_FAILURE:
                        TStatusCode.SRM_PARTIAL_SUCCESS);
        response.setReturnStatus(returnStatus);
        response.getReturnStatus().setExplanation("success");
        return response;
    }

    public SrmExtendFileLifeTimeResponse srmExtendTURLorPinLifeTime()
            throws SRMInvalidRequestException {
        try {
            requestId = Long.parseLong(token);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    token+"\"is not valid",
                    TStatusCode.SRM_INVALID_REQUEST);
        }

        ContainerRequest<?> containerRequest = Job.getJob(requestId,
                ContainerRequest.class);
        containerRequest.applyJdc();

        long newLifetimeInMillis;
        long configMaximumLifetime;
        if(containerRequest instanceof CopyRequest){
            configMaximumLifetime = configuration.getCopyLifetime();
        } else if (containerRequest instanceof PutRequest) {
            configMaximumLifetime = configuration.getPutLifetime();
        } else if (containerRequest instanceof BringOnlineRequest) {
            configMaximumLifetime = configuration.getBringOnlineLifetime();
        } else {
            configMaximumLifetime = configuration.getGetLifetime();
        }
        newLifetimeInMillis =
            newPinLifetime>0
            ?newPinLifetime*1000>configMaximumLifetime
                ?configMaximumLifetime
                :newPinLifetime*1000
            :configMaximumLifetime;
        int failuresCount = 0;
        int len = surls.length;
        TSURLLifetimeReturnStatus surlStatus[] =
                new TSURLLifetimeReturnStatus[len];
        for(int i = 0; i<len; ++i) {
            surlStatus[i] = new TSURLLifetimeReturnStatus();
            surlStatus[i].setSurl(surls[i]);
            FileRequest<?> fileRequest;

            try {
                fileRequest =
                    containerRequest.getFileRequestBySurl(URI.create(surls[i].toString()));
            } catch (Exception e) {
                String error = "request for requestToken \""+
                    token+"\" for surl="+surls[i] +"can't be found:"+e;
                logger.warn(error,e);
                failuresCount++;
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_INVALID_PATH,error));
                continue;
            }
            if(fileRequest == null) {
                String err = "fileRequest for surl="+surls[i] +
                        "is not found in request with id = "+token;
                logger.warn(err);
                failuresCount++;
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_INVALID_PATH,err));
                continue;

            }

            try{
                long lifetimeLeftMillis =
                        fileRequest.extendLifetime(newLifetimeInMillis);
                int lifetimeLeftSec = lifetimeLeftMillis>=0
                    ?(int)(lifetimeLeftMillis/1000)
                    :(int)lifetimeLeftMillis;
                surlStatus[i].setFileLifetime(lifetimeLeftSec);
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_SUCCESS,"ok"));
            } catch(SRMReleasedException e) {
                String error = "request for requestToken \""+
                    token+"\" for surl="+surls[i] +"can't be extended:"+e;
                logger.warn(error);
                failuresCount++;
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_RELEASED,error));
            } catch(SRMInvalidRequestException e) {
                String error = "request for requestToken \""+
                    token+"\" for surl="+surls[i] +"can't be extended:"+e;
                logger.warn(error);
                failuresCount++;
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_INVALID_REQUEST,error));
            } catch(SRMAbortedException e) {
                failuresCount++;
                String error = "request for requestToken \""+
                    token+"\" for surl="+surls[i] +"can't be extended:"+e;
                logger.warn(error);
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_ABORTED,error));

            } catch(SRMException e) {
                failuresCount++;
                String error = "request for requestToken \""+
                    token+"\" for surl="+surls[i] +"can't be extended:"+e;
                logger.warn(error,e);
                surlStatus[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_FAILURE,error));
            }
        }

        SrmExtendFileLifeTimeResponse response =
                new SrmExtendFileLifeTimeResponse();
        response.setArrayOfFileStatuses(
                new ArrayOfTSURLLifetimeReturnStatus(surlStatus));
        TReturnStatus returnStatus  = new TReturnStatus();
        returnStatus.setStatusCode(
                failuresCount==0 ?
                    TStatusCode.SRM_SUCCESS:
                    failuresCount == len ?
                        TStatusCode.SRM_FAILURE:
                        TStatusCode.SRM_PARTIAL_SUCCESS);
        response.setReturnStatus(returnStatus);
        return response;

    }

    public SrmExtendFileLifeTimeResponse srmExtendFileLifeTime()
        throws SRMException, URISyntaxException
    {
        if(request==null) {
            return getFailedResponse("null request passed to srmExtendFileLifeTime()",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        surls   =request.getArrayOfSURLs().getUrlArray();
        if (surls==null ) {
            return getFailedResponse("surls array is not defined",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        newFileLifetime = request.getNewFileLifeTime();
        newPinLifetime = request.getNewPinLifeTime();
        token = request.getRequestToken();
        if(token == null) {
            if(newFileLifetime ==null) {
                return getFailedResponse(" both requestToken and newFileLifetime parameters are null",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            return srmExtendSURLLifeTime();
        } else {
            if(newPinLifetime == null) {
                return getFailedResponse(" requestToken is not null and newPinLifetime is null",
                        TStatusCode.SRM_INVALID_REQUEST);
            }

            return srmExtendTURLorPinLifeTime();
        }

    }

}
