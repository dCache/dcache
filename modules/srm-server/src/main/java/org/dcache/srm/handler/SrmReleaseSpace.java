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
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmReleaseSpaceRequest;
import org.dcache.srm.v2_2.SrmReleaseSpaceResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  litvinse
 */

public class SrmReleaseSpace {
    private static Logger logger =
            LoggerFactory.getLogger(SrmReleaseSpace.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement  storage;
    SrmReleaseSpaceRequest  request;
    SrmReleaseSpaceResponse response;
    SRMUser             user;
    Scheduler               scheduler;
    RequestCredential       credential;
    Configuration           configuration;

    public SrmReleaseSpace(SRMUser user,
            RequestCredential credential,
            SrmReleaseSpaceRequest request,
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
        this.scheduler = srm.getCopyRequestScheduler();
        if (scheduler == null) {
            throw new NullPointerException("scheduler is null");
        }
        this.configuration = srm.getConfiguration();
        if (configuration == null) {
            throw new NullPointerException("configuration is null");
        }
    }

    public SrmReleaseSpaceResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = releaseSpace();
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

    private static SrmReleaseSpaceResponse getFailedResponse(String text) {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    private static SrmReleaseSpaceResponse getFailedResponse(String text, TStatusCode statusCode) {
        SrmReleaseSpaceResponse response = new SrmReleaseSpaceResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }

    public SrmReleaseSpaceResponse releaseSpace()
        throws SRMException,MalformedURIException {
        if(request==null) {
            return getFailedResponse("srmReleaseSpace: null request passed to SrmReleaseSpace",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        String token = request.getSpaceToken();
        SrmReleaseSpaceCallbacks callbacks = new SrmReleaseSpaceCallbacks();
        storage.srmReleaseSpace(user,token,null, callbacks );
        TReturnStatus status = callbacks.waitResult(60 * 1000);//one minute max
        return new SrmReleaseSpaceResponse(status);
   }

   private class  SrmReleaseSpaceCallbacks implements org.dcache.srm.SrmReleaseSpaceCallbacks {
         private boolean completed;
         private TReturnStatus status;

           public SrmReleaseSpaceCallbacks() {

           }

        public synchronized TReturnStatus waitResult(long timeout)
        {
           //System.out.println("PutCallbacks waitResult() starting for CopyFileRequest "+fileId);
           long start = System.currentTimeMillis();
           long current = start;
           while(true)
           {
               if(completed)
               {
                    //System.out.println("PutCallbacks waitResult() completed with success="+success+
                    //" for CopyFileRequest "+fileId);
                   return status;
               }
               long wait = timeout - (current -start);
               if(wait > 0)
               {
                   try
                   {
                       this.wait(wait);
                   }
                   catch(InterruptedException ie){

                   }
               }
               else
               {
                   return new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, "release takes longer then "+timeout +" millis");
               }
               current = System.currentTimeMillis();

           }
        }

        public synchronized void complete()
        {
            this.completed = true;
            this.notifyAll();
        }

           @Override
           public void ReleaseSpaceFailed(String reason){
                status = new TReturnStatus(TStatusCode.SRM_FAILURE, reason);
                complete();
            }

            @Override
            public void SpaceReleased(String spaceReservationToken,long remainingSpaceSize){
                status = new TReturnStatus(TStatusCode.SRM_SUCCESS, "Space released");
                complete();

            }

            @Override
            public void ReleaseSpaceFailed(Exception e){
                status = new TReturnStatus(TStatusCode.SRM_FAILURE, e.toString());
                complete();
            }

        }

   @Override
   public String toString(){
       return "SrmReleaseSpace("+request.getSpaceToken()+")";
   }
}
