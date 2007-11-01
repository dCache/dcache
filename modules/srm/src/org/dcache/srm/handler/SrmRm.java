//______________________________________________________________________________
//
// $Id: SrmRm.java,v 1.14.2.1 2006-12-21 18:10:16 litvinse Exp $
// $Author: litvinse $
//
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmRm
 *
 * Created on 10/05
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmRmRequest;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.RemoveFileCallbacks;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.apache.axis.types.URI;
/**
 *
 * @author  litvinse
 */

public class SrmRm {
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmRmRequest           request;
    SrmRmResponse          response;
    RequestUser            user;
    
    public SrmRm(RequestUser user,
            RequestCredential credential,
            SrmRmRequest request,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host ) {
        this.request = request;
        this.user = user;
        this.storage = storage;
    }
    
    private void say(String txt) {
        if(storage!=null) {
            storage.log(txt);
        }
    }
    
    private void esay(String txt) {
        if(storage!=null) {
            storage.elog(txt);
        }
    }
    
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmRm exception : ");
            storage.elog(t);
        }
    }
    
    public SrmRmResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmRm();
        } catch(Exception e) {
            storage.elog(e);
            response = new SrmRmResponse();
            TReturnStatus returnStatus = new TReturnStatus();
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation(e.toString());
            response.setReturnStatus(returnStatus);
        }
        return response;
    }
    
    public static final SrmRmResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final  SrmRmResponse getFailedResponse(String error,TStatusCode statusCode) {
        //esay("getFailedResponse: SrmRm "+error);
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmRmResponse response = new SrmRmResponse();
        response.setReturnStatus(status);
        return response;
    }
    
    
    /**
     * implementation of srm rm
     */
    
    public SrmRmResponse srmRm() throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        SrmRmResponse srmRmResponse = new SrmRmResponse();
        TReturnStatus returnStatus  = new TReturnStatus();
        if(request==null) {
            return getFailedResponse(" null request passed to SrmRm()",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        
        if (request.getArrayOfSURLs()==null) {
            return getFailedResponse("null array of Surls",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        URI[] surls       = request.getArrayOfSURLs().getUrlArray();
        if (surls == null || surls.length==0) {
            return getFailedResponse("empty array of Surl Infos",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        TSURLReturnStatus[] surlReturnStatusArray = new TSURLReturnStatus[surls.length];
        boolean any_failed=false;
        String error = "";
        RemoveFile callbacks[] = new RemoveFile[surls.length];
        for (int i = 0; i < surls.length; i++) {
            URI surl = surls[i];
            say("SURL["+i+"]="+surl);
            int port    = surl.getPort();
            String host = surl.getHost();
            String path = surl.getPath(true,true);
            int indx    = path.indexOf(SFN_STRING);
	    callbacks[i] = new RemoveFile(user,surl);
        }
        for ( int i=0;i<callbacks.length;i++) {
            try {
                String path = callbacks[i].getStatus().getSurl().getPath(true,true);
                int indx = path.indexOf(SFN_STRING);
                if ( indx != -1 ) {
                    path=path.substring(indx+SFN_STRING.length());
                }
		storage.removeFile(user,path,callbacks[i]);
            } catch(RuntimeException re) {
                esay(re);
                callbacks[i].getStatus().getStatus().setStatusCode(TStatusCode.SRM_FAILURE);
                callbacks[i].getStatus().getStatus().setExplanation("RunTimeException "+re);
            } catch (Exception e) {
                callbacks[i].getStatus().getStatus().setStatusCode(TStatusCode.SRM_FAILURE);
                callbacks[i].getStatus().getStatus().setExplanation("Exception "+e);
            }
        }
        try {
            for(int i = 0 ; i<callbacks.length; i++) {
                if(!callbacks[i].waitToComplete(3*60*1000)) {
                    any_failed = true;
                    surlReturnStatusArray[i]=callbacks[i].getStatus();
                    error=error+ callbacks[i].getError()+'\n';
                } else {
                    surlReturnStatusArray[i]=callbacks[i].getStatus();
                    if (callbacks[i].getStatus().getStatus().getStatusCode()!=TStatusCode.SRM_SUCCESS) {
                        any_failed=true;
                        error=error+ surlReturnStatusArray[i].getStatus().getExplanation()+'\n';
                    }
                }
            }
        } catch(InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        
        
        if ( any_failed ) {
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation("problem with one or more files: \n"+error);
        } else {
            returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
            returnStatus.setExplanation("successfully removed files");
        }
        srmRmResponse.setReturnStatus(returnStatus);
        
        srmRmResponse.setArrayOfFileStatuses(
                new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        return srmRmResponse;
    }
    
    private class RemoveFile implements RemoveFileCallbacks {
        
        public TSURLReturnStatus status;
        private boolean     done = false;
        private boolean success  = true;
        RequestUser user;
        String path;
        String error;
        public RemoveFile(RequestUser user,
                org.apache.axis.types.URI surl ) {
            this.user   = user;
            this.status = new TSURLReturnStatus();
            this.status.setSurl(surl);
        }
        
        public TSURLReturnStatus getStatus() {
            return status;
        }
        
        public void RemoveFileFailed(String reason) {
            TReturnStatus individualFileReturnStatus  = new TReturnStatus();
            individualFileReturnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            error=status.getSurl()+" : "+reason;
            individualFileReturnStatus.setExplanation(error);
            status.setStatus(individualFileReturnStatus);
            success = false;
            esay(error);
            done();
        }
        
        public void RemoveFileSucceeded(){
            TReturnStatus individualFileReturnStatus  = new TReturnStatus();
            individualFileReturnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
            status.setStatus(individualFileReturnStatus);
            done();
        }
        
        public void Exception(Exception e){
            error="Exception: "+e;
            TReturnStatus individualFileReturnStatus  = new TReturnStatus();
            individualFileReturnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            individualFileReturnStatus.setExplanation(error);
            status.setStatus(individualFileReturnStatus);
            success = false;
            esay(error);
            done();
        }
        
        public void Timeout(){
            error = "Timeouut ";
            TReturnStatus individualFileReturnStatus  = new TReturnStatus();
            individualFileReturnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            individualFileReturnStatus.setExplanation(error);
            status.setStatus(individualFileReturnStatus);
            success = false;
            esay(error);
            done();
        }
        
        public  boolean waitToComplete(long timeout) throws InterruptedException {
            long start_time = System.currentTimeMillis();
            synchronized(this) {
                while(true) {
                    if(done) {
                        return success;
                    }
                    long current_time = System.currentTimeMillis();
                    if (timeout-current_time+start_time<0) {
                        TReturnStatus individualFileReturnStatus  = new TReturnStatus();
                        individualFileReturnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
                        individualFileReturnStatus.setExplanation("Timeout");
                        status.setStatus(individualFileReturnStatus);
                        error = " RemoveFile ("+user+","+status.getSurl()+") Timeout";
                        return false;
                    }
                    wait(timeout-current_time+start_time);
                    
                }
            }
        }
        
        public  synchronized void done() {
            done = true;
            notifyAll();
        }
        
        public java.lang.String getError() {
            return error;
        }
    };
    
}
