//______________________________________________________________________________
//
// $Id$
// $Author$
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
    private final static String SFN_STRING = "?SFN=";
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
        return getResponse(error,TStatusCode.SRM_FAILURE);
    }
    
    public static final  SrmRmResponse getResponse(String error,
            TStatusCode statusCode) {
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
    
    public SrmRmResponse srmRm() throws SRMException,
            org.apache.axis.types.URI.MalformedURIException {
        
        if(request==null) {
            return getResponse(" null request passed to SrmRm()",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        
        if (request.getArrayOfSURLs()==null) {
            return getResponse("null array of Surls",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        
        URI[] surls       = request.getArrayOfSURLs().getUrlArray();
        if (surls == null || surls.length==0) {
            return getResponse("empty array of Surl Infos",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        
        TSURLReturnStatus[] surlReturnStatusArray =
                new TSURLReturnStatus[surls.length];
        boolean any_failed=false;
        String error = "";
        RemoveFile callbacks[] = new RemoveFile[surls.length];
        for (int i = 0; i < surls.length; i++) {
            surlReturnStatusArray[i] = new TSURLReturnStatus();
            surlReturnStatusArray[i].setSurl(surls[i]);
            say("SURL["+i+"]="+surls[i]);
            callbacks[i] = new RemoveFile();
        }
        
        for ( int i=0;i<callbacks.length;i++) {
            try {
                String path = surls[i]
                .getPath(true,true);
                int indx = path.indexOf(SFN_STRING);
                if ( indx != -1 ) {
                    path=path.substring(indx+SFN_STRING.length());
                }
                storage.removeFile(user,path,callbacks[i]);
                
            } catch(RuntimeException re) {
                esay(re);
               surlReturnStatusArray[i].setStatus( 
                       new TReturnStatus(
                        TStatusCode.SRM_INTERNAL_ERROR,
                        "RuntimeException "+re.getMessage()));
            } catch (Exception e) {
                esay(e);
               surlReturnStatusArray[i].setStatus( 
                       new TReturnStatus(
                        TStatusCode.SRM_INTERNAL_ERROR,
                        "Exception "+e));
                        
            }
        }
        
        try {
            for(int i = 0 ; i<callbacks.length; i++) {
                callbacks[i].waitToComplete();
                surlReturnStatusArray[i].setStatus(callbacks[i].getStatus());
                if (callbacks[i].getStatus().getStatusCode() !=
                            TStatusCode.SRM_SUCCESS) {
                   any_failed=true;
                   error=error+
                       surlReturnStatusArray[i].getStatus()
                                .getExplanation()+'\n';
                }
            }
        } catch(InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        
        SrmRmResponse srmRmResponse ;
        if ( any_failed ) {
            srmRmResponse  = getFailedResponse(
                "problem with one or more files: \n"+error);
        } else {
            srmRmResponse  = getResponse("successfully removed files",
                    TStatusCode.SRM_SUCCESS);
        }
                
        srmRmResponse.setArrayOfFileStatuses(
            new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        return srmRmResponse;
    }
    
    private class RemoveFile implements RemoveFileCallbacks {
        
        public TReturnStatus status;
        private boolean     done = false;
        private boolean success  = true;
        public RemoveFile( ) {
        }
        
        public TReturnStatus getStatus() {
            return status;
        }
        
        public void RemoveFileFailed(String reason) {
            
            status = new TReturnStatus(
                    TStatusCode.SRM_FAILURE,
                    reason);
            esay("RemoveFileFailed:"+reason);
            done();
        }

        public void FileNotFound(String reason) {
            
            status = new TReturnStatus(
                    TStatusCode.SRM_INVALID_PATH,
                    reason);
            esay("RemoveFileFailed:"+reason);
            done();
        }
        
        public void RemoveFileSucceeded(){
            status = new TReturnStatus(
                    TStatusCode.SRM_SUCCESS,
                    null);
            done();
        }
        
        public void Exception(Exception e){
            status = new TReturnStatus(
                    TStatusCode.SRM_FAILURE,
                    "Exception: "+e.getMessage());
            TReturnStatus individualFileReturnStatus
                    = new TReturnStatus();
            esay(e);
            done();
        }
        
        public void Timeout(){
            status = new TReturnStatus(
                    TStatusCode.SRM_FAILURE,
                    "Timeout: ");
            esay("Timeout");
            done();
        }
        
        public void waitToComplete() throws
                InterruptedException {
            long start_time = System.currentTimeMillis();
            synchronized(this) {
                while(true) {
                    if(done) {
                        return ;
                    }
                    wait(3600);
                    
                }
            }
        }
        
        public  synchronized void done() {
            done = true;
            notifyAll();
        }
        
    };
    
}
