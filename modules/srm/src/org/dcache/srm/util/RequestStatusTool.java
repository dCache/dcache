/*
 * RequestStatusTool.java
 *
 * Created on December 6, 2005, 5:40 PM
 */

package org.dcache.srm.util;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.SRMException;

/**
 *
 * @author  timur
 */
public class RequestStatusTool {
    
    public static final void checkValidity(TReturnStatus returnStatus) 
    throws SRMException {
            if(returnStatus == null) {
                throw new SRMException(" null return status");
            }
            TStatusCode statusCode = returnStatus.getStatusCode();
            if(statusCode == null) {
                throw new SRMException(" null status code");
            }
     }
    
    public static final boolean isFailedRequestStatus(TReturnStatus returnStatus) 
    throws SRMException {
           if(returnStatus == null) {
                throw new SRMException(" null return status");
           }
           TStatusCode statusCode = returnStatus.getStatusCode();
           if(statusCode == null) {
                throw new SRMException(" null status code");
           }
           return
               statusCode != TStatusCode.SRM_PARTIAL_SUCCESS &&
               statusCode != TStatusCode.SRM_REQUEST_INPROGRESS &&
               statusCode != TStatusCode.SRM_REQUEST_QUEUED &&
               statusCode != TStatusCode.SRM_REQUEST_SUSPENDED &&
               statusCode != TStatusCode.SRM_SUCCESS  &&
               statusCode != TStatusCode.SRM_DONE;

    }
    
    public static final boolean isFailedFileRequestStatus(TReturnStatus returnStatus) 
    throws SRMException {
           if(returnStatus == null) {
                throw new SRMException(" null return status");
           }
           TStatusCode statusCode = returnStatus.getStatusCode();
           if(statusCode == null) {
                throw new SRMException(" null status code");
           }
           return 
               statusCode != TStatusCode.SRM_SPACE_AVAILABLE &&
               statusCode != TStatusCode.SRM_FILE_PINNED &&
               statusCode != TStatusCode.SRM_FILE_IN_CACHE &&
               statusCode != TStatusCode.SRM_FILE_PINNED &&
               statusCode != TStatusCode.SRM_SUCCESS &&
               statusCode != TStatusCode.SRM_REQUEST_INPROGRESS &&
               statusCode != TStatusCode.SRM_REQUEST_QUEUED &&
               statusCode != TStatusCode.SRM_REQUEST_SUSPENDED &&
               statusCode != TStatusCode.SRM_DONE;
    }
    

    public static final boolean isTransientStateStatus(TReturnStatus returnStatus) 
       throws SRMException {
           if(returnStatus == null) {
                throw new SRMException(" null return status");
           }
           TStatusCode statusCode = returnStatus.getStatusCode();
           if(statusCode == null) {
                throw new SRMException(" null status code");
           }
           return 
               statusCode == TStatusCode.SRM_REQUEST_QUEUED ||
                   statusCode == TStatusCode.SRM_REQUEST_INPROGRESS;
    }
    
    
}
