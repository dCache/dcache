//______________________________________________________________________________
//
// $Id: SrmGetPermission.java,v 1.4.2.1 2006-12-21 18:10:15 litvinse Exp $
// $Author: litvinse $
//
// created 06/21 by Neha Sharma (neha@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmGetPermission
 *
 * Created on 06/21
 */

package org.dcache.srm.handler;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTPermissionReturn;
import org.dcache.srm.v2_2.TPermissionReturn;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.apache.axis.types.URI;

/**
 *
 * @author  litvinse
 */

public class SrmGetPermission {
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmGetPermissionRequest request;
    SrmGetPermissionResponse response;
    RequestUser user;
    
    public SrmGetPermission(RequestUser user,
            RequestCredential credential,
            SrmGetPermissionRequest request,
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
            storage.elog(" SrmGetPermission exception: ");
            storage.elog(t);
        }
    }
    
    public SrmGetPermissionResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmGetPermission();
        } catch(Exception e) {
            storage.elog(e);
        }
        return response;
    }
    
    public static final SrmGetPermissionResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmGetPermissionResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmGetPermissionResponse response = new SrmGetPermissionResponse();
        response.setReturnStatus(status);
        return response;
    }
    
    
    /**
     * implementation of srm get permission
     */
    
    public SrmGetPermissionResponse srmGetPermission() throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        SrmGetPermissionResponse response  = new SrmGetPermissionResponse();
        TReturnStatus returnStatus = new TReturnStatus();
        returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
        response.setReturnStatus(returnStatus);
        
        if(request==null) {
            return getFailedResponse(" null request passed to SrmGetPermission()");
        }
        
        ArrayOfAnyURI anyuriarray= request.getArrayOfSURLs();
        URI[] uriarray=anyuriarray.getUrlArray();
        
        ArrayOfTPermissionReturn permissionarray=new ArrayOfTPermissionReturn();
        TPermissionReturn permission=new TPermissionReturn();;
        
        int length=uriarray.length;
        String[] path=new String[length];
        
        //should be a for loop here.
        for(int i=0;i <length;i++){
            say("SURL["+i+"]= "+uriarray[i]);
            path[i] = uriarray[i].getPath(true,true);
            int indx    = path[i].indexOf(SFN_STRING);
            if(indx != -1) {
                path[i]=path[i].substring(indx+SFN_STRING.length());
	    }
	    try {
                    FileMetaData fmd=storage.getFileMetaData(user,path[i]);
                    String owner=fmd.owner;
                    permission.setOwner(owner);
                    permissionarray.setPermissionArray(i,permission);
                    //response.setArrayOfPermissionReturns(permissionarray);
	    }
	    catch (SRMException srme) {
                    esay(srme);
                    response.setArrayOfPermissionReturns(permissionarray);
                    response.getReturnStatus().setStatusCode(TStatusCode.SRM_FAILURE);
                    response.getReturnStatus().setExplanation(uriarray[i]+" "+srme.getMessage());
                    
                    return response;
	    }
	}
        response.setArrayOfPermissionReturns(permissionarray);
        response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
        response.getReturnStatus().setExplanation("success");
        return response;
    }
}
