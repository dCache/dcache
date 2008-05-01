//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 11/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmMkdir
 *
 * Created on 11/05
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInvalidPathException;

/**
 *
 * @author  litvinse
 */

public class SrmMkdir {
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement storage;
	SrmMkdirRequest        request;
	SrmMkdirResponse       response;
	RequestUser            user;
	
	public SrmMkdir(RequestUser user,
			RequestCredential credential,
			SrmMkdirRequest request,
			AbstractStorageElement storage,
			org.dcache.srm.SRM srm,
			String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}
	
	private void say(String txt) {
		if(storage!=null) {
			storage.log("SrmMkdir "+txt);
		}
	}
	
	private void esay(String txt) {
		if(storage!=null) {
			storage.elog("SrmMkdir "+txt);
		}
	}
	
	private void esay(Throwable t) {
		if(storage!=null) {
			storage.elog(" SrmMkdir exception : ");
			storage.elog(t);
		}
	}
	
	public SrmMkdirResponse getResponse() {
		if(response != null ) return response;
		try {
			response = srmRmdir();
		} 
		catch(Exception e) {
			storage.elog(e);
		}
		return response;
	}
	
	public static final SrmMkdirResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}
	
	public static final SrmMkdirResponse getFailedResponse(String error,TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
		TReturnStatus status = new TReturnStatus();
		status.setStatusCode(statusCode);
		status.setExplanation(error);
		SrmMkdirResponse response = new SrmMkdirResponse();
		response.setReturnStatus(status);
		return response;
	}
	
	
	/**
	 * implementation of srm mkdir
     */
	
	public SrmMkdirResponse srmRmdir() throws SRMException,org.apache.axis.types.URI.MalformedURIException {
		SrmMkdirResponse response  = new SrmMkdirResponse();
		TReturnStatus returnStatus = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
		return getFailedResponse(" null request passed to SrmRm()");
		}
		org.apache.axis.types.URI surl = request.getSURL();
		say("SURL[0]="+surl);
		int port    = surl.getPort();
		String host = surl.getHost();
		String path = surl.getPath(true,true);
		int indx    = path.indexOf(SFN_STRING);
		if ( indx != -1 ) {
			path=path.substring(indx+SFN_STRING.length());
		}
		try {
			storage.createDirectory(user,path);
		} 
                catch (SRMDuplicationException srmde) {
 			esay(srmde);
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_DUPLICATION_ERROR);
			response.getReturnStatus().setExplanation(surl+" : "+srmde.getMessage());
			return response;
                   
                }
                catch (SRMAuthorizationException srmae) {
 			esay(srmae);
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
			response.getReturnStatus().setExplanation(surl+" : "+srmae.getMessage());
			return response;
                   
                }
                catch (SRMInvalidPathException srmipe) {
 			esay(srmipe);
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_INVALID_PATH);
			response.getReturnStatus().setExplanation(surl+" : "+srmipe.getMessage());
			return response;
                   
                }
		catch (SRMException srme) {
			esay(srme);
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_FAILURE);
			response.getReturnStatus().setExplanation(surl+" "+srme.getMessage());
			return response;
		}
		response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
		response.getReturnStatus().setExplanation("success");
		return response;
	}
}
