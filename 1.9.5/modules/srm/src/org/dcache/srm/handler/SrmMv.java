//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmMv
 *
 * Created on 10/05
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmMvRequest;
import org.dcache.srm.v2_2.SrmMvResponse;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.apache.log4j.Logger;
import org.apache.axis.types.URI.MalformedURIException;

/**
 *
 * @author  litvinse
 */

public class SrmMv {
        private static Logger logger = 
                Logger.getLogger(SrmMv.class);
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement storage;
	SrmMvRequest           request;
	SrmMvResponse          response;
	SRMUser            user;
	
	public SrmMv(SRMUser user,
		     RequestCredential credential,
		     SrmMvRequest request,
		     AbstractStorageElement storage,
		     org.dcache.srm.SRM srm,
		     String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}
	
	public SrmMvResponse getResponse() {
		if(response != null ) return response;
		try {
			response = srmMv();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme);
            response = getFailedResponse(srme.toString());
        }
		return response;
	}
	
	public static final SrmMvResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}
	
	public static final  SrmMvResponse getFailedResponse(String error,TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
		TReturnStatus status = new TReturnStatus();
		status.setStatusCode(statusCode);
		status.setExplanation(error);
		SrmMvResponse response = new SrmMvResponse();
		response.setReturnStatus(status);
		return response;
	}
	
	/**
	 * implementation of srm mv
	 */
	
	public SrmMvResponse srmMv() throws SRMException,
            MalformedURIException {
		SrmMvResponse response      = new SrmMvResponse();
		TReturnStatus returnStatus  = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
			return getFailedResponse(" null request passed to SrmRm()");
		}
		org.apache.axis.types.URI to_surl   =request.getToSURL();
		org.apache.axis.types.URI from_surl = request.getFromSURL();
		if (to_surl==null || from_surl==null) {
			return getFailedResponse(" target or destination are not defined");
		}
		String to_path   = to_surl.getPath(true,true);
		String from_path = from_surl.getPath(true,true);
		int to_indx      = to_path.indexOf(SFN_STRING);
		int from_indx    = from_path.indexOf(SFN_STRING);
		if ( to_indx != -1 ) {
			to_path=to_path.substring(to_indx+SFN_STRING.length());
		}
		if ( from_indx != -1 ) { 
			from_path=from_path.substring(from_indx+SFN_STRING.length());
		} 
		try {
			storage.moveEntry(user,from_path,to_path);
                } 
		catch (Exception e) { 
		    logger.warn(e);
		    response.getReturnStatus().setExplanation(e.getMessage());
		    if ( e instanceof SRMDuplicationException) { 
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_DUPLICATION_ERROR);
		    }
		    else if ( e instanceof  SRMInternalErrorException) { 
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
		    }
		    else if ( e instanceof  SRMInvalidPathException ) { 
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_INVALID_PATH);
		    }
		    else if ( e instanceof SRMAuthorizationException ) { 
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
		    }
		    else if ( e instanceof SRMException ) { 
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_FAILURE);
		    }
		    else { 
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
		    }
		    return response;
		}
		response.getReturnStatus().setExplanation("success");
		return response;
	}
	
}
