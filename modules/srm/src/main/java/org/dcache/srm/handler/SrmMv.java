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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author  litvinse
 */

public class SrmMv {
        private static Logger logger =
                LoggerFactory.getLogger(SrmMv.class);
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
		if(response != null ) {
                    return response;
                }
		try {
			response = srmMv();
        } catch(URISyntaxException e) {
            logger.debug(" malformed uri : "+e.getMessage());
            response = getFailedResponse(" malformed uri : "+e.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme.toString());
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

	public SrmMvResponse srmMv()
                throws SRMException, URISyntaxException
        {
		SrmMvResponse response      = new SrmMvResponse();
		TReturnStatus returnStatus  = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
			return getFailedResponse(" null request passed to SrmRm()");
		}
		URI to_surl = new URI(request.getToSURL().toString());
		URI from_surl = new URI(request.getFromSURL().toString());
		if (to_surl==null || from_surl==null) {
			return getFailedResponse(" target or destination are not defined");
		}
		try {
                        storage.moveEntry(user, from_surl, to_surl);
                }
		catch (Exception e) {
		    logger.warn(e.toString());
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
