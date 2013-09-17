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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  litvinse
 */

public class SrmMkdir {
        private static final Logger logger =
                LoggerFactory.getLogger(SrmMkdir.class.getName());
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement storage;
	SrmMkdirRequest        request;
	SrmMkdirResponse       response;
	SRMUser            user;

	public SrmMkdir(SRMUser user,
			RequestCredential credential,
			SrmMkdirRequest request,
			AbstractStorageElement storage,
			SRM srm,
			String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}

	public SrmMkdirResponse getResponse() {
		if(response != null ) {
                    return response;
                }
		try {
			response = srmMkdir();
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

	public static final SrmMkdirResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}

	public static final SrmMkdirResponse getFailedResponse(String error,TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
                SrmMkdirResponse response = new SrmMkdirResponse();
		response.setReturnStatus(new TReturnStatus(statusCode, error));
		return response;
	}


	/**
	 * implementation of srm mkdir
     */

	public SrmMkdirResponse srmMkdir()
            throws SRMException, URISyntaxException
        {
            if(request==null) {
                return getFailedResponse("null request passed to srmMkdir()");
            }
            TReturnStatus returnStatus;
            try {
                storage.createDirectory(user, new URI(request.getSURL().toString()));
                returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, "success");
            }
            catch (SRMDuplicationException e) {
                logger.debug("srmMkdir duplication: {}", e.toString());
                returnStatus = new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR,
                        request.getSURL() + " : " + e.getMessage());
            }
            catch (SRMAuthorizationException e) {
                logger.debug("srmMkdir authorization exception: {}", e.toString());
                returnStatus = new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE,
                        request.getSURL() + " : " + e.getMessage());

            }
            catch (SRMInvalidPathException e) {
                logger.debug("srmMkdir invalid pathh: {}", e.toString());
                returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH,
                        request.getSURL() + " : " + e.getMessage());

            }
            catch (SRMException srme) {
                logger.debug("srmMkdir error: {}", srme.toString());
                returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE,
                        request.getSURL() + " " + srme.getMessage());
            }
            return new SrmMkdirResponse(returnStatus);
	}
}
