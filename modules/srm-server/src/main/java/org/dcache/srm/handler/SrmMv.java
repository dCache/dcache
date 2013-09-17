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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmMvRequest;
import org.dcache.srm.v2_2.SrmMvResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

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
		     SRM srm,
		     String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}

	public SrmMvResponse getResponse() {
            if (response == null) {
                response = srmMv();
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
                SrmMvResponse response = new SrmMvResponse();
		response.setReturnStatus(new TReturnStatus(statusCode, error));
		return response;
	}

	/**
	 * implementation of srm mv
	 */

	public SrmMvResponse srmMv()
        {
		if (request==null) {
                    return getFailedResponse(" null request passed to srmMv()");
		}
                TReturnStatus returnStatus;
                try {
                    URI to_surl = new URI(request.getToSURL().toString());
                    URI from_surl = new URI(request.getFromSURL().toString());
                    // [SRM 2.2, 4.6.3]     SRM_INVALID_PATH: status of fromSURL is SRM_FILE_BUSY.
                    // [SRM 2.2, 4.6.2, c)] srmMv must fail on SURL that its status is SRM_FILE_BUSY,
                    //                      and SRM_FILE_BUSY must be returned.
                    // [SRM 2.2, 4.6.2, e)] When moving an SURL to already existing SURL,
                    //                      SRM_DUPLICATION_ERROR must be returned.
                    //
                    // The SRM spec is somewhat inconsistent on what the correct return code should be.
                    // Instead we use SRM_DUPLICATION_ERROR if the target SURL is busy (consistent with
                    // how this situation is handled in srmPrepareToPut) and SRM_FILE_BUSY if the source
                    // SURL is busy.
                    SRM srm = SRM.getSRM();
                    if (srm.isFileBusy(from_surl)) {
                        returnStatus = new TReturnStatus(TStatusCode.SRM_FILE_BUSY,
                                "The source SURL is being used by another client.");
                    } else if (srm.isFileBusy(to_surl)) {
                        returnStatus = new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR,
                                "The target SURL is being used by another client.");
                    } else {
                        storage.moveEntry(user, from_surl, to_surl);
                        returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, "success");
                    }
                } catch (URISyntaxException e) {
                    logger.debug("Malformed URI in move request: {}", e.getMessage());
                    returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_REQUEST, "Malformed uri: " + e.getMessage());
                } catch (SRMDuplicationException e) {
                    logger.warn(e.toString());
                    returnStatus = new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR, e.getMessage());
                } catch (SRMInternalErrorException e) {
                    logger.warn(e.toString());
                    returnStatus = new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
                } catch (SRMInvalidPathException e) {
                    logger.warn(e.toString());
                    returnStatus = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
                } catch (SRMAuthorizationException e) {
                    logger.warn(e.toString());
                    returnStatus = new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
                } catch (SRMException e) {
                    logger.warn(e.toString());
                    returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
                } catch (RuntimeException e) {
                    logger.warn("Please report this error to support@dcache.org,", e);
                    returnStatus = new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
		}
                return new SrmMvResponse(returnStatus);
	}

}
