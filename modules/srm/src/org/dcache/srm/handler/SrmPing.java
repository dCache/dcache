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

import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.Configuration;
import org.apache.axis.types.URI;
import org.dcache.srm.request.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  litvinse
 */

public class SrmPing {
        private static Logger logger = 
                LoggerFactory.getLogger(SrmPing.class);
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement storage;
	SRMUser            user;
	RequestCredential      credential;
	SrmPingRequest request;
	SrmPingResponse        response;
	
	public SrmPing(SRMUser user,
		       RequestCredential credential,
		       SrmPingRequest request,
		       AbstractStorageElement storage,
		       org.dcache.srm.SRM srm,
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
	}
    
	public SrmPingResponse getResponse() {
		if(response != null ) return response;
		response = new SrmPingResponse();
		response.setVersionInfo("v2.2");
		response.setOtherInfo(new ArrayOfTExtraInfo(
					      new TExtraInfo[]{
						      new TExtraInfo("backend_type","dCache"),
						      new TExtraInfo("backend_version", storage.getStorageBackendVersion())}));
		return response;
	}
    
	public static final SrmPingResponse getFailedResponse(String text) {
		return getFailedResponse(text,null);
	}
    
	public static final SrmPingResponse getFailedResponse(String text, TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode = TStatusCode.SRM_FAILURE;
		}
		SrmPingResponse response = new SrmPingResponse();
		response.setVersionInfo("v2.2");
		response.setOtherInfo(new ArrayOfTExtraInfo(
					      new TExtraInfo[]{
						      new TExtraInfo("backend_type","dCache")}));
		return response;
	}
}
