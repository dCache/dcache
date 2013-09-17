//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 06/27 by Neha Sharma (neha@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmCheckPermission
 *
 * Created on 06/27
 */

package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTSURLPermissionReturn;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TStatusCode;
/**
 *
 * @author neha
 */

public class SrmCheckPermission {
        private static Logger logger =
            LoggerFactory.getLogger(SrmCheckPermission.class);
	AbstractStorageElement storage;
	SrmCheckPermissionRequest request;
	SrmCheckPermissionResponse response;
	SRMUser user;

	public SrmCheckPermission(SRMUser user,
				  RequestCredential credential,
				  SrmCheckPermissionRequest request,
				  AbstractStorageElement storage,
				  SRM srm,
				  String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}

	public SrmCheckPermissionResponse getResponse() {
		if(response != null ) {
                    return response;
                }
		try {
			response = srmCheckPermission();
		}
		catch(Exception e) {
			logger.error(e.toString());
		}
		return response;
	}

	public static final SrmCheckPermissionResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}

	public static final SrmCheckPermissionResponse getFailedResponse(String error,TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
		TReturnStatus status = new TReturnStatus(statusCode, error);
		SrmCheckPermissionResponse response = new SrmCheckPermissionResponse();
		response.setReturnStatus(status);
		return response;
	}


	/**
	 * implementation of srm check permission
	 */

	public SrmCheckPermissionResponse srmCheckPermission()
		throws SRMException, URISyntaxException
        {
		if(request==null) {
			return getFailedResponse(" null request passed to SrmCheckPermission()");
		}
		ArrayOfAnyURI anyuriarray=request.getArrayOfSURLs();
		String authorizationID=request.getAuthorizationID();
		org.apache.axis.types.URI[] uriarray=anyuriarray.getUrlArray();
		int length=uriarray.length;
		if (length==0) {
			return getFailedResponse(" zero length array of URLS");
		}
		ArrayOfTSURLPermissionReturn arrayOfPermissions=new ArrayOfTSURLPermissionReturn();
		TSURLPermissionReturn surlPermissionArray[] = new TSURLPermissionReturn[length];
		arrayOfPermissions.setSurlPermissionArray(surlPermissionArray);
		int nfailed = 0;
		for(int i=0;i <length;i++){
                        TSURLPermissionReturn pr = new TSURLPermissionReturn();
			pr.setStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
			pr.setSurl(uriarray[i]);
			logger.debug("SURL[{}]= {}", i, uriarray[i]);
			URI surl = new URI(uriarray[i].toString());
			try {
                            FileMetaData fmd = storage.getFileMetaData(user,surl,false);
				int permissions = fmd.permMode;
				TPermissionMode pm;
				if (fmd.isOwner(user)) {
					pm = PermissionMaskToTPermissionMode.maskToTPermissionMode(((permissions>>6)&0x7));
				}
				else if (fmd.isGroupMember(user)) {
					pm = PermissionMaskToTPermissionMode.maskToTPermissionMode(((permissions>>3)&0x7));
				}
				else {
					pm = PermissionMaskToTPermissionMode.maskToTPermissionMode((permissions&0x7));
				}
				pr.setPermission(pm);
			}
			catch (SRMException srme) {
				logger.warn(srme.toString());
				pr.setStatus(new TReturnStatus(TStatusCode.SRM_FAILURE, uriarray[i] + " " + srme
                                        .getMessage()));
				nfailed++;
			}
			finally {
				arrayOfPermissions.setSurlPermissionArray(i,pr);
			}
		}
                TReturnStatus returnStatus;
                if (nfailed == 0) {
                    returnStatus = new TReturnStatus(TStatusCode.SRM_SUCCESS, "success");
                } else if (nfailed == length) {
                    returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE,
                            "failed to check Permission for all requested surls");
                } else {
                    returnStatus = new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS,
                            "failed to check Permission for at least one file");
                }
                return new SrmCheckPermissionResponse(returnStatus, arrayOfPermissions);
	}
}
