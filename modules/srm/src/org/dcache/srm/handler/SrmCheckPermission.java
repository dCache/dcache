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

import org.dcache.srm.FileMetaData;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TUserPermission;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.ArrayOfTPermissionReturn;
import org.dcache.srm.v2_2.TPermissionReturn;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.ArrayOfTSURLPermissionReturn;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.SRMException;
import java.util.Vector;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
				  org.dcache.srm.SRM srm,
				  String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}

	public SrmCheckPermissionResponse getResponse() {
		if(response != null ) return response;
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
		TReturnStatus status = new TReturnStatus();
		status.setStatusCode(statusCode);
		status.setExplanation(error);
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
		SrmCheckPermissionResponse response  = new SrmCheckPermissionResponse();
		TReturnStatus returnStatus           = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		returnStatus.setExplanation("success");
		response.setReturnStatus(returnStatus);
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
			TReturnStatus rs = new TReturnStatus();
			rs.setStatusCode(TStatusCode.SRM_SUCCESS);
			TSURLPermissionReturn pr = new TSURLPermissionReturn();
			pr.setStatus(rs);
			pr.setSurl(uriarray[i]);
			logger.debug("SURL["+i+"]= "+uriarray[i]);
			URI surl = new URI(uriarray[i].toString());
			try {
                            FileMetaData fmd = storage.getFileMetaData(user,surl,false);
				int permissions = fmd.permMode;
				TPermissionMode pm  = TPermissionMode.NONE;
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
				pr.getStatus().setStatusCode(TStatusCode.SRM_FAILURE);
				pr.getStatus().setExplanation(uriarray[i]+" "+srme.getMessage());
				nfailed++;
			}
			finally {
				arrayOfPermissions.setSurlPermissionArray(i,pr);
			}
		}
		response.setArrayOfPermissions(arrayOfPermissions);
		if ( nfailed!=0) {
			if ( nfailed == length ) {
				response.getReturnStatus().setStatusCode(TStatusCode.SRM_FAILURE);
				response.getReturnStatus().setExplanation("failed to check Permission for all requested surls");
			}
			else {
				response.getReturnStatus().setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				response.getReturnStatus().setExplanation("failed to check Permission for at least one file");
			}
			return response;
		}
		response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
		response.getReturnStatus().setExplanation("success");
		return response;
	}
}
