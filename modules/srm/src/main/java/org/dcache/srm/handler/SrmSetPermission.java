//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 06/21 by Neha Sharma (neha@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmSetPermission
 *
 * Created on 06/21
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
import org.dcache.srm.v2_2.ArrayOfTGroupPermission;
import org.dcache.srm.v2_2.ArrayOfTUserPermission;
import org.dcache.srm.v2_2.SrmSetPermissionRequest;
import org.dcache.srm.v2_2.SrmSetPermissionResponse;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;



/**
 *
 * @author  litvinse
 */

public class SrmSetPermission {
        private static Logger logger =
            LoggerFactory.getLogger(SrmSetPermission.class);
	AbstractStorageElement   storage;
	SrmSetPermissionRequest  request;
	SrmSetPermissionResponse response;
	SRMUser              user;

	public SrmSetPermission(SRMUser user,
				RequestCredential credential,
				SrmSetPermissionRequest request,
				AbstractStorageElement storage,
				SRM srm,
				String client_host ) {
		this.request = request;
		this.user    = user;
		this.storage = storage;
	}

	public SrmSetPermissionResponse getResponse() {
		if(response != null ) {
                    return response;
                }
		try {
			response = srmSetPermission();
                } catch(URISyntaxException e) {
                    logger.trace(" malformed uri : "+e.getMessage());
                    response = getFailedResponse(" malformed uri : "+e.getMessage(),
                            TStatusCode.SRM_INVALID_REQUEST);
                } catch(SRMException srme) {
                    logger.error(srme.toString());
                    response = getFailedResponse(srme.toString());
                }
		return response;
	}

	public static final SrmSetPermissionResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}

	public static final SrmSetPermissionResponse getFailedResponse(String error,TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
		TReturnStatus status = new TReturnStatus();
		status.setStatusCode(statusCode);
		status.setExplanation(error);
		SrmSetPermissionResponse response = new SrmSetPermissionResponse();
		response.setReturnStatus(status);
		return response;
	}


	/**
	 * implementation of srm set permission
	 */

	public SrmSetPermissionResponse srmSetPermission()
                throws SRMException, URISyntaxException
        {
		SrmSetPermissionResponse response  = new SrmSetPermissionResponse();
		TReturnStatus returnStatus = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
			return getFailedResponse(" null request passed to SrmSetPermission()");
		}
		URI surl = new URI(request.getSURL().toString());
		try {
                        FileMetaData fmd= storage.getFileMetaData(user,surl,false);
			String owner    = fmd.owner;
			int permissions = fmd.permMode;
			int groupid = Integer.parseInt(fmd.group);
			if (!fmd.isOwner(user)) {
				return getFailedResponse("user "+user+" does not own file "+request.getSURL()+" Can't set permission",TStatusCode.SRM_AUTHORIZATION_FAILURE);
			}
			TPermissionType permissionType = request.getPermissionType();
			TPermissionMode ownerPermission = request.getOwnerPermission();
			ArrayOfTUserPermission arrayOfUserPermissions = request.getArrayOfUserPermissions();
			ArrayOfTGroupPermission arrayOfGroupPermissions = request.getArrayOfGroupPermissions();
			TPermissionMode otherPermission = request.getOtherPermission();
			TGroupPermission groupPermission = null;
			if (arrayOfUserPermissions  != null ) {
				return getFailedResponse("ACLs are not supported by the dCACHE",TStatusCode.SRM_NOT_SUPPORTED);
			}
			if (arrayOfGroupPermissions!=null &&
			    arrayOfGroupPermissions.getGroupPermissionArray()!=null &&
			    arrayOfGroupPermissions.getGroupPermissionArray().length>1) {
				return getFailedResponse("ACLs are not supported by the dCACHE",TStatusCode.SRM_NOT_SUPPORTED);
			}
			if (arrayOfGroupPermissions!=null &&
			    arrayOfGroupPermissions.getGroupPermissionArray()!=null) {
				if (arrayOfGroupPermissions.getGroupPermissionArray()[0]!=null) {
					groupPermission=arrayOfGroupPermissions.getGroupPermissionArray()[0];
				}
			}
			int iowner=(permissions>>6)&0x7;
			int igroup=(permissions>>3)&0x7;
			int iother=permissions&0x7;
			int requestOwner=iowner;
			int requestGroup=igroup;
			int requestOther=iother;
			if (permissionType==TPermissionType.REMOVE) {
				requestOwner=0;
				requestGroup=0;
				requestOther=0;
			}
			if ( ownerPermission != null ) {
			    requestOwner = PermissionMaskToTPermissionMode.permissionModetoMask(ownerPermission);
			}
			if ( otherPermission != null ) {
			    requestOther  = PermissionMaskToTPermissionMode.permissionModetoMask(otherPermission);
			}
			if ( groupPermission != null ) {
			    requestGroup =PermissionMaskToTPermissionMode.permissionModetoMask(groupPermission.getMode());
			}

			if (permissionType==TPermissionType.CHANGE) {
			    iowner=requestOwner;
			    igroup=requestGroup;
			    iother=requestOther;
			}
			else if (permissionType==TPermissionType.ADD) {
			    iowner|=requestOwner;
			    igroup|=requestGroup;
			    iother|=requestOther;
			}
			else if (permissionType==TPermissionType.REMOVE) {
				if (requestGroup!=0) {
                                    igroup ^= requestGroup;
                                }
				if (requestOther!=0) {
                                    iother ^= requestOther;
                                }
				if (requestOwner!=0) {
                                    iowner ^= requestOwner;
                                }
			}
			if ((permissionType==TPermissionType.ADD||permissionType==TPermissionType.CHANGE)) {
				if (ownerPermission==null && otherPermission==null && groupPermission==null ) {
					ownerPermission = TPermissionMode.R;
					otherPermission = TPermissionMode.R;
					iowner =PermissionMaskToTPermissionMode.permissionModetoMask(ownerPermission);
					iother =PermissionMaskToTPermissionMode.permissionModetoMask(otherPermission);
					if (arrayOfGroupPermissions!=null &&
					    arrayOfGroupPermissions.getGroupPermissionArray()!=null) {
						if (arrayOfGroupPermissions.getGroupPermissionArray()[0]==null) {
							groupPermission=new TGroupPermission(fmd.group,TPermissionMode.R);
							igroup=PermissionMaskToTPermissionMode.permissionModetoMask(groupPermission.getMode());
						}
					}
				}
			}


			int newPermissions = ((iowner<<6)|(igroup<<3))|iother;
			fmd.permMode=newPermissions;
			try {
			    storage.setFileMetaData(user,fmd);
			}
			catch (SRMException e) {
				logger.warn(e.toString());
				return getFailedResponse(e.getMessage(),TStatusCode.SRM_FAILURE);
			}
		}
		catch  (SRMException srme) {
			logger.warn(srme.toString());
			return getFailedResponse(srme.getMessage(),TStatusCode.SRM_FAILURE);
		}
		response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
		response.getReturnStatus().setExplanation("success");
		return response;
	}
}
