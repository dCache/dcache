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

import org.dcache.srm.FileMetaData;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.SRMException;
import java.util.Vector;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;
import java.io.File;
import org.apache.axis.types.URI;


/**
 *
 * @author  litvinse
 */

public class SrmSetPermission {
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement   storage;
	SrmSetPermissionRequest  request;
	SrmSetPermissionResponse response;
	RequestUser              user;
	
	public SrmSetPermission(RequestUser user,
				RequestCredential credential,
				SrmSetPermissionRequest request, 
				AbstractStorageElement storage,
				org.dcache.srm.SRM srm,
				String client_host ) {
		this.request = request;
		this.user    = user;
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
			storage.elog(" SrmSetPermission exception : ");
			storage.elog(t);
		}
	}
	
	public SrmSetPermissionResponse getResponse() {
		if(response != null ) return response;
		try {
			response = srmSetPermission();
		} 
		catch(Exception e) {
			storage.elog(e);
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
	
	public SrmSetPermissionResponse srmSetPermission() throws SRMException,org.apache.axis.types.URI.MalformedURIException {
		SrmSetPermissionResponse response  = new SrmSetPermissionResponse();
		TReturnStatus returnStatus = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
			return getFailedResponse(" null request passed to SrmSetPermission()");
		}
		URI uri = request.getSURL();
		String path = uri.getPath(true,true);
		int indx = path.indexOf(SFN_STRING); 
		if (indx!=-1) { 
			path=path.substring(indx+SFN_STRING.length());
		}
		try { 
			FileMetaData fmd= storage.getFileMetaData(user,path);
			String owner    = fmd.owner;
			int permissions = fmd.permMode;
			int ownerid = Integer.parseInt(fmd.owner);
			int groupid = Integer.parseInt(fmd.group);
			if (ownerid != user.getUid()) { 
				return getFailedResponse("user "+user.getUid()+" does not own file "+request.getSURL()+" Can't delete",TStatusCode.SRM_FAILURE);
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
				if (arrayOfGroupPermissions.getGroupPermissionArray()[0]==null) {
					groupPermission=arrayOfGroupPermissions.getGroupPermissionArray()[0];
				}
			}
			int iowner=(permissions>>6)&0x7;
			int igroup=(permissions>>3)&0x7;
			int iother=permissions&0x7;
			if ( ownerPermission != null ) { 
				iowner = PermissionMaskToTPermissionMode.permissionModetoMask(ownerPermission);
			}
			if ( otherPermission != null ) { 
				iother = PermissionMaskToTPermissionMode.permissionModetoMask(otherPermission);	
			}
			if ( groupPermission != null ) {
				igroup=PermissionMaskToTPermissionMode.permissionModetoMask(groupPermission.getMode());
			}
			if ((permissionType==TPermissionType.ADD||permissionType==TPermissionType.CHANGE)) {  
				if (ownerPermission==null && otherPermission==null) {
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
			if (permissionType==TPermissionType.REMOVE) { 
				iowner=0;
				igroup=0;
				iother=0;
			}
			int newPermissions = ((iowner<<6)|(igroup<<3))|iother;
			fmd.permMode=newPermissions;
			try { 
				storage.setFileMetaData(user,fmd);
			}
			catch (SRMException e) {
				esay(e);
				return getFailedResponse(e.getMessage(),TStatusCode.SRM_FAILURE);
			}
		}
		catch  (SRMException srme) {
			esay(srme);
			return getFailedResponse(srme.getMessage(),TStatusCode.SRM_FAILURE);
		}
		response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
		response.getReturnStatus().setExplanation("success");
		return response;
	}
}
