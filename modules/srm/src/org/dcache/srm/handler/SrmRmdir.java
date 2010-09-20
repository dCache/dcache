//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 11/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmRmdir
 *
 * Created on 11/05
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import java.util.Vector;
import java.util.Collections;
import java.util.Comparator;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInvalidPathException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.axis.types.URI.MalformedURIException;


/**
 *
 * @author  litvinse
 */

public class SrmRmdir {
        private static Logger logger =
            LoggerFactory.getLogger(SrmRmdir.class);
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement storage;
	SrmRmdirRequest           request;
	SrmRmdirResponse          response;
	SRMUser            user;

	public SrmRmdir(SRMUser user,
			RequestCredential credential,
			SrmRmdirRequest request,
			AbstractStorageElement storage,
			org.dcache.srm.SRM srm,
			String client_host) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}

	public SrmRmdirResponse getResponse() {
		if(response != null ) return response;
		try {
			response = srmRmdir();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
            TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme.toString());
            response = getFailedResponse(srme.toString());
		}
		return response;
	}

	private void getDirectoryTree(SRMUser user,
				      String directory,
				      Vector tree,
				      SrmRmdirRequest request,
				      SrmRmdirResponse response,
                                      boolean topdir)
                                      throws SRMException {

		if (response.getReturnStatus().getStatusCode() !=
                        TStatusCode.SRM_SUCCESS) return;
		String[] dirList;
		tree.add(directory);
		try {
			dirList  = storage.listNonLinkedDirectory(user,directory);
		}
                catch(SRMAuthorizationException srmae) {
			response.getReturnStatus().setStatusCode(
                                TStatusCode.SRM_AUTHORIZATION_FAILURE);
			response.getReturnStatus().setExplanation(
                                directory +" : "+srmae.getMessage());
			return;
                }
		catch (SRMInvalidPathException srmipe) {
                    if(topdir) {
			response.getReturnStatus().setStatusCode(
                                TStatusCode.SRM_INVALID_PATH);
			response.getReturnStatus().setExplanation(
                                directory +" : "+srmipe.getMessage());
			return;
                    } else {
			response.getReturnStatus().setStatusCode(
                                TStatusCode.SRM_NON_EMPTY_DIRECTORY);
			response.getReturnStatus().setExplanation(
                                directory +" : "+srmipe.getMessage());
			return;
                    }
		}
		catch (SRMException srme) {
			response.getReturnStatus().setStatusCode(
                                TStatusCode.SRM_FAILURE);
			response.getReturnStatus().setExplanation(
                                directory +" : "+srme.getMessage());
			return;
		}
		catch (Exception e) {
			logger.warn(e.toString());
			response.getReturnStatus().setStatusCode(
                                TStatusCode.SRM_FAILURE);
			response.getReturnStatus().setExplanation(
                                directory +" : "+e.toString());
			return;
		}
		if ( dirList == null ) {
			return;
		}
		if ( dirList.length == 0 ) {
			return;
		}
		else if ( dirList.length > 0 ) {
			if ( request.getRecursive() != null && request.getRecursive().booleanValue()) {
				for (int i=0; i<dirList.length; i++) {
					getDirectoryTree(user,directory+
                                                "/"+dirList[i],tree,request,response,false);
				}
			}
			else {
				response.getReturnStatus().setStatusCode(
                                        TStatusCode.SRM_NON_EMPTY_DIRECTORY);
				response.getReturnStatus().setExplanation(
                                        "non empty directory, no recursion flag specified ");
				return;
			}
		}
	}


	public static final SrmRmdirResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}

	public static final  SrmRmdirResponse getFailedResponse(String error,
                TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
		TReturnStatus status = new TReturnStatus();
		status.setStatusCode(statusCode);
		status.setExplanation(error);
		SrmRmdirResponse response = new SrmRmdirResponse();
		response.setReturnStatus(status);
		return response;
	}


	/**
	 * implementation of srm rmdir
	 */

	public SrmRmdirResponse srmRmdir() throws
                SRMException,MalformedURIException {
		SrmRmdirResponse response  = new SrmRmdirResponse();
		TReturnStatus returnStatus = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
			return getFailedResponse(" null request passed to SrmRm()");
		}
		Boolean recursive          = request.getRecursive();
		org.apache.axis.types.URI surl = request.getSURL();
		int port    = surl.getPort();
		String host = surl.getHost();
		String path = surl.getPath(true,true);
		int indx    = path.indexOf(SFN_STRING);
		String[] dirList;
		Vector tree = new Vector();
		boolean any_failed=false;
		if ( indx != -1 ) {
			path=path.substring(indx+SFN_STRING.length());
		}
		//
		// get list of directories
		//
		getDirectoryTree(user,path,tree,request,response,true);
		if (response.getReturnStatus().getStatusCode() !=
                        TStatusCode.SRM_SUCCESS) {
			return response;
		}
		//
		// sort directories so the deepest one is the first one
		//
		Collections.sort(tree,
				 new Comparator() {
					 public int compare(Object a, Object b) {
						 int nA = 0;
						 int nB = 0;
						 String sa = (String)a;
						 String sb = (String)b;
						 for (int i=0;i<sa.length(); i++) {
							 if (sa.charAt(i) == '/')  nA++;
						 }
						 for (int i=0;i<sb.length(); i++) {
							 if (sb.charAt(i) == '/') nB++;
						 }
						 return ( nA < nB ) ? 1 : -1;
					 }
				 });
		//
		// Delete the suckers
		//
		try {
			storage.removeDirectory(user,tree);
		}
		catch (SRMException srme) {
			logger.warn("failed to remove "+path,srme);
			response.getReturnStatus().setStatusCode(
                                TStatusCode.SRM_FAILURE);
			response.getReturnStatus().setExplanation(
                                path+" "+srme.getMessage());
			return response;
		}
		response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
		response.getReturnStatus().setExplanation("success");
		return response;
	}
}
