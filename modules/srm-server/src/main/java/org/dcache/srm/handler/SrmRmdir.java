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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  litvinse
 */

public class SrmRmdir {
        private static Logger logger =
            LoggerFactory.getLogger(SrmRmdir.class);
	AbstractStorageElement storage;
	SrmRmdirRequest           request;
	SrmRmdirResponse          response;
	SRMUser            user;

	public SrmRmdir(SRMUser user,
			RequestCredential credential,
			SrmRmdirRequest request,
			AbstractStorageElement storage,
			SRM srm,
			String client_host) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}

	public SrmRmdirResponse getResponse() {
		if(response != null ) {
                    return response;
                }
		try {
			response = srmRmdir();
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

	private void getDirectoryTree(SRMUser user,
				      URI surl,
				      List<URI> surls,
				      SrmRmdirRequest request,
				      SrmRmdirResponse response,
                                      boolean topdir)
                                      throws SRMException {

		try {
                        if (response.getReturnStatus().getStatusCode() !=
                            TStatusCode.SRM_SUCCESS) {
                            return;
                        }
                        surls.add(surl);
                        List<URI> dirList =
                                storage.listNonLinkedDirectory(user,surl);
                        if (!dirList.isEmpty()) {
                                if ( request.getRecursive() != null && request
                                        .getRecursive()) {
                                        for (URI entry: dirList) {
                                                getDirectoryTree(user,
                                                                 entry,
                                                                 surls,
                                                                 request,
                                                                 response,
                                                                 false);
                                        }
                                }
                                else {
                                        response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_NON_EMPTY_DIRECTORY,
                                                "non empty directory, no recursion flag specified "));
                                }
                        }
		}
                catch(SRMAuthorizationException srmae) {
			response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE,
                                surl + " : " + srmae.getMessage()));
                }
		catch (SRMInvalidPathException srmipe) {
                    if(topdir) {
			response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_INVALID_PATH,
                                surl + " : " + srmipe.getMessage()));
                    } else {
			response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_NON_EMPTY_DIRECTORY,
                                surl + " : " + srmipe.getMessage()));
                    }
		}
		catch (SRMException srme) {
			response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_FAILURE,
                                surl + " : " + srme.getMessage()));
		}
		catch (Exception e) {
			logger.warn(e.toString());
			response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_FAILURE,
                                surl + " : " + e.toString()));
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
                SrmRmdirResponse response = new SrmRmdirResponse();
		response.setReturnStatus(new TReturnStatus(statusCode, error));
		return response;
	}


	/**
	 * implementation of srm rmdir
	 */

	public SrmRmdirResponse srmRmdir()
                throws SRMException, URISyntaxException
        {
		if(request==null) {
			return getFailedResponse(" null request passed to SrmRm()");
		}
                SrmRmdirResponse response  = new SrmRmdirResponse();
                response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
		URI surl = new URI(request.getSURL().toString());
		List<URI> surls = new ArrayList<>();
		//
		// get list of directories
		//
		getDirectoryTree(user,surl,surls,request,response,true);
		if (response.getReturnStatus().getStatusCode() !=
                        TStatusCode.SRM_SUCCESS) {
			return response;
		}
		//
		// sort directories so the deepest one is the first one
		//
		Collections.sort(surls,
				 new Comparator<URI>() {
					 @Override
                                         public int compare(URI a, URI b) {
						 int nA = 0;
						 int nB = 0;
						 String sa = a.toString();
						 String sb = b.toString();
						 for (int i=0;i<sa.length(); i++) {
							 if (sa.charAt(i) == '/') {
                                                             nA++;
                                                         }
						 }
						 for (int i=0;i<sb.length(); i++) {
							 if (sb.charAt(i) == '/') {
                                                             nB++;
                                                         }
						 }
						 return ( nA < nB ) ? 1 : -1;
					 }
				 });
		//
		// Delete the suckers
		//
		try {
			storage.removeDirectory(user,surls);
		}
		catch (SRMException srme) {
			logger.warn("failed to remove " + surl, srme);
			response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_FAILURE,
                                surl + " " + srme.getMessage()));
			return response;
		}
		response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS, "success"));
		return response;
	}
}
