package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.FileMetaData;
import org.apache.log4j.Logger;
import org.apache.axis.types.URI.MalformedURIException;
import java.sql.SQLException;
/**
 *
 * @author  timur
 */
public class SrmPutDone {
    private static Logger logger = 
            Logger.getLogger(SrmPutDone.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmPutDoneRequest srmPutDoneRequest;
    SrmPutDoneResponse response;
    Scheduler putScheduler;
    SRMUser user;
    RequestCredential credential;
    PutRequestStorage putStorage;
    PutFileRequestStorage putFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;

    public SrmPutDone(SRMUser user,
            RequestCredential credential,
            SrmPutDoneRequest srmPutDoneRequest,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        this.srmPutDoneRequest = srmPutDoneRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.putScheduler = srm.getPutRequestScheduler();
        this.configuration = srm.getConfiguration();
    }
    
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPutDoneResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmPutDone();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle);
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        } catch(SRMException srme) {
            logger.error(srme);
            response = getFailedResponse(srme.toString());
        } catch(IllegalStateTransition ist) {
            logger.error(ist);
            response = getFailedResponse(ist.toString());
        }
        return response;
    }
    
    public static final SrmPutDoneResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmPutDoneResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();
        srmPutDoneResponse.setReturnStatus(status);
        return srmPutDoneResponse;
    }

    public SrmPutDoneResponse srmPutDone()
	throws SRMException,
	MalformedURIException,
	SQLException, 
	IllegalStateTransition {
        String requestToken = srmPutDoneRequest.getRequestToken();
        if( requestToken == null ) {
            return getFailedResponse("request contains no request token");
        }
        Long requestId;
        try {
            requestId = new Long( requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_FAILURE);
        }
        
        ContainerRequest request =(ContainerRequest) ContainerRequest.getRequest(requestId);
        if(request == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_FAILURE);
            
        }
        if ( !(request instanceof PutRequest) ){
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not srmPrepareToGet request",
                    TStatusCode.SRM_FAILURE);
            
        }
        PutRequest putRequest = (PutRequest) request;
        org.apache.axis.types.URI [] surls;
        if(srmPutDoneRequest.getArrayOfSURLs() ==null) {
            surls = null;
        } else {
            surls = srmPutDoneRequest.getArrayOfSURLs().getUrlArray();
        }
        String[] surl_strings = null;
	TReturnStatus status = new TReturnStatus();
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();

	synchronized(putRequest) {
		FileRequest requests[] = putRequest.getFileRequests();
		State state = putRequest.getState();
		if(!State.isFinalState(state)) {
			if( surls == null ){
				int fail_counter=0;
				int success_counter=0;
				if ( requests != null ) { 
					for (int i=0;i<requests.length;i++) { 
						PutFileRequest fileRequest =  (PutFileRequest) requests[i];
						synchronized(fileRequest) { 
							if ( !State.isFinalState(fileRequest.getState())) {
								if ( fileRequest.getTurlString()==null) { 
									fileRequest.setStatusCode(TStatusCode.SRM_INVALID_PATH);
									fileRequest.setState(State.FAILED,"SrmPutDone called, TURL is not ready");
									fail_counter++;
								}
								else { 
                                                                    try {
                                                                        if (storage.exists(user,fileRequest.getPath())) {
                                                                                fileRequest.setState(State.DONE,"SrmPutDone called");
										success_counter++;
									}
                                                                        else { 
										fail_counter++;
										fileRequest.setStatusCode(TStatusCode.SRM_INVALID_PATH);
										fileRequest.setState(State.FAILED,"SrmPutDone called : file does not exist");
									}
                                                                    }
                                                                    catch (SRMException e) { 
                                                                        fail_counter++;
                                                                        fileRequest.setStatusCode(TStatusCode.SRM_FAILURE);
                                                                        fileRequest.setState(State.FAILED,"SrmPutDone called : " + e.getMessage());
                                                                    }
								}
							}
							else { 
								if (fileRequest.getState()==State.DONE) { 
									success_counter++;
								}
								if (fileRequest.getState()==State.FAILED) { 
									fail_counter++;
								}
								if (fileRequest.getState()==State.CANCELED) { 
									fail_counter++;
								}
							}
						}
					}
					if (success_counter==requests.length) { 
						putRequest.setState(State.DONE,"SrmPutDone called");
						status.setStatusCode(TStatusCode.SRM_SUCCESS);
						status.setExplanation("success");
					}
					else if (success_counter<requests.length) { 
						putRequest.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
						status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
						status.setExplanation("request in progress");
					}
					if (fail_counter>0&&fail_counter<requests.length) { 
						putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
						status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
						status.setExplanation("some file transfer(s) were not performed on all SURLs");
					}
					else if (fail_counter==requests.length) { 
						putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
						putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
						status.setStatusCode(TStatusCode.SRM_FAILURE);
						status.setExplanation("no file transfer(s) were performed on SURL(s)");
					}
				}
				else { 
					return getFailedResponse("0 length file request array",
								 TStatusCode.SRM_INVALID_REQUEST);
				}
			}
			else {
				if(surls.length == 0) {
					return getFailedResponse("0 lenght SiteURLs array",
								 TStatusCode.SRM_INVALID_REQUEST);
				}
				surl_strings = new String[surls.length];
				int fail_counter=0;
				int success_counter=0;
				for(int i = 0; i< surls.length; ++i) {
					if(surls[i] != null ) {
						surl_strings[i] =surls[i].toString();
						PutFileRequest fileRequest = (PutFileRequest) putRequest.getFileRequestBySurl(surl_strings[i]);
						synchronized(fileRequest) {
							if ( !State.isFinalState(fileRequest.getState())) {
								if ( fileRequest.getTurlString()==null) { 
									fileRequest.setStatusCode(TStatusCode.SRM_INVALID_PATH);
									fileRequest.setState(State.FAILED,"SrmPutDone called, TURL is not ready");
									fail_counter++;
								}
								else { 
                                                                    try {
                                                                        if (storage.exists(user,fileRequest.getPath())) {
                                                                                fileRequest.setState(State.DONE,"SrmPutDone called");
										success_counter++;
									}
                                                                        else { 
										fail_counter++;
										fileRequest.setStatusCode(TStatusCode.SRM_INVALID_PATH);
										fileRequest.setState(State.FAILED,"SrmPutDone called : file does not exist");
									}
                                                                    }
                                                                    catch (SRMException e) { 
                                                                        fail_counter++;
                                                                        fileRequest.setStatusCode(TStatusCode.SRM_FAILURE);
                                                                        fileRequest.setState(State.FAILED,"SrmPutDone called : "+e.getMessage());
                                                                    }
								}
                                                        }
							else { 
								if (fileRequest.getState()==State.DONE) { 
									success_counter++;
								}
								if (fileRequest.getState()==State.FAILED) { 
									fail_counter++;
								}
								if (fileRequest.getState()==State.CANCELED) { 
									fail_counter++;
								}
								
							}
						}
					}
					else {
						return getFailedResponse("SiteURLs["+i+"] is null",
									 TStatusCode.SRM_INVALID_REQUEST);
					}
				}
				if (success_counter==requests.length) { 
					putRequest.setState(State.DONE,"SrmPutDone called");
					status.setStatusCode(TStatusCode.SRM_SUCCESS);
					status.setExplanation("success");
				}
				else if (success_counter<requests.length) { 
					putRequest.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
					status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
					status.setExplanation("request in progress");
				}
				if (fail_counter>0&&fail_counter<requests.length) { 
					putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
					status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
					status.setExplanation("some file transfer(s) were not performed on all SURLs");
					
				}
				else if (fail_counter==requests.length) { 
					putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
					putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
					status.setStatusCode(TStatusCode.SRM_FAILURE);
					status.setExplanation("no file transfer(s) were performed on SURL(s)");
				}
			}
		}
		else {
			int fail_counter=0;
			int success_counter=0;
			if( surls == null ){
				if ( requests != null ) { 
					for (int i=0;i<requests.length;i++) { 
						PutFileRequest fileRequest =  (PutFileRequest) requests[i];
						synchronized(fileRequest) { 
							if (fileRequest.getState()==State.DONE) { 
								success_counter++;
							} else { 
								fail_counter++;
							}
						}
					}
				}
			} 
			else {
				surl_strings = new String[surls.length];
				for(int i = 0; i< surls.length; ++i) {
					if(surls[i] != null ) {
						surl_strings[i] =surls[i].toString();
						PutFileRequest fileRequest = (PutFileRequest) putRequest.getFileRequestBySurl(surl_strings[i]);
						synchronized(fileRequest) {
							if (fileRequest.getState()==State.DONE) { 
								success_counter++;
							}
							else { 
								fail_counter++;
							}
							
						}
					}
					else {
						return getFailedResponse("SiteURLs["+i+"] is null",
									 TStatusCode.SRM_INVALID_REQUEST);
					}
				}
			}
			if (success_counter==requests.length) { 
				putRequest.setState(State.DONE,"SrmPutDone called");
				status.setStatusCode(TStatusCode.SRM_SUCCESS);
				status.setExplanation("success");
			}
			else if (success_counter<requests.length) { 
				putRequest.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
				status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
				status.setExplanation("request in progress");
			}
			if (fail_counter>0&&fail_counter<requests.length) { 
				putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				status.setExplanation("some file transfer(s) were not performed on all SURLs");
				
			}
			else if (fail_counter==requests.length) { 
				putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
				putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
				status.setStatusCode(TStatusCode.SRM_FAILURE);
				status.setExplanation("no file transfer(s) were performed on SURL(s)");
			}
		}
		if(surls != null) {
			srmPutDoneResponse.setArrayOfFileStatuses(
				new ArrayOfTSURLReturnStatus(
					putRequest.getArrayOfTSURLReturnStatus(surl_strings)));
		}
		srmPutDoneResponse.setReturnStatus(status);
	}
	return srmPutDoneResponse;
    }
	
    
}
