package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
/**
 *
 * @author  timur
 */
public class SrmPutDone {
    private static Logger logger =
            LoggerFactory.getLogger(SrmPutDone.class);
    AbstractStorageElement storage;
    SrmPutDoneRequest srmPutDoneRequest;
    SrmPutDoneResponse response;
    Scheduler putScheduler;
    SRMUser user;
    RequestCredential credential;
    PutRequestStorage putStorage;
    PutFileRequestStorage putFileRequestStorage;
    Configuration configuration;
    int numOfLevels;

    public SrmPutDone(SRMUser user,
            RequestCredential credential,
            SrmPutDoneRequest srmPutDoneRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {
        this.srmPutDoneRequest = srmPutDoneRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.putScheduler = srm.getPutRequestScheduler();
        this.configuration = srm.getConfiguration();
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPutDoneResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmPutDone();
        } catch(URISyntaxException e) {
            logger.debug(" malformed uri : "+e.getMessage());
            response = getFailedResponse(" malformed uri : "+e.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle.toString());
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        } catch(SRMInvalidRequestException e) {
            logger.error(e.toString());
            response = getFailedResponse(e.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme.toString());
            response = getFailedResponse(srme.toString());
        } catch(IllegalStateTransition ist) {
            logger.error("Illegal State Transition : " +ist.getMessage());
            response = getFailedResponse("Illegal State Transition : " +ist.getMessage());
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

    private static URI[] toUris(org.apache.axis.types.URI[] uris)
        throws URISyntaxException
    {
        URI[] result = new URI[uris.length];
        for (int i = 0; i < uris.length; i++) {
            result[i] = new URI(uris[i].toString());
        }
        return result;
    }

    public SrmPutDoneResponse srmPutDone()
	throws SRMException,
               URISyntaxException,
               SQLException,
               IllegalStateTransition
    {
        String requestToken = srmPutDoneRequest.getRequestToken();
        if( requestToken == null ) {
            return getFailedResponse("request contains no request token");
        }
        long requestId;
        try {
            requestId = Long.parseLong(requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_FAILURE);
        }

        PutRequest putRequest = Job.getJob(requestId, PutRequest.class);
        putRequest.applyJdc();

        URI[] surls;
        if(srmPutDoneRequest.getArrayOfSURLs() == null) {
            surls = null;
        } else {
            surls = toUris(srmPutDoneRequest.getArrayOfSURLs().getUrlArray());
        }
	TReturnStatus status = new TReturnStatus();
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();

	synchronized(putRequest) {
		List<PutFileRequest> requests = putRequest.getFileRequests();
		State state = putRequest.getState();
		if(!State.isFinalState(state)) {
			if( surls == null ){
				int fail_counter=0;
				int success_counter=0;
				for (PutFileRequest fileRequest : requests) {
                                    try(JDC ignored = fileRequest.applyJdc()) {
                                        synchronized(fileRequest) {
                                            if ( !State.isFinalState(fileRequest.getState())) {
                                                if (fileRequest.getTurlString()==null) {
                                                    fileRequest.setStateAndStatusCode(State.FAILED,
                                                            "SrmPutDone called, TURL is not ready",
                                                            TStatusCode.SRM_INVALID_PATH);
                                                    fail_counter++;
                                                }
                                                else {
                                                    try {
                                                        if (storage.exists(user, fileRequest.getSurl())) {
                                                            fileRequest.setState(State.DONE,"SrmPutDone called");
                                                            success_counter++;
                                                        }
                                                        else {
                                                            fail_counter++;
                                                            fileRequest.setStateAndStatusCode(
                                                                    State.FAILED,
                                                                    "SrmPutDone called : file does not exist",
                                                                    TStatusCode.SRM_INVALID_PATH);
                                                        }
                                                    }
                                                    catch (SRMException e) {
                                                        fail_counter++;
                                                        fileRequest.setStateAndStatusCode(
                                                                State.FAILED,
                                                                "SrmPutDone called : " + e.getMessage(),
                                                                TStatusCode.SRM_FAILURE);
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
				}
				if (success_counter==requests.size()) {
				    putRequest.setState(State.DONE,"SrmPutDone called");
				    status.setStatusCode(TStatusCode.SRM_SUCCESS);
				    status.setExplanation("success");
				}
				else if (success_counter<requests.size()) {
				    putRequest.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
				    status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
				    status.setExplanation("request in progress");
				}
				if (fail_counter>0&&fail_counter<requests.size()) {
				    putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				    status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				    status.setExplanation("some file transfer(s) were not performed on all SURLs");
				}
				else if (fail_counter==requests.size()) {
				    putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
				    putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
				    status.setStatusCode(TStatusCode.SRM_FAILURE);
				    status.setExplanation("no file transfer(s) were performed on SURL(s)");
				}
			}
			else {
				if(surls.length == 0) {
					return getFailedResponse("0 lenght SiteURLs array",
								 TStatusCode.SRM_INVALID_REQUEST);
				}
				int fail_counter=0;
				int success_counter=0;
				for(int i = 0; i< surls.length; ++i) {
					if(surls[i] != null ) {
						PutFileRequest fileRequest = putRequest.getFileRequestBySurl(surls[i]);
                                                try (JDC ignored = fileRequest.applyJdc()) {
                                                    synchronized(fileRequest) {
                                                            if ( !State.isFinalState(fileRequest.getState())) {
                                                                    if ( fileRequest.getTurlString()==null) {
                                                                            fileRequest.setStatusCode(TStatusCode.SRM_INVALID_PATH);
                                                                            fileRequest.setState(State.FAILED,"SrmPutDone called, TURL is not ready");
                                                                            fail_counter++;
                                                                    }
                                                                    else {
                                        try {
                                            if (storage.exists(user,fileRequest.getSurl())) {
                                                    fileRequest.setState(State.DONE,"SrmPutDone called");
                                                                                    success_counter++;
                                            }
                                            else {
                                                fail_counter++;
                                                fileRequest.setStateAndStatusCode(
                                                        State.FAILED,
                                                        "SrmPutDone called : file does not exist",
                                                        TStatusCode.SRM_INVALID_PATH);
                                            }
                                        }
                                        catch (SRMException e) {
                                            fail_counter++;
                                            fileRequest.setStateAndStatusCode(
                                                    State.FAILED,
                                                    "SrmPutDone called : "+e.getMessage(),
                                                    TStatusCode.SRM_FAILURE);
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
					}
					else {
						return getFailedResponse("SiteURLs["+i+"] is null",
									 TStatusCode.SRM_INVALID_REQUEST);
					}
				}
				if (success_counter==requests.size()) {
					putRequest.setState(State.DONE,"SrmPutDone called");
					status.setStatusCode(TStatusCode.SRM_SUCCESS);
					status.setExplanation("success");
				}
				else if (success_counter<requests.size()) {
					putRequest.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
					status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
					status.setExplanation("request in progress");
				}
				if (fail_counter>0&&fail_counter<requests.size()) {
					putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
					status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
					status.setExplanation("some file transfer(s) were not performed on all SURLs");

				}
				else if (fail_counter==requests.size()) {
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
			    for (PutFileRequest fileRequest : requests) {
                                try (JDC ignored = fileRequest.applyJdc()) {
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
				for(int i = 0; i< surls.length; ++i) {
					if(surls[i] != null ) {
						PutFileRequest fileRequest = putRequest.getFileRequestBySurl(surls[i]);
                                                try (JDC ignored = fileRequest.applyJdc()) {
                                                    synchronized(fileRequest) {
                                                            if (fileRequest.getState()==State.DONE) {
                                                                    success_counter++;
                                                            }
                                                            else {
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
			}
			if (success_counter==requests.size()) {
				putRequest.setState(State.DONE,"SrmPutDone called");
				status.setStatusCode(TStatusCode.SRM_SUCCESS);
				status.setExplanation("success");
			}
			else if (success_counter<requests.size()) {
				putRequest.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
				status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
				status.setExplanation("request in progress");
			}
			if (fail_counter>0&&fail_counter<requests.size()) {
				putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
				status.setExplanation("some file transfer(s) were not performed on all SURLs");

			}
			else if (fail_counter==requests.size()) {
				putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
				putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
				status.setStatusCode(TStatusCode.SRM_FAILURE);
				status.setExplanation("no file transfer(s) were performed on SURL(s)");
			}
		}
		if(surls != null) {
			srmPutDoneResponse.setArrayOfFileStatuses(
				new ArrayOfTSURLReturnStatus(
					putRequest.getArrayOfTSURLReturnStatus(surls)));
		}
		srmPutDoneResponse.setReturnStatus(status);
	}
	return srmPutDoneResponse;
    }


}
