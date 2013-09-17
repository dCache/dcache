package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
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
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();
        srmPutDoneResponse.setReturnStatus(new TReturnStatus(statusCode, error));
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
	TReturnStatus status;
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();

	synchronized(putRequest) {
		List<PutFileRequest> requests = putRequest.getFileRequests();
		State state = putRequest.getState();
		if(!State.isFinalState(state)) {
			if( surls == null ){
                                boolean hasFailures = false;
                                boolean hasSuccesses = false;
				for (PutFileRequest fileRequest : requests) {
                                    try(JDC ignored = fileRequest.applyJdc()) {
                                        synchronized(fileRequest) {
                                            if ( !State.isFinalState(fileRequest.getState())) {
                                                if (fileRequest.getTurlString()==null) {
                                                    fileRequest.setStateAndStatusCode(State.FAILED,
                                                            "SrmPutDone called, TURL is not ready",
                                                            TStatusCode.SRM_INVALID_PATH);
                                                    hasFailures = true;
                                                }
                                                else {
                                                    try {
                                                        if (storage.exists(user, fileRequest.getSurl())) {
                                                            fileRequest.setState(State.DONE,"SrmPutDone called");
                                                            hasSuccesses = true;
                                                        }
                                                        else {
                                                            hasFailures = true;
                                                            fileRequest.setStateAndStatusCode(
                                                                    State.FAILED,
                                                                    "SrmPutDone called : file does not exist",
                                                                    TStatusCode.SRM_INVALID_PATH);
                                                        }
                                                    }
                                                    catch (SRMException e) {
                                                        hasFailures = true;
                                                        fileRequest.setStateAndStatusCode(
                                                                State.FAILED,
                                                                "SrmPutDone called : " + e.getMessage(),
                                                                TStatusCode.SRM_FAILURE);
                                                    }
                                                }
                                            }
                                            else {
                                                if (fileRequest.getState()==State.DONE) {
                                                    hasSuccesses = true;
                                                }
                                                if (fileRequest.getState()==State.FAILED) {
                                                    hasFailures = true;
                                                }
                                                if (fileRequest.getState()==State.CANCELED) {
                                                    hasFailures = true;
                                                }
                                            }
                                        }
                                    }
				}
				if (!hasFailures) {
				    putRequest.setState(State.DONE,"SrmPutDone called");
				    status = new TReturnStatus(TStatusCode.SRM_SUCCESS, "success");
				}
                                else if (!hasSuccesses) {
                                    putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
                                    putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
                                    status = new TReturnStatus(TStatusCode.SRM_FAILURE, "no file transfer(s) were performed on SURL(s)");
                                }
				else {
				    putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
                                    status = new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, "some file transfer(s) were not performed on all SURLs");
				}
                        }
			else {
				if(surls.length == 0) {
					return getFailedResponse("0 lenght SiteURLs array",
								 TStatusCode.SRM_INVALID_REQUEST);
				}
				boolean hasFailures = false;
				boolean hasSuccesses = false;
				for(int i = 0; i< surls.length; ++i) {
					if(surls[i] != null ) {
						PutFileRequest fileRequest = putRequest.getFileRequestBySurl(surls[i]);
                                                try (JDC ignored = fileRequest.applyJdc()) {
                                                    synchronized(fileRequest) {
                                                            if ( !State.isFinalState(fileRequest.getState())) {
                                                                    if ( fileRequest.getTurlString()==null) {
                                                                            fileRequest.setStatusCode(TStatusCode.SRM_INVALID_PATH);
                                                                            fileRequest.setState(State.FAILED,"SrmPutDone called, TURL is not ready");
                                                                            hasFailures = true;
                                                                    }
                                                                    else {
                                        try {
                                            if (storage.exists(user,fileRequest.getSurl())) {
                                                    fileRequest.setState(State.DONE,"SrmPutDone called");
                                                    hasSuccesses = true;
                                            }
                                            else {
                                                hasFailures = true;
                                                fileRequest.setStateAndStatusCode(
                                                        State.FAILED,
                                                        "SrmPutDone called : file does not exist",
                                                        TStatusCode.SRM_INVALID_PATH);
                                            }
                                        }
                                        catch (SRMException e) {
                                            hasFailures = true;
                                            fileRequest.setStateAndStatusCode(
                                                    State.FAILED,
                                                    "SrmPutDone called : "+e.getMessage(),
                                                    TStatusCode.SRM_FAILURE);
                                        }
                                                                    }
                                                            }
                                                            else {
                                                                    if (fileRequest.getState()==State.DONE) {
                                                                            hasSuccesses = true;
                                                                    }
                                                                    if (fileRequest.getState()==State.FAILED) {
                                                                            hasFailures = true;
                                                                    }
                                                                    if (fileRequest.getState()==State.CANCELED) {
                                                                            hasFailures = true;
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
				if (!hasFailures) {
					putRequest.setState(State.DONE,"SrmPutDone called");
                                        status = new TReturnStatus(TStatusCode.SRM_SUCCESS, "success");
				}
                                else if (!hasSuccesses) {
                                    putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
                                    putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
                                    status = new TReturnStatus(TStatusCode.SRM_FAILURE, "no file transfer(s) were performed on SURL(s)");
				}
				else {
					putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
					status = new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, "some file transfer(s) were not performed on all SURLs");
				}
			}
		}
		else {
			boolean hasFailures = false;
			boolean hasSuccesses = false;
			if( surls == null ){
			    for (PutFileRequest fileRequest : requests) {
                                try (JDC ignored = fileRequest.applyJdc()) {
                                    synchronized(fileRequest) {
                                        if (fileRequest.getState()==State.DONE) {
                                            hasSuccesses = true;
                                        } else {
                                            hasFailures = true;
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
                                                                    hasSuccesses = true;
                                                            }
                                                            else {
                                                                    hasFailures = true;
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
			if (!hasFailures) {
				putRequest.setState(State.DONE,"SrmPutDone called");
                                status = new TReturnStatus(TStatusCode.SRM_SUCCESS, "success");
			}
                        else if (!hasSuccesses) {
                            putRequest.setStatusCode(TStatusCode.SRM_FAILURE);
                            putRequest.setState(State.FAILED,"no file transfer(s) were performed on SURL(s)");
                            status = new TReturnStatus(TStatusCode.SRM_FAILURE, "no file transfer(s) were performed on SURL(s)");
                        }
			else {
				putRequest.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
                                status = new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, "some file transfer(s) were not performed on all SURLs");
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
