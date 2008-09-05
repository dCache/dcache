/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.sql.GetRequestStorage;
import org.dcache.srm.request.sql.GetFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;

/**
 *
 * @author  timur
 */
public class SrmPrepareToGet {
    
    
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmPrepareToGetRequest request;
    SrmPrepareToGetResponse responce;
    Scheduler getScheduler;
    RequestUser user;
    RequestCredential credential;
    GetRequestStorage getStorage;
    GetFileRequestStorage getFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    private String client_host;
    /** Creates a new instance of SrmLs */
    public SrmPrepareToGet(RequestUser user,
            RequestCredential credential,
            SrmPrepareToGetRequest request,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        if(request == null) {
            throw new NullPointerException("request is null");
        }
        this.request = request;
        this.user = user;
        this.credential = credential;
        this.client_host = client_host;
        if(storage == null) {
            throw new NullPointerException("storage is null");
        }
        this.storage = storage;
        this.getScheduler = srm.getGetRequestScheduler();
        if(getScheduler == null) {
            throw new NullPointerException("getScheduler is null");
        }
        this.configuration = srm.getConfiguration();
        if(configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        this.getStorage = srm.getGetStorage();
        if(getStorage == null) {
            throw new NullPointerException("getStorage is null");
        }
        this.getFileRequestStorage = srm.getGetFileRequestStorage();
        if(getFileRequestStorage == null) {
            throw new NullPointerException("getFileRequestStorage is null");
        }
    }
    
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmPrepareToGet "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmPrepareToGet "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmPrepareToGet exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPrepareToGetResponse getResponse() {
        if(responce != null ) return responce;
        try {
            responce = srmPrepareToGet();
        } catch(Exception e) {
            storage.elog(e);
            responce = new SrmPrepareToGetResponse();
            TReturnStatus returnStatus = new TReturnStatus();
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation(e.toString());
            responce.setReturnStatus(returnStatus);
        }
        
        return responce;
    }
    
    public static final SrmPrepareToGetResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmPrepareToGetResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmPrepareToGetResponse srmPrepareToGetResponce = new SrmPrepareToGetResponse();
        srmPrepareToGetResponce.setReturnStatus(status);
        return srmPrepareToGetResponce;
    }
    
    private static final String[] emptyArr = new String[0];
    
    /**
     * implementation of srm ls
     */
    public SrmPrepareToGetResponse srmPrepareToGet()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        
        
        say("Entering srmPrepareToGet.");
        
        String [] protocols = null;
        if(request.getTransferParameters() != null &&
                request.getTransferParameters().getArrayOfTransferProtocols() != null ) {
            protocols =
                    request.getTransferParameters().getArrayOfTransferProtocols().getStringArray();
        }
        
        protocols = Tools.trimStringArray(protocols);

        if(protocols == null || protocols.length <1) {
            return getFailedResponse("request contains no transfer protocols");
        }

        if(request.getTransferParameters() != null &&
                request.getTransferParameters().getArrayOfClientNetworks() != null ) {
            String[] clientNetworks = 
                request.getTransferParameters().getArrayOfClientNetworks().getStringArray();
            if(clientNetworks != null && 
                clientNetworks.length >0 &&
                clientNetworks[0] != null) {
                client_host = clientNetworks[0];
            }
        }
        
        TGetFileRequest [] fileRequests = null;
        if(request.getArrayOfFileRequests() != null ) {
            fileRequests = request.getArrayOfFileRequests().getRequestArray();
        }
        if(fileRequests == null || fileRequests.length <1) {
            return getFailedResponse("request contains no file requests");
        }
        String[] surls = new String[fileRequests.length];
        long lifetimeInSeconds = 0;
        if( request.getDesiredTotalRequestTime() != null ) {
            long reqLifetime = (long)request.getDesiredTotalRequestTime().intValue();
            if(  lifetimeInSeconds < reqLifetime) {
                lifetimeInSeconds = reqLifetime;
            }
        }

	String[] supportedProtocols = storage.supportedPutProtocols();
	boolean foundMatchedProtocol=false;
	for(int i=0;i<supportedProtocols.length;i++){
	     for(int j=0; j<protocols.length; ++j) {
		 if(supportedProtocols[i].equals(protocols[j])){
		     foundMatchedProtocol=true;
		     break;
		 }
	     }
	}

	
	if (!foundMatchedProtocol) { 
            
	    TReturnStatus status = new TReturnStatus();
 	    status.setStatusCode(TStatusCode.SRM_NOT_SUPPORTED);
            StringBuffer errorsb = 
                new StringBuffer("Protocol(s) specified not supported: [ ");
	    for(String protocol:protocols) {
                errorsb.append(protocol).append(' ');
            }
            errorsb.append(']');
 	    status.setExplanation(errorsb.toString());
            esay(errorsb.toString());
 	    SrmPrepareToGetResponse srmPrepareToGetResponse = new SrmPrepareToGetResponse();
 	    srmPrepareToGetResponse.setReturnStatus(status);
 	    org.dcache.srm.v2_2.TGetRequestFileStatus[] statusArray = new org.dcache.srm.v2_2.TGetRequestFileStatus[fileRequests.length];
 	    for (int i = 0; i < fileRequests.length ; ++i ) {
 		TGetFileRequest fr = fileRequests[i];
 		if (fr!=null) { 
 		    if (fr.getSourceSURL()!=null) { 
 			TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
 			TReturnStatus fileReturnStatus = new TReturnStatus();
 			fileReturnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
 			fileReturnStatus.setExplanation(errorsb.toString());
 			fileStatus.setSourceSURL(fr.getSourceSURL());
 			fileStatus.setStatus(fileReturnStatus);
 			statusArray[i]=fileStatus;
 		    }
 		}
 	    }
 	    ArrayOfTGetRequestFileStatus arrayOfFileStatuses = new ArrayOfTGetRequestFileStatus(statusArray);
 	    srmPrepareToGetResponse.setArrayOfFileStatuses(arrayOfFileStatuses);
 	    return srmPrepareToGetResponse;
 	}
	
   

        
        for (int i = 0; i < fileRequests.length ; ++i ) {
            TGetFileRequest nextRequest = fileRequests[i];
            if(nextRequest == null ) {
                return getFailedResponse("file request #"+i+" is null",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            String nextSurl =null;
            if(nextRequest.getSourceSURL() != null) {
                
                nextSurl = nextRequest.getSourceSURL().toString();
            }
            if(nextSurl == null) {
                return getFailedResponse("can't get surl of file request #"+i+"  null",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            surls[i] = nextSurl;
        }
        long lifetime =
                lifetimeInSeconds>0
                ?lifetimeInSeconds*1000>configuration.getGetLifetime()
                ?configuration.getGetLifetime()
                :lifetimeInSeconds*1000
                :configuration.getGetLifetime();
        try {
            say("getStorage ="+getStorage);
            GetRequest r =
                    new  GetRequest(user.getId(),credential.getId(),
                    getStorage,
                    surls,
                    protocols,
                    configuration,
                    lifetime,
                    getFileRequestStorage,
                    configuration.getGetRetryTimeout(),
                    configuration.getGetMaxNumOfRetries(),
                    request.getUserRequestDescription(),
                    client_host);

	    if (request.getStorageSystemInfo()!=null) { 
		    if ( request.getStorageSystemInfo().getExtraInfoArray()!=null) { 
			    if (request.getStorageSystemInfo().getExtraInfoArray().length>0) { 
				    for (int i=0;i<request.getStorageSystemInfo().getExtraInfoArray().length;i++) { 
					    TExtraInfo extraInfo = request.getStorageSystemInfo().getExtraInfoArray()[i];
					    if (extraInfo.getKey().equals("priority")) { 
						    int priority = Integer.parseInt(extraInfo.getValue());
						    r.setPriority(priority);
					    }
				    }
			    }
		    }
	    }
	    
            r.setScheduler(getScheduler.getId(),0);
            getStorage.saveJob(r,true);
            
            r.schedule(getScheduler);
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmPrepareToGetResponse();
        } catch(Exception e) {
            esay(e);
            return getFailedResponse(e.toString());
        }
        
    }
    
    
}
