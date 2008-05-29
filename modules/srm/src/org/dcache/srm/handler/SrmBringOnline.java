/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmBringOnlineRequest;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
import org.dcache.srm.v2_2.TBringOnlineRequestFileStatus;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.sql.BringOnlineRequestStorage;
import org.dcache.srm.request.sql.BringOnlineFileRequestStorage;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class SrmBringOnline {
    
    
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmBringOnlineRequest request;
    SrmBringOnlineResponse response;
    Scheduler bringOnlineScheduler;
    RequestUser user;
    RequestCredential credential;
    BringOnlineRequestStorage bringOnlineStorage;
    BringOnlineFileRequestStorage bringOnlineFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    String client_host;
    /** Creates a new instance of SrmLs */
    public SrmBringOnline(RequestUser user,
            RequestCredential credential,
            SrmBringOnlineRequest request,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        if(request == null) {
            throw new NullPointerException("request is null");
        }
        this.request = request;
        this.user = user;
        this.client_host = client_host;
        this.credential = credential;
        if(storage == null) {
            throw new NullPointerException("storage is null");
        }
        this.storage = storage;
        this.bringOnlineScheduler = srm.getBringOnlineRequestScheduler();
        if(bringOnlineScheduler == null) {
            throw new NullPointerException("bringOnlineScheduler is null");
        }
        this.configuration = srm.getConfiguration();
        if(configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        this.bringOnlineStorage = srm.getBringOnlineStorage();
        if(bringOnlineStorage == null) {
            throw new NullPointerException("bringOnlineStorage is null");
        }
        this.bringOnlineFileRequestStorage = srm.getBringOnlineFileRequestStorage();
        if(bringOnlineFileRequestStorage == null) {
            throw new NullPointerException("bringOnlineFileRequestStorage is null");
        }
    }
    
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmBringOnline "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmBringOnline "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmBringOnline exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmBringOnlineResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmBringOnline();
        } catch(Exception e) {
            storage.elog(e);
            response = new SrmBringOnlineResponse();
            TReturnStatus returnStatus = new TReturnStatus();
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation(e.toString());
            response.setReturnStatus(returnStatus);
        }
        
        return response;
    }
    
    public static final SrmBringOnlineResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmBringOnlineResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmBringOnlineResponse srmBringOnlineResponse = new SrmBringOnlineResponse();
        srmBringOnlineResponse.setReturnStatus(status);
        return srmBringOnlineResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmBringOnlineResponse srmBringOnline()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        
        
        say("Entering srmBringOnline.");
        
        String [] protocols = null;
        
        if(request.getTransferParameters() != null &&
                request.getTransferParameters().getArrayOfTransferProtocols() != null ) {
            protocols =
                    request.getTransferParameters().getArrayOfTransferProtocols().getStringArray();
        }
        if(protocols == null || protocols.length <1) {
            esay("request contains no transfer protocols");
           // not for bring online
           // return getFailedResponse("request contains no transfer protocols",
           //     TStatusCode.SRM_INVALID_REQUEST);
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
            return getFailedResponse("request contains no file requests",
                TStatusCode.SRM_INVALID_REQUEST);
        }
        String[] surls = new String[fileRequests.length];
        long lifetimeInSeconds = 0;
        if( request.getDesiredTotalRequestTime() != null ) {
            long reqLifetime = (long)request.getDesiredTotalRequestTime().intValue();
            if(  lifetimeInSeconds < reqLifetime) {
                lifetimeInSeconds = reqLifetime;
            }
        }
        long lifetime =
                lifetimeInSeconds>0
                ?lifetimeInSeconds*1000
                :configuration.getGetLifetime();
        
        long desiredLietimeInSeconds ;
        
        if (request.getDesiredLifeTime() != null 
            && request.getDesiredLifeTime().intValue() != 0) {
            desiredLietimeInSeconds = 
                (long)request.getDesiredLifeTime().intValue();
        } else if( lifetimeInSeconds>0 ) {
            desiredLietimeInSeconds = lifetimeInSeconds;
        } else {
            desiredLietimeInSeconds = configuration.getGetLifetime() / 1000;
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
        //for bring online request we do not limit lifetime from above for bring online request
        try {
            say("BringOnlineStorage ="+bringOnlineStorage);
            BringOnlineRequest r =
                    new  BringOnlineRequest(user.getId(),
                    credential.getId(),
                    bringOnlineStorage,
                    surls,
                    protocols,
                    configuration,
                    lifetime,
                    desiredLietimeInSeconds,
                    bringOnlineFileRequestStorage,
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


            r.setScheduler(bringOnlineScheduler.getId(),0);
            bringOnlineStorage.saveJob(r,true);
            
            r.schedule(bringOnlineScheduler);
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmBringOnlineResponse();
        } catch(Exception e) {
            esay(e);
            return getFailedResponse(e.toString());
        }
        
    }
    
    
}
