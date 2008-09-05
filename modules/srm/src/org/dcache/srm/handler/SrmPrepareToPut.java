/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.TPutFileRequest;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.ArrayOfTPutRequestFileStatus;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;

/**
 *
 * @author  timur
 */
public class SrmPrepareToPut {
    
    
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmPrepareToPutRequest request;
    SrmPrepareToPutResponse responce;
    Scheduler putScheduler;
    RequestUser user;
    RequestCredential credential;
    PutRequestStorage putStorage;
    PutFileRequestStorage putFileRequestStorage;
    Configuration configuration;
    String client_host;
    
    /** Creates a new instance of SrmLs */
    public SrmPrepareToPut(RequestUser user,
            RequestCredential credential,
            SrmPrepareToPutRequest request,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        if(request == null) {
            throw new NullPointerException("request is null");
        }
        this.request = request;
        this.user = user;
        this.credential = credential;
        if(storage == null) {
            throw new NullPointerException("storage is null");
        }
        this.storage = storage;
        this.putScheduler = srm.getPutRequestScheduler();
        if(putScheduler == null) {
            throw new NullPointerException("putScheduler is null");
        }
        this.configuration = srm.getConfiguration();
        if(configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        this.putStorage = srm.getPutStorage();
        if(putStorage == null) {
            throw new NullPointerException("putStorage is null");
        }
        this.putFileRequestStorage = srm.getPutFileRequestStorage();
        if(putFileRequestStorage == null) {
            throw new NullPointerException("putFileRequestStorage is null");
        }
        this.client_host = client_host;
    }
    
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmPrepareToPut "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmPrepareToPut "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmPrepareToPut exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    
    public Scheduler getPutScheduler() {
        return putScheduler;
    }
    
    public PutRequestStorage getPutStorage() {
        return putStorage;
    }
    
    public PutFileRequestStorage getPutFileRequestStorage() {
        return putFileRequestStorage;
    }
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPrepareToPutResponse getResponse() {
        if(responce != null ) return responce;
        try {
            responce = srmPrepareToPut();
        } catch(Exception e) {
            storage.elog(e);
            responce = new SrmPrepareToPutResponse();
            TReturnStatus returnStatus = new TReturnStatus();
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation(e.toString());
            responce.setReturnStatus(returnStatus);
        }
        
        return responce;
    }
    
    public static final SrmPrepareToPutResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmPrepareToPutResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmPrepareToPutResponse srmPrepareToPutResponce = new SrmPrepareToPutResponse();
        srmPrepareToPutResponce.setReturnStatus(status);
        return srmPrepareToPutResponce;
    }
    
    private static final String[] emptyArr = new String[0];

    public SrmPrepareToPutResponse srmPrepareToPut()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        
        
        say("Entering srmPrepareToPut()");
        TFileStorageType storageType = request.getDesiredFileStorageType();
        if(storageType != null && !storageType.equals(TFileStorageType.PERMANENT)) {
             return getFailedResponse("DesiredFileStorageType "+storageType+" is not supported",
                 TStatusCode.SRM_NOT_SUPPORTED);
        }
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
        
        String spaceToken = request.getTargetSpaceToken();
        TRetentionPolicy retentionPolicy =null;
        TAccessLatency accessLatency = null;
        if(request.getTargetFileRetentionPolicyInfo() != null) {
            retentionPolicy = 
                    request.getTargetFileRetentionPolicyInfo().getRetentionPolicy();
            accessLatency = 
                    request.getTargetFileRetentionPolicyInfo().getAccessLatency();
        }
        TPutFileRequest [] fileRequests = null;
        if(request.getArrayOfFileRequests() != null ) {
            fileRequests = request.getArrayOfFileRequests().getRequestArray();
        }
        if(fileRequests == null || fileRequests.length <1) {
            return getFailedResponse("request contains no file requests");
        }
        String[] srcFileNames = new String[fileRequests.length];
        String[] destUrls= new String[fileRequests.length];
        long[] sizes= new long[fileRequests.length];
        boolean[] wantPermanent= new boolean[fileRequests.length];
        long lifetimeInSeconds = 0;
        if( request.getDesiredTotalRequestTime() != null ) {
            long reqLifetime = (long)request.getDesiredTotalRequestTime().intValue();
            if(  lifetimeInSeconds < reqLifetime) {
                lifetimeInSeconds = reqLifetime;
            }
        }
        TOverwriteMode overwriteMode = request.getOverwriteOption();
        if(overwriteMode != null && 
           overwriteMode.equals(TOverwriteMode.WHEN_FILES_ARE_DIFFERENT)) {
            return getFailedResponse(
		"Overwrite Mode WHEN_FILES_ARE_DIFFERENT is not supported",
		TStatusCode.SRM_NOT_SUPPORTED);
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
 	    SrmPrepareToPutResponse srmPrepareToPutResponse = new SrmPrepareToPutResponse();
 	    srmPrepareToPutResponse.setReturnStatus(status);
 	    org.dcache.srm.v2_2.TPutRequestFileStatus[] statusArray = new org.dcache.srm.v2_2.TPutRequestFileStatus[fileRequests.length];
 	    for (int i = 0; i < fileRequests.length ; ++i ) {
 		TPutFileRequest fr = fileRequests[i];
 		if (fr!=null) { 
 		    if (fr.getTargetSURL()!=null) { 
 			TPutRequestFileStatus fileStatus = new TPutRequestFileStatus();
 			TReturnStatus fileReturnStatus = new TReturnStatus();
 			fileReturnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
 			fileReturnStatus.setExplanation(errorsb.toString());
 			fileStatus.setSURL(fr.getTargetSURL());
 			fileStatus.setStatus(fileReturnStatus);
 			statusArray[i]=fileStatus;
 		    }
 		}
 	    }
 	    ArrayOfTPutRequestFileStatus arrayOfFileStatuses = new ArrayOfTPutRequestFileStatus(statusArray);
 	    srmPrepareToPutResponse.setArrayOfFileStatuses(arrayOfFileStatuses);
 	    return srmPrepareToPutResponse;
 	}
	
        for (int i = 0; i < fileRequests.length ; ++i ) {
            TPutFileRequest nextRequest = fileRequests[i];
            if(nextRequest == null ) {
                return getFailedResponse("file request #"+i+" is null");
            }
            String nextSurl =null;
            if(nextRequest.getTargetSURL() != null ) {
                
                nextSurl = nextRequest.getTargetSURL().toString();
            }
            if(nextSurl == null) {
                return getFailedResponse("can't get surl of file request #"+i+"  null");
            }
            org.apache.axis.types.UnsignedLong knownSize = nextRequest.getExpectedFileSize();
            if(knownSize != null) {
                
                sizes[i] = knownSize.longValue();
            }
            // if knownsize is null then the sizes[i] should have be
            // initialized to zero by array constructor, no need to
            // explicitely assuign zerro
            wantPermanent[i] = true; //for now, extract type info from space token in the future
/*                nextRequest.getFileStorageType()==
                TFileStorageType.PERMANENT;*/
            srcFileNames[i] = nextSurl;
            destUrls[i] = nextSurl;
        }
        long lifetime =
                lifetimeInSeconds>0
                ?lifetimeInSeconds*1000>configuration.getPutLifetime()
                ?configuration.getPutLifetime()
                :lifetimeInSeconds*1000
                :configuration.getPutLifetime();
        try {
            say("[iyStorage ="+putStorage);
            /*
             *
             */
            PutRequest r =
                    new  PutRequest(
                    user.getId(),
                    credential.getId(),
                    putStorage,
                    srcFileNames,
                    destUrls,
                    sizes,
                    wantPermanent,
                    protocols,
                    configuration,
                    lifetime,
                    putFileRequestStorage,
                    configuration.getGetRetryTimeout(),
                    configuration.getGetMaxNumOfRetries(),
                    client_host,
                    spaceToken,
                    retentionPolicy,
                    accessLatency,
                    request.getUserRequestDescription());
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
            
            if(overwriteMode != null) {
                say("setting overwriteMode to "+overwriteMode);
                r.setOverwriteMode(overwriteMode);
            }
	    
            r.setScheduler(putScheduler.getId(),0);
            putStorage.saveJob(r,true);
            
            r.schedule(putScheduler);
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmPrepareToPutResponse();
        } catch(Exception e) {
            esay(e);
            return getFailedResponse(e.toString());
        }
        
    }
    
    
}
