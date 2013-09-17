/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import com.google.common.base.Joiner;
import org.apache.axis.types.URI.MalformedURIException;
import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.ArrayOfTPutRequestFileStatus;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TPutFileRequest;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */
public class SrmPrepareToPut {
    private static Logger logger =
            LoggerFactory.getLogger(SrmPrepareToPut.class);


    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmPrepareToPutRequest request;
    SrmPrepareToPutResponse response;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    String client_host;

    /** Creates a new instance of SrmLs */
    public SrmPrepareToPut(SRMUser user,
            RequestCredential credential,
            SrmPrepareToPutRequest request,
            AbstractStorageElement storage,
            SRM srm,
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
        this.configuration = srm.getConfiguration();
        if(configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        this.client_host = client_host;
    }

    boolean longFormat;

    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPrepareToPutResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmPrepareToPut();
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

    public static final SrmPrepareToPutResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmPrepareToPutResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmPrepareToPutResponse srmPrepareToPutResponse = new SrmPrepareToPutResponse();
        srmPrepareToPutResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToPutResponse;
    }

    private static final String[] emptyArr = new String[0];

    public SrmPrepareToPutResponse srmPrepareToPut()
    throws SRMException,MalformedURIException {
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
        for (String supportedProtocol : supportedProtocols) {
            for (String protocol : protocols) {
                if (supportedProtocol.equals(protocol)) {
                    foundMatchedProtocol = true;
                    break;
                }
            }
        }
	if (!foundMatchedProtocol) {
            String error = "Protocol(s) specified not supported: [ " + Joiner.on(' ').join(protocols) + ']';
            SrmPrepareToPutResponse srmPrepareToPutResponse = new SrmPrepareToPutResponse();
 	    srmPrepareToPutResponse.setReturnStatus(new TReturnStatus(TStatusCode.SRM_NOT_SUPPORTED, error));
 	    TPutRequestFileStatus[] statusArray = new TPutRequestFileStatus[fileRequests.length];
 	    for (int i = 0; i < fileRequests.length ; ++i ) {
 		TPutFileRequest fr = fileRequests[i];
 		if (fr!=null) {
 		    if (fr.getTargetSURL()!=null) {
 			TPutRequestFileStatus fileStatus = new TPutRequestFileStatus();
                        fileStatus.setSURL(fr.getTargetSURL());
 			fileStatus.setStatus(new TReturnStatus(TStatusCode.SRM_FAILURE, error));
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
            UnsignedLong knownSize = nextRequest.getExpectedFileSize();
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
            PutRequest r =
                    new  PutRequest(
                    user,
                    credential.getId(),
                    srcFileNames,
                    destUrls,
                    sizes,
                    wantPermanent,
                    protocols,
                    lifetime,
                    configuration.getGetRetryTimeout(),
                    configuration.getGetMaxNumOfRetries(),
                    client_host,
                    spaceToken,
                    retentionPolicy,
                    accessLatency,
                    request.getUserRequestDescription());
	    r.applyJdc();
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
                r.setOverwriteMode(overwriteMode);
            }

            r.schedule();
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmPrepareToPutResponse(configuration.getPutSwitchToAsynchronousModeDelay());
        } catch(Exception e) {
            logger.warn(e.toString());
            return getFailedResponse(e.toString());
        }

    }


}
