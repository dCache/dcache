/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import com.google.common.base.Joiner;
import org.apache.axis.types.URI.MalformedURIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  timur
 */
public class SrmPrepareToGet {

    private static Logger logger =
            LoggerFactory.getLogger(SrmPrepareToGet.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmPrepareToGetRequest request;
    SrmPrepareToGetResponse response;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    private String client_host;
    /** Creates a new instance of SrmLs */
    public SrmPrepareToGet(SRMUser user,
            RequestCredential credential,
            SrmPrepareToGetRequest request,
            AbstractStorageElement storage,
            SRM srm,
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
        this.configuration = srm.getConfiguration();
        if(configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPrepareToGetResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmPrepareToGet();
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

    public static final SrmPrepareToGetResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmPrepareToGetResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmPrepareToGetResponse srmPrepareToGetResponse = new SrmPrepareToGetResponse();
        srmPrepareToGetResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToGetResponse;
    }

    private static final String[] emptyArr = new String[0];

    /**
     * implementation of srm ls
     */
    public SrmPrepareToGetResponse srmPrepareToGet()
    throws SRMException,MalformedURIException {
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

        String[] supportedProtocols = storage.supportedGetProtocols();
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
            String error = "Protocol(s) specified not supported: [" + Joiner.on(' ').join(protocols) + "]";
            TReturnStatus status = new TReturnStatus(TStatusCode.SRM_NOT_SUPPORTED, error);
 	    SrmPrepareToGetResponse srmPrepareToGetResponse = new SrmPrepareToGetResponse();
 	    srmPrepareToGetResponse.setReturnStatus(status);
 	    TGetRequestFileStatus[] statusArray = new TGetRequestFileStatus[fileRequests.length];
 	    for (int i = 0; i < fileRequests.length ; ++i ) {
 		TGetFileRequest fr = fileRequests[i];
 		if (fr!=null) {
 		    if (fr.getSourceSURL()!=null) {
 			TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
                        fileStatus.setSourceSURL(fr.getSourceSURL());
 			fileStatus.setStatus(new TReturnStatus(TStatusCode.SRM_FAILURE, error));
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
            GetRequest r =
                    new  GetRequest(user,credential.getId(),
                    surls,
                    protocols,
                    lifetime,
                    configuration.getGetRetryTimeout(),
                    configuration.getGetMaxNumOfRetries(),
                    request.getUserRequestDescription(),
                    client_host);
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

            r.schedule();
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmPrepareToGetResponse(configuration.getGetSwitchToAsynchronousModeDelay());
        } catch(Exception e) {
            logger.warn(e.toString());
            return getFailedResponse(e.toString());
        }

    }


}
