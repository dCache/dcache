/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.SrmBringOnlineRequest;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */
public class SrmBringOnline {

    private static Logger logger =
            LoggerFactory.getLogger(SrmBringOnline.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmBringOnlineRequest request;
    SrmBringOnlineResponse response;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    String client_host;
    /** Creates a new instance of SrmLs */
    public SrmBringOnline(SRMUser user,
            RequestCredential credential,
            SrmBringOnlineRequest request,
            AbstractStorageElement storage,
            SRM srm,
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
        this.configuration = srm.getConfiguration();
        if(configuration == null) {
            throw new NullPointerException("configuration is null");
        }
        }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmBringOnlineResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmBringOnline();
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

    public static final SrmBringOnlineResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmBringOnlineResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmBringOnlineResponse srmBringOnlineResponse = new SrmBringOnlineResponse();
        srmBringOnlineResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmBringOnlineResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmBringOnlineResponse srmBringOnline()
        throws SRMException, URISyntaxException
    {
        String [] protocols = null;
        if(request.getTransferParameters() != null &&
                request.getTransferParameters().getArrayOfTransferProtocols() != null ) {
            protocols =
                    request.getTransferParameters().getArrayOfTransferProtocols().getStringArray();
        }
        protocols = Tools.trimStringArray(protocols);
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
        URI[] surls = new URI[fileRequests.length];
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
                :configuration.getBringOnlineLifetime();

        long desiredLietimeInSeconds ;

        if (request.getDesiredLifeTime() != null
            && request.getDesiredLifeTime() != 0) {
            desiredLietimeInSeconds =
                (long)request.getDesiredLifeTime().intValue();
        } else if( lifetimeInSeconds>0 ) {
            desiredLietimeInSeconds = lifetimeInSeconds;
        } else {
            desiredLietimeInSeconds = configuration.getBringOnlineLifetime() / 1000;
        }

        for (int i = 0; i < fileRequests.length ; ++i ) {
            TGetFileRequest nextRequest = fileRequests[i];
            if(nextRequest == null ) {
                return getFailedResponse("file request #"+i+" is null",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            if (nextRequest.getSourceSURL() == null) {
                return getFailedResponse("can't get surl of file request #"+i+"  null",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            surls[i] = new URI(nextRequest.getSourceSURL().toString());
        }
        //for bring online request we do not limit lifetime from above for bring online request
        try {
            BringOnlineRequest r =
                    new  BringOnlineRequest(user,
                    credential.getId(),
                    surls,
                    protocols,
                    lifetime,
                    desiredLietimeInSeconds,
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
            return r.getSrmBringOnlineResponse(configuration.getBringOnlineSwitchToAsynchronousModeDelay());
        } catch(Exception e) {
            logger.error(e.toString());
            return getFailedResponse(e.toString());
        }

    }


}
