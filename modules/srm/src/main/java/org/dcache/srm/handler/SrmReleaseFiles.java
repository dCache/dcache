/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.apache.axis.types.URI.MalformedURIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static java.util.Arrays.asList;


/**
 *
 * @author  timur
 */
public class SrmReleaseFiles {


    private static final Logger logger=
        LoggerFactory.getLogger(SrmReleaseFiles.class.getName()) ;

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmReleaseFilesRequest srmReleaseFilesRequest;
    SrmReleaseFilesResponse response;
    Scheduler getScheduler;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    SRM srm;

    /** Creates a new instance of SrmReleaseFiles */
    public SrmReleaseFiles(SRMUser user,
            RequestCredential credential,
            SrmReleaseFilesRequest srmReleaseFilesRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {
        logger.info("SrmReleaseFiles user="+user);
        this.srmReleaseFilesRequest = srmReleaseFilesRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.getScheduler = srm.getGetRequestScheduler();
        this.configuration = srm.getConfiguration();
        this.srm = srm;
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmReleaseFilesResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmReleaseFiles();
        } catch(MalformedURIException | URISyntaxException e) {
            logger.debug(" malformed uri : "+e.getMessage());
            response = getFailedResponse(" malformed uri : "+e.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle.toString());
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        } catch(SRMInvalidRequestException ire) {
            response = getFailedResponse(ire.toString(),
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

    private static URI[] toUris(org.apache.axis.types.URI[] uris)
        throws URISyntaxException
    {
        URI[] result = new URI[uris.length];
        for (int i = 0; i < uris.length; i++) {
            result[i] = new URI(uris[i].toString());
        }
        return result;
    }

    public static final SrmReleaseFilesResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmReleaseFilesResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        logger.error("getFailedResponse: "+error+" StatusCode "+statusCode);

        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmReleaseFilesResponse srmReleaseFilesResponse = new SrmReleaseFilesResponse();
        srmReleaseFilesResponse.setReturnStatus(status);
        return srmReleaseFilesResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmReleaseFilesResponse srmReleaseFiles()
        throws SRMException, URISyntaxException, MalformedURIException,
               SQLException, IllegalStateTransition
    {
        String requestToken = srmReleaseFilesRequest.getRequestToken();
        URI[] surls;
        if(  srmReleaseFilesRequest.getArrayOfSURLs() == null ){
            if(requestToken == null) {
                return getFailedResponse(
                        "request contains no request token and no SURLs",
                        TStatusCode.SRM_NOT_SUPPORTED);
            }
            surls = null;
        }
	else if (requestToken == null) {
            surls = toUris(srmReleaseFilesRequest.getArrayOfSURLs().getUrlArray());

            return unpinDirectlyBySURLs(surls);

        }
        else {
            surls = toUris(srmReleaseFilesRequest.getArrayOfSURLs().getUrlArray());
        }

        long requestId;
        try {
            requestId = Long.parseLong(requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_INVALID_REQUEST);
        }

        ContainerRequest<?> request = Job.getJob(requestId, ContainerRequest.class);
        request.applyJdc();

        if ( !(request instanceof GetRequest || request instanceof BringOnlineRequest) ){
            return getFailedResponse("request for requestToken \""+
				     requestToken+"\"is not srmPrepareToGet or srmBringOnlineRequest request",
				     TStatusCode.SRM_INVALID_REQUEST);
        }

        //if(request instanceof GetRequest) {
        if( surls == null ){
            if(request instanceof GetRequest) {
                request.setState(State.DONE,"SrmReleaseFiles called");
            } else {
                BringOnlineRequest bringOnlineRequest = (BringOnlineRequest)request;
                return bringOnlineRequest.releaseFiles(null);
            }
        } else {
            if(surls.length == 0) {
                return getFailedResponse("0 lenght SiteURLs array");
            }
            if(request instanceof GetRequest) {
                for (URI surl : surls) {
                    FileRequest<?> fileRequest = request
                            .getFileRequestBySurl(surl);
                    fileRequest.setState(State.DONE, "SrmReleaseFiles called");
                }
            } else {
                BringOnlineRequest bringOnlineRequest = (BringOnlineRequest)request;
                return bringOnlineRequest.releaseFiles(surls);

            }
        }

        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(TStatusCode.SRM_SUCCESS);
        SrmReleaseFilesResponse srmReleaseFilesResponse = new SrmReleaseFilesResponse();
        srmReleaseFilesResponse.setReturnStatus(status);
        if( surls != null) {
            TSURLReturnStatus[] surlReturnStatusArray =  request.getArrayOfTSURLReturnStatus(surls);
            for (TSURLReturnStatus surlReturnStatus:surlReturnStatusArray) {
                if(surlReturnStatus.getStatus().getStatusCode() == TStatusCode.SRM_RELEASED) {
                    surlReturnStatus.getStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
                }
            }
            srmReleaseFilesResponse.setArrayOfFileStatuses(
                    new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        }

        // FIXME we do this to make the srm update the status of the request if it changed
        request.getTReturnStatus();
        return srmReleaseFilesResponse;

    }

    private SrmReleaseFilesResponse unpinFilesDirectlyBySURLAndRequestId(long requestId, URI[] surls)
        throws MalformedURIException
    {
        TSURLReturnStatus[] surlReturnStatusArray =
            new TSURLReturnStatus[surls.length];
        int failure_num=0;
        for (int i = 0; i< surls.length; ++i) {
           URI surl = surls[i];
           surlReturnStatusArray[i] = new TSURLReturnStatus();
           surlReturnStatusArray[i].setSurl(new org.apache.axis.types.URI(surl.toASCIIString()));
            try {
                BringOnlineFileRequest.unpinBySURLandRequestId(storage,
                    user,requestId,surl);
                surlReturnStatusArray[i].setStatus(
                    new TReturnStatus(TStatusCode.SRM_SUCCESS,"released"));
            }
            catch(Exception e) {
                surlReturnStatusArray[i].setStatus(
                    new TReturnStatus(TStatusCode.SRM_FAILURE,"release failed: "+e));
                failure_num++;

            }
        }

        TReturnStatus status = new TReturnStatus();
        if(failure_num == 0) {
            status.setStatusCode(TStatusCode.SRM_SUCCESS);
        } else if(failure_num < surls.length) {
            status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
        } else {
            status.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        SrmReleaseFilesResponse srmReleaseFilesResponse = new SrmReleaseFilesResponse();
        srmReleaseFilesResponse.setReturnStatus(status);
        srmReleaseFilesResponse.setArrayOfFileStatuses(
                                new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        return srmReleaseFilesResponse;
    }

   private SrmReleaseFilesResponse unpinDirectlyBySURLs(URI[] surls)
       throws SQLException, IllegalStateTransition, SRMInvalidRequestException,
              MalformedURIException
   {
        //prepare initial return statuses
        Map<URI,TSURLReturnStatus> surlsMap =
            new HashMap<>();
        for(URI surl: surls) {
            TSURLReturnStatus rs = new TSURLReturnStatus();
            rs.setSurl(new org.apache.axis.types.URI(surl.toASCIIString()));
            rs.setStatus(
                new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR,"not released"));
            surlsMap.put(surl,rs);
        }

        //try to find and relelease active get and bring online file requests
        releaseFileRequestsDirectlyBySURLs(surls,surlsMap);

        //try to unpin surls directly in the pin manager too
        unpinFilesDirectlyBySURLs(surls,surlsMap);

        //analize the results to form SrmReleaseFilesResponse
        int failure_num=0;
        TSURLReturnStatus[] surlReturnStatusArray =
            new TSURLReturnStatus[surls.length];
        for(int i = 0; i<surls.length; i++) {
            TSURLReturnStatus rs = surlsMap.get(surls[i]);
            if(!rs.getStatus().getStatusCode().equals(TStatusCode.SRM_SUCCESS)) {
                failure_num++;
            }
            surlReturnStatusArray[i] = rs;
        }

        TReturnStatus status = new TReturnStatus();
        if(failure_num == 0) {
            status.setStatusCode(TStatusCode.SRM_SUCCESS);
        } else if(failure_num < surls.length) {
            status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
        } else {
            status.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        SrmReleaseFilesResponse srmReleaseFilesResponse = new SrmReleaseFilesResponse();
        srmReleaseFilesResponse.setReturnStatus(status);
        srmReleaseFilesResponse.setArrayOfFileStatuses(
                                new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        return srmReleaseFilesResponse;

    }

    private void unpinFilesDirectlyBySURLs(URI[] surls, Map<URI,TSURLReturnStatus> surlsMap) {
        for (URI surl:surls) {
           TSURLReturnStatus rs=surlsMap.get(surl);
            try {
                BringOnlineFileRequest.unpinBySURL(storage, user,surl);
                rs.setStatus(
                    new TReturnStatus(TStatusCode.SRM_SUCCESS,"released"));
            }
            catch(Exception e) {
                logger.warn(e.toString());
                //if rs status is TStatusCode.SRM_INTERNAL_ERROR
                // this means it was not changed since
                // it is set initially in the calling function
                // if it chanded, we do not want to override it
                // as other method of releasing might have already
                // succeded

                if(rs.getStatus().getStatusCode().equals(TStatusCode.SRM_INTERNAL_ERROR)) {
                    rs.setStatus(
                        new TReturnStatus(TStatusCode.SRM_FAILURE,"release failed: "+e));
                }
            }
        }

    }

    private void releaseFileRequestsDirectlyBySURLs(URI[] surls,
        Map<URI,TSURLReturnStatus> surlsMap)
        throws SQLException,
        IllegalStateTransition,
        SRMInvalidRequestException
   {
        Set<BringOnlineFileRequest> bofrsToRelease =
            findBringOnlineFileRequestBySURLs(surls);
       for (BringOnlineFileRequest fileRequest: bofrsToRelease) {

            TSURLReturnStatus release_rs = fileRequest.releaseFile();
            if(release_rs.getStatus().getStatusCode().equals(TStatusCode.SRM_SUCCESS) ) {
                surlsMap.put(fileRequest.getSurl(), release_rs);
            } else {
                TSURLReturnStatus rs = surlsMap.get(release_rs.getSurl());

                //if rs status is TStatusCode.SRM_INTERNAL_ERROR
                // this means it was not changed since
                // it is set initially in the calling function
                // if it chanded, we do not want to override it
                // as other method of releasing might have already
                // succeded

                if(rs.getStatus().getStatusCode().equals(TStatusCode.SRM_INTERNAL_ERROR)) {
                    surlsMap.put(fileRequest.getSurl(),release_rs);
                }

            }
        }

       Set<GetFileRequest> gfrToRelease =
            findGetFileRequestBySURLs(surls);
       for (GetFileRequest fileRequest: gfrToRelease) {
            fileRequest.setState(State.DONE,"SrmReleaseFiles called");
            TSURLReturnStatus surlReturnStatus =  fileRequest.getTSURLReturnStatus();
            if(surlReturnStatus.getStatus().getStatusCode() == TStatusCode.SRM_RELEASED) {
                surlReturnStatus.getStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
                surlsMap.put(fileRequest.getSurl(), surlReturnStatus);
            } else {
                TSURLReturnStatus rs = surlsMap.get(surlReturnStatus.getSurl());

                //if rs status is TStatusCode.SRM_INTERNAL_ERROR
                // this means it was not changed since
                // it is set initially in the calling function
                // if it chanded, we do not want to override it
                // as other method of releasing might have already
                // succeded

                if(rs.getStatus().getStatusCode().equals(TStatusCode.SRM_INTERNAL_ERROR)) {
                    surlsMap.put(fileRequest.getSurl(), surlReturnStatus);
                }

            }
        }

    }

    private Set<BringOnlineFileRequest> findBringOnlineFileRequestBySURLs(URI[] surls)
            throws SQLException
    {
        Collection<URI> surlList = (surls.length > 2) ? new HashSet<>(asList(surls)) : asList(surls);
        Set<BringOnlineFileRequest> requests = new HashSet<>();
        for (BringOnlineFileRequest request : Job.getActiveJobs(BringOnlineFileRequest.class)) {
            if (surlList.contains(request.getSurl())) {
                requests.add(request);
            }
        }
        return requests;
    }

    private Set<GetFileRequest> findGetFileRequestBySURLs(URI[] surls) throws SQLException
    {
        Collection<URI> surlList = (surls.length > 2) ? new HashSet<>(asList(surls)) : asList(surls);
        Set<GetFileRequest> requests = new HashSet<>();
        for (GetFileRequest request : Job.getActiveJobs(GetFileRequest.class)) {
            if (surlList.contains(request.getSurl())) {
                requests.add(request);
            }
        }
        return requests;
    }
}
