package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static java.util.Arrays.asList;
import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

public class SrmReleaseFiles
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SrmReleaseFiles.class);

    private final AbstractStorageElement storage;
    private final SrmReleaseFilesRequest srmReleaseFilesRequest;
    private final SRMUser user;
    private SrmReleaseFilesResponse response;

    /** Creates a new instance of SrmReleaseFiles */
    public SrmReleaseFiles(SRMUser user,
                           RequestCredential credential,
                           SrmReleaseFilesRequest srmReleaseFilesRequest,
                           AbstractStorageElement storage,
                           SRM srm,
                           String clientHost)
    {
        this.srmReleaseFilesRequest = srmReleaseFilesRequest;
        this.user = user;
        this.storage = storage;
    }

    public SrmReleaseFilesResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmReleaseFiles();
            } catch (DataAccessException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse("Internal database error, please try again.", TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            }
        }
        return response;
    }

    private SrmReleaseFilesResponse srmReleaseFiles()
            throws DataAccessException, SRMInvalidRequestException, SRMInternalErrorException
    {
        ArrayOfAnyURI arrayOfSURLs = srmReleaseFilesRequest.getArrayOfSURLs();
        org.apache.axis.types.URI[] surls;
        if (arrayOfSURLs != null) {
            surls = arrayOfSURLs.getUrlArray();
        } else {
            surls = null;
        }

        if (surls != null && surls.length == 0) {
            throw new SRMInvalidRequestException("Request contains no SURL");
        }

        String requestToken = srmReleaseFilesRequest.getRequestToken();
        if (requestToken == null) {
            if (surls == null) {
                throw new SRMInvalidRequestException("Request contains no SURL");
            }
            return releaseBySURLs(surls);
        }

        ContainerRequest<?> requestToRelease;
        try {
            requestToRelease = Request.getRequest(requestToken, ContainerRequest.class);
        } catch (SRMInvalidRequestException e) {
            return unpinBySURLAndRequestToken(requestToken, surls);
        }
        try (JDC ignored = requestToRelease.applyJdc()) {
            TSURLReturnStatus[] surlReturnStatuses;
            if (requestToRelease instanceof GetRequest) {
                GetRequest getRequest = (GetRequest) requestToRelease;
                if (surls == null) {
                    surlReturnStatuses = getRequest.release();
                } else {
                    surlReturnStatuses = getRequest.releaseFiles(surls);
                }
            } else if (requestToRelease instanceof BringOnlineRequest) {
                BringOnlineRequest bringOnlineRequest = (BringOnlineRequest) requestToRelease;
                if (surls == null) {
                    surlReturnStatuses = bringOnlineRequest.release();
                } else {
                    surlReturnStatuses = bringOnlineRequest.releaseFiles(surls);
                }
            } else {
                throw new SRMInvalidRequestException("No such get or bring online request: " + requestToken);
            }
            return new SrmReleaseFilesResponse(
                    getSummaryReturnStatus(surlReturnStatuses),
                    new ArrayOfTSURLReturnStatus(surlReturnStatuses));
        }
    }

    private SrmReleaseFilesResponse unpinBySURLAndRequestToken(String requestToken, org.apache.axis.types.URI[] surls)
            throws SRMInternalErrorException
    {
        TSURLReturnStatus[] surlReturnStatusArray =
                new TSURLReturnStatus[surls.length];
        for (int i = 0; i < surls.length; ++i) {
            org.apache.axis.types.URI surl = surls[i];
            TReturnStatus returnStatus = BringOnlineFileRequest.unpinBySURLandRequestToken(
                    storage, user, requestToken, URI.create(surl.toString()));
            surlReturnStatusArray[i] = new TSURLReturnStatus(surl, returnStatus);
        }

        return new SrmReleaseFilesResponse(
                getSummaryReturnStatus(surlReturnStatusArray),
                new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
    }

    private SrmReleaseFilesResponse releaseBySURLs(org.apache.axis.types.URI[] surls)
            throws DataAccessException, SRMInternalErrorException, SRMInvalidRequestException
    {
        URI[] uris = toUris(surls);
        Map<URI, TReturnStatus> returnStatuses = new HashMap<>();
        releaseFileRequestsBySURLs(uris, returnStatuses);
        unpinBySURLs(uris, returnStatuses);
        TSURLReturnStatus[] surlReturnStatusArray = toTSURLReturnStatus(surls, returnStatuses);
        return new SrmReleaseFilesResponse(
                getSummaryReturnStatus(surlReturnStatusArray),
                new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
    }

    private void unpinBySURLs(URI[] surls, Map<URI, TReturnStatus> surlsMap)
    {
        for (URI surl : surls) {
            try {
                BringOnlineFileRequest.unpinBySURL(storage, user, surl);
                surlsMap.put(surl, new TReturnStatus(TStatusCode.SRM_SUCCESS, "released"));
            } catch (Exception e) {
                LOGGER.warn(e.toString());
                TReturnStatus existingStatus = surlsMap.get(surl);
                if (existingStatus == null || existingStatus.getStatusCode().equals(TStatusCode.SRM_SUCCESS)) {
                    surlsMap.put(surl, new TReturnStatus(TStatusCode.SRM_FAILURE, "release failed: " + e));
                }
            }
        }
    }

    private static void releaseFileRequestsBySURLs(URI[] surls, Map<URI, TReturnStatus> returnStatuses)
            throws DataAccessException, SRMInvalidRequestException, SRMInternalErrorException
    {
        for (BringOnlineFileRequest fileRequest : findBringOnlineFileRequestBySURLs(surls)) {
            TReturnStatus returnStatus = fileRequest.release(fileRequest.getContainerRequest().getUser());
            URI surl = fileRequest.getSurl();
            TReturnStatus existingStatus = returnStatuses.get(surl);
            if (existingStatus == null || existingStatus.getStatusCode().equals(TStatusCode.SRM_SUCCESS)) {
                returnStatuses.put(surl, returnStatus);
            }
        }

        for (GetFileRequest fileRequest : findGetFileRequestBySURLs(surls)) {
            TReturnStatus returnStatus = fileRequest.release();
            URI surl = fileRequest.getSurl();
            TReturnStatus existingStatus = returnStatuses.get(surl);
            if (existingStatus == null || existingStatus.getStatusCode().equals(TStatusCode.SRM_SUCCESS)) {
                returnStatuses.put(surl, returnStatus);
            }
        }
    }

    private static Set<BringOnlineFileRequest> findBringOnlineFileRequestBySURLs(URI[] surls)
            throws DataAccessException
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

    private static Set<GetFileRequest> findGetFileRequestBySURLs(URI[] surls) throws DataAccessException
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

    private static URI[] toUris(org.apache.axis.types.URI[] uris)
    {
        URI[] result = new URI[uris.length];
        for (int i = 0; i < uris.length; i++) {
            result[i] = URI.create(uris[i].toString());
        }
        return result;
    }

    private static TSURLReturnStatus[] toTSURLReturnStatus(org.apache.axis.types.URI[] surls,
                                                           Map<URI, TReturnStatus> returnStatuses)
    {
        TSURLReturnStatus[] surlReturnStatusArray = new TSURLReturnStatus[surls.length];
        for (int i = 0; i < surls.length; i++) {
            TReturnStatus status = returnStatuses.get(URI.create(surls[i].toString()));
            if (status == null) {
                status = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, "No pin found");
            }
            surlReturnStatusArray[i] = new TSURLReturnStatus(surls[i], status);
        }
        return surlReturnStatusArray;
    }

    public static final SrmReleaseFilesResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmReleaseFilesResponse getFailedResponse(String error,
                                                                  TStatusCode statusCode)
    {
        SrmReleaseFilesResponse srmReleaseFilesResponse = new SrmReleaseFilesResponse();
        srmReleaseFilesResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmReleaseFilesResponse;
    }
}
