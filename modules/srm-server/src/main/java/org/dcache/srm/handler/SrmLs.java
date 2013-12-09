package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmLs
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmLs.class);

    private final int maxNumOfLevels;
    private final Configuration configuration;
    private final SrmLsRequest request;
    private SrmLsResponse response;
    private final RequestCredential credential;
    private final SRMUser user;
    private final SRM srm;
    private final String clientHost;
    private final int max_results_num;

    public SrmLs(SRMUser user,
                 RequestCredential credential,
                 SrmLsRequest request,
                 AbstractStorageElement storage,
                 SRM srm,
                 String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.max_results_num = srm.getConfiguration().getMaxNumberOfLsEntries();
        this.maxNumOfLevels = srm.getConfiguration().getMaxNumberOfLsLevels();
        this.credential = checkNotNull(credential);
        this.clientHost = clientHost;
        this.configuration = srm.getConfiguration();
        this.srm = checkNotNull(srm);
    }

    public SrmLsResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmLs();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            }
        }
        return response;
    }

    private SrmLsResponse srmLs() throws SRMInvalidRequestException, SRMInternalErrorException
    {
        int numOfLevels = Math.min(getNumOfLevels(request), maxNumOfLevels);
        int offset = getOffset(request);
        int count = getCount(request);
        boolean longFormat = getFullDetailedList(request);
        URI[] surls = getSurls(request);

        LsRequest r = new LsRequest(user,
                credential.getId(),
                surls,
                TimeUnit.HOURS.toMillis(1),
                configuration.getLsRetryTimeout(),
                configuration.getLsMaxNumOfRetries(),
                clientHost,
                count,
                offset,
                numOfLevels,
                longFormat,
                max_results_num);
        try (JDC ignored = r.applyJdc()) {
            srm.schedule(r);
            return r.getSrmLsResponse(configuration.getLsSwitchToAsynchronousModeDelay());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (IllegalStateTransition e) {
            throw new SRMInternalErrorException("Scheduling failure", e);
        }
    }

    private static URI[] getSurls(SrmLsRequest request) throws SRMInvalidRequestException
    {
        if (request.getArrayOfSURLs() == null ||
                request.getArrayOfSURLs().getUrlArray() == null ||
                request.getArrayOfSURLs().getUrlArray().length == 0) {
            throw new SRMInvalidRequestException("empty list of paths");
        }
        org.apache.axis.types.URI[] urls = request.getArrayOfSURLs().getUrlArray();
        URI[] surls = new URI[urls.length];
        for (int i = 0 ; i < urls.length; i++) {
            surls[i] = URI.create(urls[i].toString());
        }
        return surls;
    }

    private static boolean getFullDetailedList(SrmLsRequest request)
    {
        return (request.getFullDetailedList() != null) && request.getFullDetailedList();
    }

    private static int getCount(SrmLsRequest request) throws SRMInvalidRequestException
    {
        int count = request.getCount() != null ? request.getCount() : Integer.MAX_VALUE;
        if (count < 0) {
            throw new SRMInvalidRequestException("count value less than 0, disallowed");
        }
        return count;
    }

    private static int getOffset(SrmLsRequest request) throws SRMInvalidRequestException
    {
        int offset = request.getOffset() != null ? request.getOffset() : 0;
        if (offset < 0) {
            throw new SRMInvalidRequestException("offset value less than 0, disallowed ");
        }
        return offset;
    }

    private static int getNumOfLevels(SrmLsRequest request) throws SRMInvalidRequestException
    {
        // The SRM specification is not clear, but
        // probably intends that zero (0) means "no
        // recursion", one (1) means "current
        // directory plus one (1) level down, et
        // cetera.
        if (request.getAllLevelRecursive() != null && request.getAllLevelRecursive()) {
            return Integer.MAX_VALUE;
        }
        if (request.getNumOfLevels() != null) {
            int numOfLevels = request.getNumOfLevels();
            // The spec doesn't say what to do in case of negative
            // values, so filter 'em out...
            if (numOfLevels < 0) {
                throw new SRMInvalidRequestException("numOfLevels < 0");
            }
            return numOfLevels;
        }
        return 1;
    }

    public static final SrmLsResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmLsResponse getFailedResponse(String error,
                                                        TStatusCode statusCode)
    {
        SrmLsResponse response = new SrmLsResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}
