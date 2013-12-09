package org.dcache.srm.handler;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TTransferParameters;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.any;
import static java.util.Arrays.asList;

public class SrmPrepareToGet
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmPrepareToGet.class);

    private final AbstractStorageElement storage;
    private final SrmPrepareToGetRequest request;
    private final SRMUser user;
    private final SRM srm;
    private final RequestCredential credential;
    private final Configuration configuration;
    private final String clientHost;
    private SrmPrepareToGetResponse response;

    public SrmPrepareToGet(SRMUser user,
                           RequestCredential credential,
                           SrmPrepareToGetRequest request,
                           AbstractStorageElement storage,
                           SRM srm,
                           String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.credential = checkNotNull(credential);
        this.clientHost = clientHost;
        this.storage = checkNotNull(storage);
        this.configuration = checkNotNull(srm.getConfiguration());
        this.srm = checkNotNull(srm);
    }

    public SrmPrepareToGetResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmPrepareToGet();
            } catch (SRMNotSupportedException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_NOT_SUPPORTED);
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (InterruptedException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse("Operation interrupted", TStatusCode.SRM_INTERNAL_ERROR);
            } catch (IllegalStateTransition e) {
                LOGGER.error(e.toString());
                response = getFailedResponse("Scheduling failed", TStatusCode.SRM_INTERNAL_ERROR);
            }
        }
        return response;
    }

    private SrmPrepareToGetResponse srmPrepareToGet()
            throws IllegalStateTransition, InterruptedException, SRMInvalidRequestException, SRMNotSupportedException,
                   SRMInternalErrorException
    {
        String[] protocols = getTransferProtocols(request);
        String clientHost = getClientHost(request).or(this.clientHost);
        long lifetime = getLifetime(request, configuration.getGetLifetime());
        String[] supportedProtocols = storage.supportedGetProtocols();
        URI[] surls = getSurls(request);

        boolean isAnyProtocolSupported = any(asList(protocols), in(asList(supportedProtocols)));
        if (!isAnyProtocolSupported) {
            throw new SRMNotSupportedException("Protocol(s) not supported: " + Arrays.toString(protocols));
        }

        GetRequest r =
                new GetRequest(
                        user,
                        credential.getId(),
                        surls,
                        protocols,
                        lifetime,
                        configuration.getGetRetryTimeout(),
                        configuration.getGetMaxNumOfRetries(),
                        request.getUserRequestDescription(),
                        clientHost);
        try (JDC ignored = r.applyJdc()) {
            String priority = getExtraInfo(request, "priority");
            if (priority != null) {
                try {
                    r.setPriority(Integer.parseInt(priority));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Ignoring non-integer user priority: {}" , priority);
                }
            }
            srm.schedule(r);
            return r.getSrmPrepareToGetResponse(configuration.getGetSwitchToAsynchronousModeDelay());
        }
    }

    private static URI[] getSurls(SrmPrepareToGetRequest request)
            throws SRMInvalidRequestException
    {
        TGetFileRequest[] fileRequests = getFileRequests(request);
        URI[] surls = new URI[fileRequests.length];
        for (int i = 0; i < fileRequests.length; ++i) {
            TGetFileRequest nextRequest = fileRequests[i];
            if (nextRequest == null) {
                throw new SRMInvalidRequestException("file request #" + (i + 1) + " is null");
            }
            if (nextRequest.getSourceSURL() == null) {
                throw new SRMInvalidRequestException("can't get surl of file request #" + (i + 1) + "  null");
            }
            surls[i] = URI.create(nextRequest.getSourceSURL().toString());
        }
        return surls;
    }

    private static String getExtraInfo(SrmPrepareToGetRequest request, String key)
    {
        ArrayOfTExtraInfo storageSystemInfo = request.getStorageSystemInfo();
        if (storageSystemInfo == null) {
            return null;
        }
        TExtraInfo[] extraInfoArray = storageSystemInfo.getExtraInfoArray();
        if (extraInfoArray == null || extraInfoArray.length <= 0) {
            return null;
        }
        for (TExtraInfo extraInfo : extraInfoArray) {
            if (extraInfo.getKey().equals(key)) {
                return extraInfo.getValue();
            }
        }
        return null;
    }

    private static long getLifetime(SrmPrepareToGetRequest request, long max) throws SRMInvalidRequestException
    {
        long lifetimeInSeconds = 0;
        if (request.getDesiredTotalRequestTime() != null) {
            long reqLifetime = (long) request.getDesiredTotalRequestTime().intValue();
            if (reqLifetime < 0) {
                /* [ SRM 2.2, 5.2.1 ]
                 * m) If input parameter desiredTotalRequestTime is 0 (zero), each file request
                 *    must be tried at least once. Negative value must be invalid.
                 */
                throw new SRMInvalidRequestException("Negative desiredTotalRequestTime is invalid.");
            }
            lifetimeInSeconds = reqLifetime;
        }

        if (lifetimeInSeconds > 0) {
            long lifetime = TimeUnit.SECONDS.toMillis(lifetimeInSeconds);
            return (lifetime > max) ? max : lifetime;
        } else {
            // Revisit: Behaviour doesn't match the SRM spec
            return max;
        }
    }

    private static TGetFileRequest[] getFileRequests(SrmPrepareToGetRequest request)
            throws SRMInvalidRequestException
    {
        TGetFileRequest[] fileRequests = request.getArrayOfFileRequests().getRequestArray();
        if (fileRequests == null || fileRequests.length <= 0) {
            throw new SRMInvalidRequestException("arrayOfFileRequest is empty");
        }
        return fileRequests;
    }

    private static Optional<String> getClientHost(SrmPrepareToGetRequest request)
    {
        TTransferParameters transferParameters = request.getTransferParameters();
        if (transferParameters != null && transferParameters.getArrayOfClientNetworks() != null) {
            String[] clientNetworks = transferParameters.getArrayOfClientNetworks().getStringArray();
            if (clientNetworks != null && clientNetworks.length > 0 && clientNetworks[0] != null) {
                return Optional.of(clientNetworks[0]);
            }
        }
        return Optional.absent();
    }

    private static String[] getTransferProtocols(SrmPrepareToGetRequest request)
            throws SRMInvalidRequestException
    {
        TTransferParameters transferParameters = request.getTransferParameters();
        if (transferParameters != null && transferParameters.getArrayOfTransferProtocols() != null) {
            String[] protocols = transferParameters.getArrayOfTransferProtocols().getStringArray();
            protocols = Tools.trimStringArray(protocols);
            if (protocols != null && protocols.length > 0) {
                return protocols;
            }
        }
        throw new SRMInvalidRequestException("request contains no transfer protocols");
    }

    public static final SrmPrepareToGetResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmPrepareToGetResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmPrepareToGetResponse srmPrepareToGetResponse = new SrmPrepareToGetResponse();
        srmPrepareToGetResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToGetResponse;
    }
}
