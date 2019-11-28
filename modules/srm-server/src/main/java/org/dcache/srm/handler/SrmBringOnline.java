package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.util.Lifetimes;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.SrmBringOnlineRequest;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.any;
import static java.util.Arrays.asList;

public class SrmBringOnline
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmBringOnline.class);

    private final SrmBringOnlineRequest request;
    private final AbstractStorageElement storage;
    private SrmBringOnlineResponse response;
    private final SRMUser user;
    private final SRM srm;
    private final Configuration configuration;
    private final String clientHost;

    public SrmBringOnline(SRMUser user,
                          SrmBringOnlineRequest request,
                          AbstractStorageElement storage,
                          SRM srm,
                          String clientHost)
    {
        this.srm = checkNotNull(srm);
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.clientHost = clientHost;
        this.storage = checkNotNull(storage);
        this.configuration = srm.getConfiguration();
    }

    public SrmBringOnlineResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmBringOnline();
            } catch (SRMNotSupportedException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_NOT_SUPPORTED);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmBringOnlineResponse srmBringOnline()
            throws SRMInvalidRequestException, SRMInternalErrorException, SRMNotSupportedException
    {
        String[] protocols = getProtocols(request);
        String clientHost = getClientNetwork(request).orElse(this.clientHost);
        TGetFileRequest[] fileRequests = getFileRequests(request);
        URI[] surls = getSurls(fileRequests);
        long requestTime = Lifetimes.calculateLifetime(request.getDesiredTotalRequestTime(), configuration.getBringOnlineLifetime());
        long desiredLifetimeInSeconds = getDesiredLifetime(request, requestTime);

        if (protocols != null && protocols.length > 0) {
            String[] supportedProtocols = storage.supportedGetProtocols();
            boolean isAnyProtocolSupported = any(asList(protocols), in(asList(supportedProtocols)));
            if (!isAnyProtocolSupported) {
                throw new SRMNotSupportedException("Protocol(s) not supported: " + Arrays.toString(protocols));
            }
        }

        BringOnlineRequest r =
                new BringOnlineRequest(
                        srm.getSrmId(),
                        user,
                        surls,
                        protocols,
                        requestTime,
                        desiredLifetimeInSeconds,
                        configuration.getBringOnlineMaxPollPeriod(),
                        request.getUserRequestDescription(),
                        clientHost);
        try (JDC ignored = r.applyJdc()) {
            srm.acceptNewJob(r);
            return r.getSrmBringOnlineResponse(configuration.getBringOnlineSwitchToAsynchronousModeDelay());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (IllegalStateTransition e) {
            throw new SRMInternalErrorException("Scheduling failure", e);
        }
    }

    private static long getDesiredLifetime(SrmBringOnlineRequest request, long requestTime)
    {
        if (request.getDesiredLifeTime() == null
                || request.getDesiredLifeTime() == 0) {
            return TimeUnit.MILLISECONDS.toSeconds(requestTime);
        }
        return (long) request.getDesiredLifeTime();
    }

    private static URI[] getSurls(TGetFileRequest[] fileRequests) throws SRMInvalidRequestException
    {
        URI[] surls = new URI[fileRequests.length];
        for (int i = 0; i < fileRequests.length; ++i) {
            TGetFileRequest fileRequest = fileRequests[i];
            if (fileRequest == null) {
                throw new SRMInvalidRequestException("file request #" + (i + 1) + " is null");
            }
            if (fileRequest.getSourceSURL() == null) {
                throw new SRMInvalidRequestException("can't get surl of file request #" + (i + 1) + "  null");
            }
            surls[i] = URI.create(fileRequest.getSourceSURL().toString());
        }
        return surls;
    }

    private static TGetFileRequest[] getFileRequests(SrmBringOnlineRequest request) throws SRMInvalidRequestException
    {
        TGetFileRequest[] fileRequests = null;
        if (request.getArrayOfFileRequests() != null) {
            fileRequests = request.getArrayOfFileRequests().getRequestArray();
        }
        if (fileRequests == null || fileRequests.length < 1) {
            throw new SRMInvalidRequestException("request contains no file requests");
        }
        return fileRequests;
    }

    private static String[] getProtocols(SrmBringOnlineRequest request)
    {
        String[] protocols = null;
        if (request.getTransferParameters() != null &&
                request.getTransferParameters().getArrayOfTransferProtocols() != null) {
            protocols = request.getTransferParameters().getArrayOfTransferProtocols().getStringArray();
        }
        return Tools.trimStringArray(protocols);
    }

    private static Optional<String> getClientNetwork(SrmBringOnlineRequest request)
    {
        if (request.getTransferParameters() != null &&
                request.getTransferParameters().getArrayOfClientNetworks() != null) {
            String[] clientNetworks =
                    request.getTransferParameters().getArrayOfClientNetworks().getStringArray();
            if (clientNetworks != null &&
                    clientNetworks.length > 0 &&
                    clientNetworks[0] != null) {
                return Optional.of(clientNetworks[0]);
            }
        }
        return Optional.empty();
    }

    public static final SrmBringOnlineResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmBringOnlineResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmBringOnlineResponse srmBringOnlineResponse = new SrmBringOnlineResponse();
        srmBringOnlineResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmBringOnlineResponse;
    }
}
