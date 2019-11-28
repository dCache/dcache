package org.dcache.srm.handler;

import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Optional;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.util.Lifetimes;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TPutFileRequest;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TTransferParameters;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.any;
import static java.util.Arrays.asList;

public class SrmPrepareToPut
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmPrepareToPut.class);

    private final AbstractStorageElement storage;
    private final SrmPrepareToPutRequest request;
    private SrmPrepareToPutResponse response;
    private final SRMUser user;
    private final Configuration configuration;
    private final String clientHost;
    private final SRM srm;

    public SrmPrepareToPut(SRMUser user,
                           SrmPrepareToPutRequest request,
                           AbstractStorageElement storage,
                           SRM srm,
                           String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
        this.configuration = checkNotNull(srm.getConfiguration());
        this.clientHost = clientHost;
        this.srm = checkNotNull(srm);
    }

    public SrmPrepareToPutResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmPrepareToPut();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse("Operation interrupted.", TStatusCode.SRM_INTERNAL_ERROR);
            } catch (IllegalStateTransition e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse("Failed to schedule operation.", TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMNotSupportedException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_NOT_SUPPORTED);
            }
        }
        return response;
    }

    private SrmPrepareToPutResponse srmPrepareToPut()
            throws IllegalStateTransition, InterruptedException, SRMNotSupportedException, SRMInvalidRequestException,
                   SRMInternalErrorException
    {
        checkFileStorageType(request, TFileStorageType.PERMANENT);
        String[] protocols = getProtocols();
        String clientHost = getClientHost(request).orElse(this.clientHost);
        String spaceToken = request.getTargetSpaceToken();
        TRetentionPolicy retentionPolicy = null;
        TAccessLatency accessLatency = null;
        if (request.getTargetFileRetentionPolicyInfo() != null) {
            retentionPolicy =
                    request.getTargetFileRetentionPolicyInfo().getRetentionPolicy();
            accessLatency =
                    request.getTargetFileRetentionPolicyInfo().getAccessLatency();
        }
        TPutFileRequest[] fileRequests = getFileRequests(request);

        // assume transfers will take place in parallel
        long effectiveSize = largestFileOf(fileRequests);
        long lifetime = Lifetimes.calculateLifetime(request.getDesiredTotalRequestTime(),
                effectiveSize, configuration.getMaximumClientAssumedBandwidth(),
                configuration.getPutLifetime());
        TOverwriteMode overwriteMode = getOverwriteMode(request);

        String[] supportedProtocols = storage.supportedPutProtocols();
        boolean isAnyProtocolSupported = any(asList(protocols), in(asList(supportedProtocols)));
        if (!isAnyProtocolSupported) {
            throw new SRMNotSupportedException("Protocol(s) not supported: " + Arrays.toString(protocols));
        }

        URI[] surls = new URI[fileRequests.length];
        Long[] sizes = new Long[fileRequests.length];
        boolean[] wantPermanent = new boolean[fileRequests.length];
        for (int i = 0; i < fileRequests.length; ++i) {
            TPutFileRequest fileRequest = fileRequests[i];
            if (fileRequest == null) {
                throw new SRMInvalidRequestException("file request #" + (i + 1) + " is null.");
            }
            if (fileRequest.getTargetSURL() == null) {
                throw new SRMInvalidRequestException("surl of file request #" + (i + 1) + " is null.");
            }
            URI surl = URI.create(fileRequest.getTargetSURL().toString());
            UnsignedLong knownSize = fileRequest.getExpectedFileSize();
            if (knownSize != null) {
                sizes[i] = knownSize.longValue();
                if (sizes[i] < 0) {
                    throw new SRMInvalidRequestException("Negative file size is not allowed.");
                }
            }
            wantPermanent[i] = true; //for now, extract type info from space token in the future
/*                nextRequest.getFileStorageType()==
                TFileStorageType.PERMANENT;*/
            surls[i] = surl;
        }

        PutRequest r =
                new PutRequest(
                        srm.getSrmId(),
                        user,
                        surls,
                        sizes,
                        wantPermanent,
                        protocols,
                        lifetime,
                        configuration.getPutMaxPollPeriod(),
                        clientHost,
                        spaceToken,
                        retentionPolicy,
                        accessLatency,
                        request.getUserRequestDescription());
        try (JDC ignored = r.applyJdc()) {
            if (overwriteMode != null) {
                r.setOverwriteMode(overwriteMode);
            }

            srm.acceptNewJob(r);
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmPrepareToPutResponse(configuration.getPutSwitchToAsynchronousModeDelay());
        }
    }


    private long largestFileOf(TPutFileRequest[] requests)
    {
        long effectiveSize = 0;

        for (TPutFileRequest request : requests) {
            UnsignedLong size = request.getExpectedFileSize();
            if (size != null && size.longValue() > effectiveSize) {
                effectiveSize = size.longValue();
            }
        }

        return effectiveSize;
    }

    private static TOverwriteMode getOverwriteMode(SrmPrepareToPutRequest request)
            throws SRMNotSupportedException
    {
        TOverwriteMode overwriteMode = request.getOverwriteOption();
        if (overwriteMode != null && overwriteMode.equals(TOverwriteMode.WHEN_FILES_ARE_DIFFERENT)) {
            throw new SRMNotSupportedException(
                    "Overwrite Mode WHEN_FILES_ARE_DIFFERENT is not supported.");
        }
        return overwriteMode;
    }

    private static TPutFileRequest[] getFileRequests(SrmPrepareToPutRequest request)
            throws SRMInvalidRequestException
    {
        TPutFileRequest[] fileRequests = null;
        if (request.getArrayOfFileRequests() != null) {
            fileRequests = request.getArrayOfFileRequests().getRequestArray();
        }
        if (fileRequests == null || fileRequests.length < 1) {
            throw new SRMInvalidRequestException("request contains no file requests.");
        }
        return fileRequests;
    }

    private static Optional<String> getClientHost(SrmPrepareToPutRequest request)
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

    private static void checkFileStorageType(SrmPrepareToPutRequest request, TFileStorageType expectedStorageType)
            throws SRMNotSupportedException
    {
        TFileStorageType storageType = request.getDesiredFileStorageType();
        if (storageType != null && !storageType.equals(expectedStorageType)) {
            throw new SRMNotSupportedException("DesiredFileStorageType " + storageType + " is not supported.");
        }
    }

    private String[] getProtocols() throws SRMInvalidRequestException
    {
        String[] protocols = null;
        TTransferParameters transferParameters = request.getTransferParameters();
        if (transferParameters != null && transferParameters.getArrayOfTransferProtocols() != null) {
            protocols = transferParameters.getArrayOfTransferProtocols().getStringArray();
        }
        protocols = Tools.trimStringArray(protocols);
        if (protocols == null || protocols.length < 1) {
            throw new SRMInvalidRequestException("request contains no transfer protocols.");
        }
        return protocols;
    }

    public static final SrmPrepareToPutResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmPrepareToPutResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmPrepareToPutResponse srmPrepareToPutResponse = new SrmPrepareToPutResponse();
        srmPrepareToPutResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToPutResponse;
    }
}
