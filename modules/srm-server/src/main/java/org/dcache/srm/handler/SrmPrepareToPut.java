package org.dcache.srm.handler;

import com.google.common.base.Optional;
import org.apache.axis.types.UnsignedLong;
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
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TExtraInfo;
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
    private final RequestCredential credential;
    private final Configuration configuration;
    private final String clientHost;
    private final SRM srm;

    public SrmPrepareToPut(SRMUser user,
                           RequestCredential credential,
                           SrmPrepareToPutRequest request,
                           AbstractStorageElement storage,
                           SRM srm,
                           String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.credential = checkNotNull(credential);
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
        String clientHost = getClientHost(request).or(this.clientHost);
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

        long lifetime = getLifetime(request, configuration.getPutLifetime());
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
                        user,
                        credential.getId(),
                        surls,
                        sizes,
                        wantPermanent,
                        protocols,
                        lifetime,
                        configuration.getGetRetryTimeout(),
                        configuration.getGetMaxNumOfRetries(),
                        clientHost,
                        spaceToken,
                        retentionPolicy,
                        accessLatency,
                        request.getUserRequestDescription());
        try (JDC ignored = r.applyJdc()) {
            String priority = getExtraInfo(request, "priority");
            if (priority != null) {
                try {
                    r.setPriority(Integer.parseInt(priority));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Ignoring non-integer user priority: {}" , priority);
                }
            }

            if (overwriteMode != null) {
                r.setOverwriteMode(overwriteMode);
            }

            srm.schedule(r);
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            return r.getSrmPrepareToPutResponse(configuration.getPutSwitchToAsynchronousModeDelay());
        }
    }

    private static String getExtraInfo(SrmPrepareToPutRequest request, String key)
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

    private static long getLifetime(SrmPrepareToPutRequest request, long max) throws SRMInvalidRequestException
    {
        long lifetimeInSeconds = 0;
        if (request.getDesiredTotalRequestTime() != null) {
            long reqLifetime = (long) request.getDesiredTotalRequestTime().intValue();
            if (reqLifetime < 0) {
                /* [ SRM 2.2, 5.5.2 ]
                 * q)    If input parameter desiredTotalRequestTime is 0 (zero), each file
                 *       request must be tried at least once. Negative value must be invalid.
                 */
                throw new SRMInvalidRequestException("Negative desiredTotalRequestTime is invalid.");
            }
            lifetimeInSeconds = reqLifetime;
        }
        if (lifetimeInSeconds <= 0) {
            // Revisit: Behaviour doesn't match the SRM spec
            return max;
        }
        long lifetime = TimeUnit.SECONDS.toMillis(lifetimeInSeconds);
        return lifetime > max ? max : lifetime;
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
        return Optional.absent();
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
