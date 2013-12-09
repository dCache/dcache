package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMProtocol;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.SrmCopyRequest;
import org.dcache.srm.v2_2.SrmCopyResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TCopyFileRequest;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmCopy
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmCopy.class);

    private final SrmCopyRequest request;
    private SrmCopyResponse response;
    private final SRMUser user;
    private final SRM srm;
    private final RequestCredential credential;
    private final Configuration configuration;
    private String clientHost;

    public SrmCopy(SRMUser user,
                   RequestCredential credential,
                   SrmCopyRequest request,
                   AbstractStorageElement storage,
                   SRM srm,
                   String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.credential = checkNotNull(credential);
        this.configuration = srm.getConfiguration();
        this.clientHost = clientHost;
        this.srm = checkNotNull(srm);
    }

    public SrmCopyResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmCopy();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMNotSupportedException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_NOT_SUPPORTED);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            }
        }
        return response;
    }

    private SrmCopyResponse srmCopy()
            throws SRMInvalidRequestException, SRMNotSupportedException,
                   SRMInternalErrorException
    {
        TCopyFileRequest[] arrayOfFileRequests = getFileRequests(request);
        long lifetime = getTotalRequestTime(request, configuration.getCopyLifetime());
        String spaceToken = request.getTargetSpaceToken();

        URI from_urls[] = new URI[arrayOfFileRequests.length];
        URI to_urls[] = new URI[arrayOfFileRequests.length];
        for (int i = 0; i < arrayOfFileRequests.length; i++) {
            from_urls[i] = URI.create(arrayOfFileRequests[i].getSourceSURL().toString());
            to_urls[i] = URI.create(arrayOfFileRequests[i].getTargetSURL().toString());
        }

        TRetentionPolicy targetRetentionPolicy = null;
        TAccessLatency targetAccessLatency = null;
        if (request.getTargetFileRetentionPolicyInfo() != null) {
            targetRetentionPolicy = request.getTargetFileRetentionPolicyInfo().getRetentionPolicy();
            targetAccessLatency = request.getTargetFileRetentionPolicyInfo().getAccessLatency();
        }

        TOverwriteMode overwriteMode = getOverwriteMode(request);

        CopyRequest r = new CopyRequest(
                user,
                credential.getId(),
                from_urls,
                to_urls,
                spaceToken,
                lifetime,
                configuration.getCopyRetryTimeout(),
                configuration.getCopyMaxNumOfRetries(),
                SRMProtocol.V2_1,  // Revisit: v2.1?
                request.getTargetFileStorageType(),
                targetRetentionPolicy,
                targetAccessLatency,
                request.getUserRequestDescription(),
                clientHost,
                overwriteMode);
        try (JDC ignored = r.applyJdc()) {
            String priority = getExtraInfo(request, "priority");
            if (priority != null) {
                try {
                    r.setPriority(Integer.parseInt(priority));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Ignoring non-integer priority value: {}", priority);
                }
            }
            srm.schedule(r);
            return r.getSrmCopyResponse();
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (IllegalStateTransition e) {
            throw new SRMInternalErrorException("Scheduling failure", e);
        }
    }

    private static String getExtraInfo(SrmCopyRequest request, String key)
    {
        ArrayOfTExtraInfo sourceStorageSystemInfo = request.getSourceStorageSystemInfo();
        if (sourceStorageSystemInfo == null) {
            return null;
        }
        TExtraInfo[] extraInfoArray = sourceStorageSystemInfo.getExtraInfoArray();
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

    private static TOverwriteMode getOverwriteMode(SrmCopyRequest request) throws SRMNotSupportedException
    {
        TOverwriteMode overwriteMode = request.getOverwriteOption();
        if (overwriteMode != null && overwriteMode.equals(TOverwriteMode.WHEN_FILES_ARE_DIFFERENT)) {
            throw new SRMNotSupportedException("Overwrite Mode WHEN_FILES_ARE_DIFFERENT is not supported");
        }
        return overwriteMode;
    }

    private static long getTotalRequestTime(SrmCopyRequest request, long max) throws SRMInvalidRequestException
    {
        long lifetimeInSeconds = 0;
        if (request.getDesiredTotalRequestTime() != null) {
            long reqLifetime = (long) request.getDesiredTotalRequestTime().intValue();
            /* [ SRM 2.2, 5.7.2 ]
             *
             * o)    If input parameter desiredTotalRequestTime is 0 (zero),
             *       each file request must be tried at least once. Negative
             *       value must be invalid.
             */
            if (reqLifetime < 0) {
                throw new SRMInvalidRequestException("Negative desiredTotalRequestTime is invalid");
            }
            lifetimeInSeconds = reqLifetime;
        }

        if (lifetimeInSeconds <= 0) {
            /* FIXME: This is not spec compliant. */
            return max;
        }
        return Math.min(TimeUnit.SECONDS.toMillis(lifetimeInSeconds), max);
    }

    private static TCopyFileRequest[] getFileRequests(SrmCopyRequest request) throws SRMInvalidRequestException
    {
        if (request.getArrayOfFileRequests() == null) {
            throw new SRMInvalidRequestException("ArrayOfFileRequests is null");
        }
        TCopyFileRequest[] arrayOfFileRequests =
                request.getArrayOfFileRequests().getRequestArray();
        if (arrayOfFileRequests == null) {
            throw new SRMInvalidRequestException("null array of file requests");
        }
        if (arrayOfFileRequests.length == 0) {
            throw new SRMInvalidRequestException("empty array of file requests");
        }
        return arrayOfFileRequests;
    }

    public static final SrmCopyResponse getFailedResponse(String text)
    {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmCopyResponse getFailedResponse(String text, TStatusCode statusCode)
    {
        SrmCopyResponse response = new SrmCopyResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}
