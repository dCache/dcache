package org.dcache.srm.handler;

import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmReserveSpaceRequest;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SrmReserveSpace
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmReserveSpace.class);
    public static final int MAX_NUMBER_OF_RETRIES = 3;

    private final SrmReserveSpaceRequest request;
    private final SRMUser user;
    private final RequestCredential credential;
    private final Configuration configuration;
    private final String client_host;
    private SrmReserveSpaceResponse response;
    private final SRM srm;

    public SrmReserveSpace(SRMUser user,
                           RequestCredential credential,
                           SrmReserveSpaceRequest request,
                           AbstractStorageElement storage,
                           SRM srm,
                           String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.credential = checkNotNull(credential);
        this.configuration = checkNotNull(srm.getConfiguration());
        this.client_host = checkNotNull(clientHost);
        this.srm = checkNotNull(srm);
    }

    public SrmReserveSpaceResponse getResponse()
    {
        if (response == null) {
            try {
                response = reserveSpace();
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_FAILURE);
            }
        }
        return response;
    }

    private SrmReserveSpaceResponse reserveSpace()
            throws SRMException
    {
        TRetentionPolicyInfo retentionPolicyInfo = request.getRetentionPolicyInfo();
        if (retentionPolicyInfo == null) {
            throw new SRMInvalidRequestException("retentionPolicyInfo is missing");
        }
        TRetentionPolicy retentionPolicy = retentionPolicyInfo.getRetentionPolicy();
        if (retentionPolicy == null) {
            throw new SRMInvalidRequestException("retentionPolicy is missing");
        }
        TAccessLatency accessLatency = retentionPolicyInfo.getAccessLatency();
        UnsignedLong size = request.getDesiredSizeOfGuaranteedSpace();
        String userSpaceTokenDescription = request.getUserSpaceTokenDescription();
        long lifetime = getDesiredLifetimeOfReservedSpace(request, configuration.getDefaultSpaceLifetime());
        long requestLifetime = getRequestLifetime(lifetime);
        try {
            ReserveSpaceRequest reserveRequest =
                    new ReserveSpaceRequest(
                            credential.getId(),
                            user,
                            requestLifetime,
                            MAX_NUMBER_OF_RETRIES,
                            size.longValue(),
                            lifetime,
                            retentionPolicy,
                            accessLatency,
                            userSpaceTokenDescription,
                            client_host);
            reserveRequest.applyJdc();
            srm.schedule(reserveRequest);
            return reserveRequest.getSrmReserveSpaceResponse();
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted");
        } catch (IllegalStateTransition e) {
            LOGGER.error("Failed to schedule srmReserveSpace: {}", e);
            throw new SRMException("Failed to schedule operation");
        }
    }

    private static long getRequestLifetime(long lifetime)
    {
        //make reserve request lifetime no longer than 24 hours
        //request lifetime is different from space reservation lifetime
        if (lifetime == -1 || lifetime > DAYS.toMillis(1)) {
            return DAYS.toMillis(1);
        } else {
            return lifetime;
        }
    }

    private static long getDesiredLifetimeOfReservedSpace(SrmReserveSpaceRequest request, long defaultLifetime)
            throws SRMInvalidRequestException
    {
        Integer desiredLifetimeOfReservedSpace = request.getDesiredLifetimeOfReservedSpace();
        long lifetime;
        if (desiredLifetimeOfReservedSpace == null) {
            lifetime = SECONDS.toMillis(defaultLifetime);
        } else if (desiredLifetimeOfReservedSpace == -1) {
            lifetime = -1;
        } else if (desiredLifetimeOfReservedSpace > 0) {
            lifetime = SECONDS.toMillis(desiredLifetimeOfReservedSpace);
        } else {
            throw new SRMInvalidRequestException("Invalid lifetime: " + desiredLifetimeOfReservedSpace);
        }
        return lifetime;
    }

    public static final SrmReserveSpaceResponse getFailedResponse(String text)
    {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmReserveSpaceResponse getFailedResponse(String text, TStatusCode statusCode)
    {
        SrmReserveSpaceResponse response = new SrmReserveSpaceResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}
