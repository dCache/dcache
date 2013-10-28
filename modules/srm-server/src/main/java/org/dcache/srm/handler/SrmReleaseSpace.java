package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SrmReleaseSpaceCallback;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmReleaseSpaceRequest;
import org.dcache.srm.v2_2.SrmReleaseSpaceResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class SrmReleaseSpace
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmReleaseSpace.class);
    private static final long TIMEOUT = MINUTES.toMillis(1);

    private final AbstractStorageElement storage;
    private final SrmReleaseSpaceRequest request;
    private final SRMUser user;
    private SrmReleaseSpaceResponse response;

    public SrmReleaseSpace(SRMUser user,
                           RequestCredential credential,
                           SrmReleaseSpaceRequest request,
                           AbstractStorageElement storage,
                           SRM srm,
                           String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
    }

    public SrmReleaseSpaceResponse getResponse()
    {
        if (response == null) {
            response = releaseSpace();
        }
        return response;
    }

    private SrmReleaseSpaceResponse releaseSpace()
    {
        String token = request.getSpaceToken();
        Callback callback = new Callback();
        storage.srmReleaseSpace(user, token, null, callback);
        TReturnStatus status = callback.waitResult(TIMEOUT);
        return new SrmReleaseSpaceResponse(status);
    }

    private class Callback implements SrmReleaseSpaceCallback
    {
        private final CountDownLatch latch = new CountDownLatch(1);
        private TReturnStatus status;

        public TReturnStatus waitResult(long timeout)
        {
            try {
                if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    return new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, "Operation timed out");
                }
            } catch (InterruptedException e) {
                return new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, "Operation interrupted");
            }
            return status;
        }

        private void completed(TReturnStatus status)
        {
            this.status = status;
            latch.countDown();
        }

        @Override
        public void failed(String reason)
        {
            completed(new TReturnStatus(TStatusCode.SRM_FAILURE, reason));
        }

        @Override
        public void internalError(String reason)
        {
            completed(new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, reason));
        }

        @Override
        public void invalidRequest(String reason)
        {
            completed(new TReturnStatus(TStatusCode.SRM_INVALID_REQUEST, reason));
        }

        @Override
        public void success(String spaceReservationToken, long remainingSpaceSize)
        {
            completed(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
        }
    }

    public static SrmReleaseSpaceResponse getFailedResponse(String text)
    {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static SrmReleaseSpaceResponse getFailedResponse(String text, TStatusCode statusCode)
    {
        return new SrmReleaseSpaceResponse(new TReturnStatus(statusCode, text));
    }
}
