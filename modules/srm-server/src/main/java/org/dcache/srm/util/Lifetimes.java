package org.dcache.srm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.util.TimeUtils;
import org.dcache.util.TimeUtils.TimeUnitFormat;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *  Utility methods for handling lifetime of requests.
 */
public class Lifetimes
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Lifetimes.class);

    /**
     * Calculate the lifetime of this request.
     * @param requestedLifetime the requested lifetime in seconds, or null if absent from request
     * @param maximumLifetime the maximum allowed lifetime in milliseconds.
     * @return the lifetime of this request in milliseconds
     * @throws SRMInvalidRequestException
     */
    public static long calculateLifetime(Integer requestedLifetime, long maximumLifetime)
            throws SRMInvalidRequestException
    {
        return Lifetimes.calculateLifetime(requestedLifetime, 0, 0, maximumLifetime);
    }

    /**
     * Calculate the lifetime of this request.  If the client supplied lifetime
     * requires an average bandwidth greater than {@literal bandwidth} then
     * an extended lifetime is returned.
     * @param requestedLifetime the requested lifetime in seconds, or null if absent from request
     * @param size the size of the file or zero if no value is supplied
     * @param bandwidth the maximum bandwidth the client may assume for this transfer in kiB/s, or zero if there is no limit
     * @param maximumLifetime the maximum allowed lifetime in milliseconds.
     * @return the lifetime of this request in milliseconds
     * @throws SRMInvalidRequestException
     */
    public static long calculateLifetime(Integer requestedLifetime, long size,
            long bandwidth, long maximumLifetime) throws SRMInvalidRequestException
    {
        long lifetimeInSeconds = (requestedLifetime != null) ? requestedLifetime : 0;

        if (lifetimeInSeconds < 0) {
            /* [ SRM 2.2, 5.2.1 ]
             * m) If input parameter desiredTotalRequestTime is 0 (zero), each file request
             *    must be tried at least once. Negative value must be invalid.
             */
            throw new SRMInvalidRequestException("Negative desiredTotalRequestTime is invalid.");
        } else if (lifetimeInSeconds == 0) {
            // Revisit: Behaviour doesn't match the SRM spec
            return maximumLifetime;
        } else {
            long lifetime = TimeUnit.SECONDS.toMillis(lifetimeInSeconds);
            lifetime = calculateRequestLifetimeWithWorkaround(lifetime, size, bandwidth, maximumLifetime);
            return Math.min(lifetime, maximumLifetime);
        }
    }

    /**
     * Calculate an updated request lifetime that tries to ensure sufficient
     * time to transfer the supplied number of bytes, assuming a minimum
     * average bandwidth.  The supplied lifetime is returned unless this is
     * (possibly) insufficient, in which case the estimated duration is returned.
     * @param lifetime client-supplied request lifetime, in milliseconds
     * @param size the number of bytes to transfer
     * @param bandwidth the maximum bandwidth the client may assume in kiB/s
     * @param maximumLifetime the configured maximum lifetime in milliseconds
     * @return a reasonable request lifetime, in milliseconds
     */
    public static long calculateRequestLifetimeWithWorkaround(long lifetime,
            long size, long bandwidth, long maximumLifetime)
    {
        if (size > 0 && bandwidth > 0) {
            long estimatedDuration = SECONDS.toMillis((size/bandwidth) / 1024L);
            long cappedDuration = Math.min(estimatedDuration, maximumLifetime);
            if (lifetime < cappedDuration) {
                LOGGER.info("Requested lifetime of {} too short to transfer {} bytes; adjusting to {}",
                        TimeUtils.duration(lifetime, MILLISECONDS, TimeUnitFormat.SHORT),
                        size,
                        TimeUtils.duration(cappedDuration, MILLISECONDS, TimeUnitFormat.SHORT));
                lifetime = cappedDuration;
            }
        }
        return lifetime;
    }
}
