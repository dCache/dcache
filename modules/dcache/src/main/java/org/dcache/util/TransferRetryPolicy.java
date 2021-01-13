/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Describe the behaviour of a dCache transfer when faced with a dCache-internal
 * error.
 * <p>
 * This class implements a fluent interface by allowing partially defined
 * objects; that is, an object may not have all fields defined.  The intention
 * is that such partially defined objects are used as a stepping-stone to
 * building a completely defined object.  The exception to this rule is that
 * if the policy is to try only once then the pause-before-retry may be
 * omitted.
 */
public final class TransferRetryPolicy
{
    private static final long DUMMY_VALUE = Long.MAX_VALUE;

    private static enum OptionalField {PAUSE, TIMEOUT};

    private final EnumSet<OptionalField> uninitiated;
    private final int tries;
    private final long retryPause;
    private final long timeout;

    private TransferRetryPolicy(int tries, long retryPause, long timeout,
            EnumSet<OptionalField> unassigned)
    {
        this.tries = tries;
        this.retryPause = retryPause;
        this.timeout = timeout;
        uninitiated = EnumSet.copyOf(unassigned);
    }

    private TransferRetryPolicy(int tries)
    {
        this.tries = tries;
        retryPause = DUMMY_VALUE;
        timeout = DUMMY_VALUE;
        uninitiated = EnumSet.allOf(OptionalField.class);
    }

    /**
     * Check that the policy is fully defined.  This is NOT needed to ensure
     * correct behaviour because the getter methods will check fields have been
     * defined.  This method is intended to support fail-fast behaviour.
     */
    public void checkValid() throws IllegalStateException
    {
        checkState(!uninitiated.contains(OptionalField.TIMEOUT),
                "Timeout is not defined");

        if (tries > 1) {
            checkState(!uninitiated.contains(OptionalField.PAUSE),
                    "Retry pause is not defined");
        }
    }

    private void guard(OptionalField field)
    {
        checkState(!uninitiated.contains(field), "Attempt to read"
                + " TransferRetryPolicy field %s before being initialised", field);
    }

    public int getMaximumTries()
    {
        return tries;
    }

    public long getRetryPause()
    {
        if (tries == 1) {
            throw new UnsupportedOperationException("Retry pause called when policy is not to retry.");
        }

        guard(OptionalField.PAUSE);
        return retryPause;
    }

    public long getTimeout()
    {
        guard(OptionalField.TIMEOUT);
        return timeout;
    }

    private TransferRetryPolicy withRetryPause(long millis)
    {
        TransferRetryPolicy newPolicy = new TransferRetryPolicy(tries,
                millis, timeout, uninitiated);
        if (!newPolicy.uninitiated.remove(OptionalField.PAUSE)) {
            throw new UnsupportedOperationException("Not allowed to update retry pause.");
        }
        return newPolicy;
    }

    private TransferRetryPolicy withTimeout(long millis)
    {
        TransferRetryPolicy newPolicy = new TransferRetryPolicy(tries,
                retryPause, millis, uninitiated);
        if (!newPolicy.uninitiated.remove(OptionalField.TIMEOUT)) {
            throw new UnsupportedOperationException("Not allowed to update timeout.");
        }
        return newPolicy;
    }

    /* Create partially initialise objects. */

    public static TransferRetryPolicy tryOnce()
    {
        return new TransferRetryPolicy(1);
    }

    public static TransferRetryPolicy maximumTries(int count)
    {
        checkArgument(count >= 1, "Number of times a transfer is attempted must be positive.");
        return new TransferRetryPolicy(count);
    }

    public static TransferRetryPolicy alwaysRetry()
    {
        return new TransferRetryPolicy(Integer.MAX_VALUE);
    }

    /* Fluent interface for completing objects initialisation. */

    public TransferRetryPolicy timeoutAfter(long millis)
    {
        // REVISIT what should be the minimum timeout?
        checkArgument(millis >= 1, "Timeout for registering a transfer is too short.");
        return withTimeout(millis);
    }

    public TransferRetryPolicy timeoutAfter(long duration, TimeUnit unit)
    {
        checkArgument(duration >= 1, "Timeout for registering a transfer must use a positive number.");
        return withTimeout(unit.toMillis(duration));
    }

    public TransferRetryPolicy doNotTimeout()
    {
        return withTimeout(Long.MAX_VALUE);
    }

    public TransferRetryPolicy pauseBeforeRetrying(long millis)
    {
        // REVISIT what should be the minimum pause period?
        checkArgument(millis >= 1, "Pause before retrying a transfer after internal failure is too short.");
        return withRetryPause(millis);
    }

    public TransferRetryPolicy pauseBeforeRetrying(long duration, TimeUnit unit)
    {
        checkArgument(duration >= 1, "Pause before retrying a transfer after internal failure must use a positive number.");
        return withRetryPause(unit.toMillis(duration));
    }
}
