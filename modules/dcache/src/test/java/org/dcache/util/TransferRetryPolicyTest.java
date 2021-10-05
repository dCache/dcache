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

import static org.dcache.util.TransferRetryPolicy.alwaysRetry;
import static org.dcache.util.TransferRetryPolicy.maximumTries;
import static org.dcache.util.TransferRetryPolicy.tryOnce;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class TransferRetryPolicyTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectZeroTries() {
        maximumTries(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectZeroTimeout() {
        alwaysRetry().timeoutAfter(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectZeroTimeoutUnit() {
        alwaysRetry().timeoutAfter(0, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectZeroPauseBeforeRetrying() {
        alwaysRetry().pauseBeforeRetrying(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectZeroUnitPauseBeforeRetrying() {
        alwaysRetry().pauseBeforeRetrying(0, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDenyReadWhenTimeoutUninitiated() {
        // Missing timeout
        TransferRetryPolicy partial = tryOnce();

        partial.getTimeout();
    }

    @Test
    public void shouldBuildTryOnceDoNotTimeoutPolicy() {
        TransferRetryPolicy policy = tryOnce().doNotTimeout();

        policy.checkValid();
        assertThat(policy.getMaximumTries(), is(equalTo(1)));
        assertThat(policy.getTimeout(), is(equalTo(Long.MAX_VALUE)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldDenyReadRetryPauseForTryOncePolicy() {
        TransferRetryPolicy policy = tryOnce().doNotTimeout();

        policy.getRetryPause();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDenyReadTimeoutWhenNotDefined() {
        // Partial policy with missing timeout definition
        TransferRetryPolicy partial = alwaysRetry().pauseBeforeRetrying(1_000);

        partial.getTimeout();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldBeInvalidWhenTimeoutNotDefined() {
        // Partial policy with missing timeout definition
        TransferRetryPolicy partial = alwaysRetry().pauseBeforeRetrying(1_000);

        partial.checkValid();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDenyReadRetryPauseWhenNotDefined() {
        // Partial policy with missing pause-before-retrying
        TransferRetryPolicy partial = alwaysRetry().timeoutAfter(1_000);

        partial.getRetryPause();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldBeInvalidWhenRetryPauseNotDefined() {
        // Partial policy with missing pause-before-retrying
        TransferRetryPolicy partial = alwaysRetry().timeoutAfter(1_000);

        partial.checkValid();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowUpdateRetryPause() {
        TransferRetryPolicy policy = alwaysRetry().pauseBeforeRetrying(2_000).doNotTimeout();

        policy.pauseBeforeRetrying(1_000);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowUpdateTimeout() {
        TransferRetryPolicy policy = alwaysRetry().pauseBeforeRetrying(2_000).doNotTimeout();

        policy.timeoutAfter(1_000);
    }

    @Test
    public void shouldBuildLimitedRetriesAndRetryPauseAndDoNotTimeoutPolicy() {
        TransferRetryPolicy policy = maximumTries(10).pauseBeforeRetrying(1_000)
              .doNotTimeout();

        policy.checkValid();
        assertThat(policy.getMaximumTries(), is(equalTo(10)));
        assertThat(policy.getRetryPause(), is(equalTo(1_000L)));
        assertThat(policy.getTimeout(), is(equalTo(Long.MAX_VALUE)));
    }

    @Test
    public void shouldBuildKeepRetryingAndRetryPauseAndDoNotTimeoutPolicy() {
        TransferRetryPolicy policy = alwaysRetry().pauseBeforeRetrying(1_000)
              .doNotTimeout();

        policy.checkValid();
        assertThat(policy.getMaximumTries(), is(equalTo(Integer.MAX_VALUE)));
        assertThat(policy.getRetryPause(), is(equalTo(1_000L)));
        assertThat(policy.getTimeout(), is(equalTo(Long.MAX_VALUE)));
    }

    @Test
    public void shouldBuildKeepRetryingAndRetryPauseAndTimeUnitDoNotTimeoutPolicy() {
        TransferRetryPolicy policy = alwaysRetry()
              .pauseBeforeRetrying(1, TimeUnit.SECONDS)
              .doNotTimeout();

        policy.checkValid();
        assertThat(policy.getMaximumTries(), is(equalTo(Integer.MAX_VALUE)));
        assertThat(policy.getRetryPause(), is(equalTo(1_000L)));
        assertThat(policy.getTimeout(), is(equalTo(Long.MAX_VALUE)));
    }

    @Test
    public void shouldBuildKeepRetryingAndRetryPauseAndTimeout() {
        TransferRetryPolicy policy = alwaysRetry().pauseBeforeRetrying(1_000)
              .timeoutAfter(2_000);

        policy.checkValid();
        assertThat(policy.getMaximumTries(), is(equalTo(Integer.MAX_VALUE)));
        assertThat(policy.getRetryPause(), is(equalTo(1_000L)));
        assertThat(policy.getTimeout(), is(equalTo(2_000L)));
    }

    @Test
    public void shouldBuildKeepRetryingAndRetryPauseAndTimeoutUnit() {
        TransferRetryPolicy policy = alwaysRetry().pauseBeforeRetrying(1_000)
              .timeoutAfter(2, TimeUnit.SECONDS);

        policy.checkValid();
        assertThat(policy.getMaximumTries(), is(equalTo(Integer.MAX_VALUE)));
        assertThat(policy.getRetryPause(), is(equalTo(1_000L)));
        assertThat(policy.getTimeout(), is(equalTo(2_000L)));
    }
}
