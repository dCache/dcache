/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package dmg.util;

import org.junit.Test;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class CpuUsageTest
{
    /**
     * Fluent builder class for CpuUsage instances.
     */
    private static class CpuUsageBuilder
    {
        private Duration user;
        private Duration system;

        public CpuUsageBuilder withUser(long amount, TemporalUnit unit)
        {
            return this.withUser(Duration.of(amount, unit));
        }

        public CpuUsageBuilder withUser(Duration duration)
        {
            user = requireNonNull(duration);
            return this;
        }

        public CpuUsageBuilder withSystem(long amount, TemporalUnit unit)
        {
            return this.withSystem(Duration.of(amount, unit));
        }

        public CpuUsageBuilder withSystem(Duration duration)
        {
            system = requireNonNull(duration);
            return this;
        }

        public CpuUsage build()
        {
            checkState(user != null, "Must specify user duration");
            checkState(system != null, "Must specify system duration");
            return new CpuUsage(system, user);
        }
    }

    private CpuUsage usage;

    @Test
    public void shouldInitiallyHoldZeroDurations()
    {
        usage = new CpuUsage();

        assertThat(usage.getSystem(), is(equalTo(Duration.ZERO)));
        assertThat(usage.getUser(), is(equalTo(Duration.ZERO)));
        assertThat(usage.getTotal(), is(equalTo(Duration.ZERO)));
    }

    @Test
    public void shouldHoldZeroDurationWhenCreationWithExplicitZeros()
    {
        given(cpuUsage().withSystem(Duration.ZERO).withUser(Duration.ZERO));

        assertThat(usage.getSystem(), is(equalTo(Duration.ZERO)));
        assertThat(usage.getUser(), is(equalTo(Duration.ZERO)));
        assertThat(usage.getTotal(), is(equalTo(Duration.ZERO)));
    }

    @Test
    public void shouldHoldNonZeroDurationWhenCreationWithNonZeroDurations()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        assertThat(usage.getSystem(), is(equalTo(Duration.of(2, SECONDS))));
        assertThat(usage.getUser(), is(equalTo(Duration.of(3, SECONDS))));
        assertThat(usage.getTotal(), is(equalTo(Duration.of(5, SECONDS))));
    }

    @Test
    public void shouldAcceptValidNewDurations()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        when(advanceTo(cpuUsage().withSystem(3, SECONDS).withUser(5, SECONDS)));

        assertThat(usage.getSystem(), is(equalTo(Duration.of(3, SECONDS))));
        assertThat(usage.getUser(), is(equalTo(Duration.of(5, SECONDS))));
        assertThat(usage.getTotal(), is(equalTo(Duration.of(8, SECONDS))));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectNewDurationsWithRetrogradeSystem()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        when(advanceTo(cpuUsage().withSystem(1, SECONDS).withUser(5, SECONDS)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectNewDurationsWithRetrogradeUser()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        when(advanceTo(cpuUsage().withSystem(3, SECONDS).withUser(1, SECONDS)));
    }

    @Test
    public void shouldAcceptValidIncrease()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        when(increaseBy(cpuUsage().withSystem(1, SECONDS).withUser(2, SECONDS)));

        assertThat(usage.getSystem(), is(equalTo(Duration.of(3, SECONDS))));
        assertThat(usage.getUser(), is(equalTo(Duration.of(5, SECONDS))));
        assertThat(usage.getTotal(), is(equalTo(Duration.of(8, SECONDS))));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectNegativeSystemIncrease()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        when(increaseBy(cpuUsage().withSystem(-1, SECONDS).withUser(2, SECONDS)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectNegativeUserIncrease()
    {
        given(cpuUsage().withSystem(2, SECONDS).withUser(3, SECONDS));

        when(increaseBy(cpuUsage().withSystem(1, SECONDS).withUser(-2, SECONDS)));
    }

    private static CpuUsageBuilder cpuUsage()
    {
        return new CpuUsageBuilder();
    }

    private static Consumer<CpuUsage> advanceTo(CpuUsageBuilder builder)
    {
        return u -> u.advanceTo(builder.build());
    }

    private static Consumer<CpuUsage> increaseBy(CpuUsageBuilder builder)
    {
        return u -> u.increaseBy(builder.build());
    }

    private void given(CpuUsageBuilder builder)
    {
        usage = builder.build();
    }

    private void when(Consumer<CpuUsage> operation)
    {
        operation.accept(usage);
    }
}
