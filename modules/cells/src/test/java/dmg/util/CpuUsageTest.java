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

import static org.junit.Assert.*;

public class CpuUsageTest
{
    private static class CpuUsageBuilder
    {
        private CpuUsage usage;

        public CpuUsageBuilder withUser(Duration duration)
        {
            usage.addUser(duration);
            return this;
        }

        public CpuUsageBuilder withCombined(Duration duration)
        {
            usage.addCombined(duration);
            return this;
        }

        public CpuUsage build()
        {
            return null;
        }
    }

    private CpuUsage usage;

    @Test
    public void shouldNotFailAssertAfterCreation()
    {
        given(cpuUsage());

        usage.assertValues();
    }

    @Test
    public void shouldNotFailAssertWithUser()
    {
        given(cpuUsage().withUser(Duration.ofSeconds(1)));

        usage.assertValues();
    }

    private static CpuUsageBuilder cpuUsage()
    {
        return new CpuUsageBuilder();
    }

    private void given(CpuUsageBuilder builder)
    {
        usage = builder.build();
    }

    private CpuUsageBuilder givenCpuUsage()
    {
        return new CpuUsageBuilder();
    }
}
