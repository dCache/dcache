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
package org.dcache.gplazma.omnisession;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;

/**
 * This implementation of Clock tracks some external clock, but allows for
 * an offset.  Unlike {@ref Clock#offset}, with this implementation the
 * offset may be adjusted over time, simulating the passing of time.
 */
public class AdvanceableClock extends Clock
{
    private final Clock inner;
    private Duration offset = Duration.ZERO;

    public AdvanceableClock(Clock inner)
    {
        this.inner = inner;
    }

    public AdvanceableClock(Clock inner, Duration offset)
    {
        this.inner = inner;
        this.offset = offset;
    }

    @Override
    public ZoneId getZone() {
        return inner.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new AdvanceableClock(inner.withZone(zone), offset);
    }

    @Override
    public Instant instant() {
        return inner.instant().plus(offset);
    }

    public void advance(int value, TemporalUnit unit)
    {
        Duration delta = Duration.of(value, unit);
        offset = offset.plus(delta);
    }
}
