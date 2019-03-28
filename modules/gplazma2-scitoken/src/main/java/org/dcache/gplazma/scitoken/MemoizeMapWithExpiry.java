/* dCache - http://www.dcache.org/
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
package org.dcache.gplazma.scitoken;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Provide access to a Map, obtained from some supplier where the value is
 * cached for a configurable duration.  The cached duration may be different
 * depending on whether the Map is empty.
 */
public class MemoizeMapWithExpiry<C extends Map<?,?>> implements Supplier<C>
{
    private final Supplier<C> supplier;
    private final Duration whenNonEmpty;
    private final Duration whenEmpty;

    private C value;
    private Instant nextCheck;

    public MemoizeMapWithExpiry(Supplier<C> supplier, Duration whenNonEmpty,
            Duration whenEmpty)
    {
        this.supplier = supplier;
        this.whenEmpty = whenEmpty;
        this.whenNonEmpty = whenNonEmpty;
    }

    @Override
    public synchronized C get()
    {
        Instant now = Instant.now();
        if (nextCheck == null || now.isAfter(nextCheck)) {
            value = supplier.get();
            Duration cacheDuration = (value == null || value.isEmpty())
                    ? whenEmpty : whenNonEmpty;
            nextCheck = now.plus(cacheDuration);
        }
        return value;
    }

    public static <C extends Map<?,?>> Builder memorize(Supplier<C> supplier)
    {
        return new Builder(supplier);
    }

    public static class Builder<C extends Map<?,?>>
    {
        private final Supplier<C> supplier;
        private Duration whenNonEmpty;
        private Duration whenEmpty;

        public Builder(Supplier<C> supplier)
        {
            this.supplier = supplier;
        }

        public Builder whenEmptyFor(Duration duration)
        {
            whenEmpty = duration;
            return this;
        }

        public Builder whenNonEmptyFor(Duration duration)
        {
            whenNonEmpty = duration;
            return this;
        }

        public MemoizeMapWithExpiry build()
        {
            return new MemoizeMapWithExpiry(supplier, whenNonEmpty, whenEmpty);
        }
    }
}
