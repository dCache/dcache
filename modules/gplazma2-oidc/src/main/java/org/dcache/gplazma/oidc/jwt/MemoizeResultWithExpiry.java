/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc.jwt;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.dcache.util.Result;

/**
 * Provide access to a Result, obtained from some supplier where the value is cached for a configurable
 * duration.  The cached duration may be different depending on whether the Result is successful.
 */
public class MemoizeResultWithExpiry<C extends Result<?, ?>> implements Supplier<C> {

    private final Supplier<C> supplier;
    private final Duration whenSuccess;
    private final Duration whenFailure;

    private C value;
    private Instant nextCheck;

    public MemoizeResultWithExpiry(Supplier<C> supplier, Duration whenSuccess,
          Duration whenFailure) {
        this.supplier = supplier;
        this.whenFailure = whenFailure;
        this.whenSuccess = whenSuccess;
    }

    @Override
    public synchronized C get() {
        Instant now = Instant.now();
        if (nextCheck == null || now.isAfter(nextCheck)) {
            value = supplier.get();
            Duration cacheDuration = (value == null || value.isFailure())
                  ? whenFailure : whenSuccess;
            nextCheck = now.plus(cacheDuration);
        }
        return value;
    }

    public static <C extends Result<?, ?>> Builder memorize(Supplier<C> supplier) {
        return new Builder(supplier);
    }

    public static class Builder<C extends Result<?, ?>> {

        private final Supplier<C> supplier;
        private Duration whenSuccess;
        private Duration whenFailure;

        public Builder(Supplier<C> supplier) {
            this.supplier = supplier;
        }

        public Builder whenFailureFor(Duration duration) {
            whenFailure = duration;
            return this;
        }

        public Builder whenSuccessFor(Duration duration) {
            whenSuccess = duration;
            return this;
        }

        public MemoizeResultWithExpiry build() {
            return new MemoizeResultWithExpiry(supplier, whenSuccess, whenFailure);
        }
    }
}
