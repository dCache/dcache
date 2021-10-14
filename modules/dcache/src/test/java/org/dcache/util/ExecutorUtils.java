/*
 * dCache - http://www.dcache.org/
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

import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecutorUtils {

    /**
     * This works by injecting a test job (a canary) into the executor.
     * <p/>
     * The executor <b>must</b> be single-threaded, so that the canary is only executed after all
     * existing jobs have completed.
     *
     * @param executor
     * @throws InterruptedException
     */
    public static void waitUntilQuiescent(ExecutorService executor)
          throws InterruptedException {
        Future canary = executor.submit(() -> {
        });
        try {
            canary.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throwIfUnchecked(cause);
            throw new RuntimeException(cause);
        } catch (TimeoutException e) {
            fail("Component with executor took too long to go quiet");
        }
    }
}
