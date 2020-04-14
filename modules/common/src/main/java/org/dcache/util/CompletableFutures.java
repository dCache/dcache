/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 - 2020 Deutsches Elektronen-Synchrotron
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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Helper class to handle CompletableFuture.
 */
public class CompletableFutures {

    private CompletableFutures() {
        // no instance allowed
    }


    /**
     * A ListenableFuture that is wrapped around CompletableFuture.
     * @param <T> The result type returned by this Future's {@code get} method.
     */
    private static class ListenableFutureImpl<T> extends AbstractFuture<T>
            implements ListenableFuture<T>, BiConsumer<T, Throwable> {

        private final CompletableFuture<T> inner;

        public ListenableFutureImpl(CompletableFuture<T> inner) {
            this.inner = inner;
            inner.whenComplete(this);
        }

        @Override
        public void accept(T value, Throwable throwable) {
            if (throwable != null) {
                if (throwable instanceof CancellationException) {
                    // interruption flag is not propagated. just be on the safe side...
                    cancel(false);
                } else {
                    setException(throwable);
                }
            } else {
                set(value);
            }
        }
    }

    /**
     * Create a ListenableFuture from  java 8 CompletableFuture.
     *
     * @param completable ListenableFuture to convert.
     * @return new ListenableFuture.
     * @param <T> The result type returned by this Future's {@code get} method
     */
    public static <T> ListenableFuture<T> fromCompletableFuture(CompletableFuture<T> completable) {
        return new ListenableFutureImpl<>(completable);
    }

    /**
     * Create a CompletableFuture from guava's ListenableFuture to
     * help migration from Guava to Java8.
     * @param listenable ListenableFuture to convert.
     * @return new CompletableFuture.
     */
    public static <T> CompletableFuture<T> fromListenableFuture(ListenableFuture<T> listenable) {

        final CompletableFuture<T> completable = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // propagate cancel to the listenable future
                boolean result = listenable.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };

        // propagate results to completable future
        Futures.addCallback(listenable, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completable.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return completable;
    }
}
