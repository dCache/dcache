/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public class Callables
{
    private Callables()
    {
    }

    /**
     * Returns a Caller that caches the instance returned by the delegate and
     * removes the cached value after the specified time has passed. Subsequent
     * calls to {@code call()} return the cached value if the expiration time has
     * not passed. After the expiration time, a new value is retrieved, cached,
     * and returned.
     *
     * <p>The returned Caller is thread-safe.
     */
    public static <T> Callable<T> memoizeWithExpiration(
            Callable<T> delegate, long duration, TimeUnit unit)
    {
        return new ExpiringMemoizingCallable<>(delegate, duration, unit);
    }

    /**
     * Returns a Caller that caches the instance returned by the delegate and
     * removes the cached value when any of the supplied files have been modified.
     * Subsequent calls to {@code call()} return the cached value if files have
     * not been modified since the cached value was retrieved. If the files have
     * been modified, a new value is retrieved.
     *
     * <p>The returned Caller is thread-safe.
     */
    public static <T> Callable<T> memoizeFromFiles(Callable<T> delegate, Path... files)
    {
        return new MemoizingCallableFromFiles<>(delegate, files);
    }

    /**
     * Adapted from com.google.common.base.Suppliers.
     *
     * Copyright (C) 2007 The Guava Authors
     */
    private static class ExpiringMemoizingCallable<T> implements Callable<T>
    {
        final Callable<T> delegate;
        final long durationNanos;
        transient volatile T value;
        // The special value 0 means "not yet initialized".
        transient volatile long expirationNanos;

        ExpiringMemoizingCallable(
                Callable<T> delegate, long duration, TimeUnit unit)
        {
            Preconditions.checkArgument(duration > 0);
            this.delegate = Preconditions.checkNotNull(delegate);
            this.durationNanos = unit.toNanos(duration);
        }

        @Override public T call() throws Exception
        {
            // Another variant of Double Checked Locking.
            //
            // We use two volatile reads.  We could reduce this to one by
            // putting our fields into a holder class, but (at least on x86)
            // the extra memory consumption and indirection are more
            // expensive than the extra volatile reads.
            long nanos = expirationNanos;
            long now = System.nanoTime();
            if (nanos == 0 || now - nanos >= 0) {
                synchronized (this) {
                    if (nanos == expirationNanos) {  // recheck for lost race
                        T t = delegate.call();
                        value = t;
                        nanos = now + durationNanos;
                        // In the very unlikely event that nanos is 0, set it to 1;
                        // no one will notice 1 ns of tardiness.
                        expirationNanos = (nanos == 0) ? 1 : nanos;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override public String toString() {
            // This is a little strange if the unit the user provided was not NANOS,
            // but we don't want to store the unit just for toString
            return "Suppliers.memoizeWithExpiration(" + delegate + ", " +
                   durationNanos + ", NANOS)";
        }
    }

    private static class MemoizingCallableFromFiles<T> implements Callable<T>
    {
        final Path[] files;
        final Callable<T> delegate;

        FileTime lastLastModifiedTime;
        T value;

        public MemoizingCallableFromFiles(Callable<T> delegate, Path[] files)
        {
            this.delegate = delegate;
            this.files = files;
        }

        @Override
        public T call() throws Exception
        {
            FileTime lastModified = getLastModifiedTime();
            synchronized (this) {
                if (lastModified == null || lastLastModifiedTime == null ||
                    lastModified.compareTo(lastLastModifiedTime) > 0) {
                    value = delegate.call();
                    lastLastModifiedTime = lastModified;
                }
                return value;
            }
        }

        private FileTime getLastModifiedTime()
        {
            try {
                FileTime[] times = new FileTime[files.length];
                for (int i = 0; i < files.length; i++) {
                    times[i] = java.nio.file.Files.getLastModifiedTime(files[i]);
                }
                return Ordering.natural().max(asList(times));
            } catch (IOException e) {
                return null;
            }
        }
    }
}
