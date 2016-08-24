/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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

import com.google.common.io.Closer;

import java.io.Closeable;
import java.nio.channels.CompletionHandler;

import org.dcache.pool.classic.Cancellable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implements an asynchronous variant of the try-with-resource construct of Java 7.
 *
 * The class follows the template pattern often seen in the Spring Framework. A client
 * is supposed to create a (possibly anonymous inner) subclass implementing either
 * the {@code execute} or {@code executeWithCancellable} method.
 *
 * The template implements Cancellable, which may be used to cancel any asynchronous
 * operations started by {@code execute}. The template implements CompletionHandler and
 * {@code execute} or an asynchronous operation started by {@code execute} must use this
 * interface to signal completion.
 *
 * Once the template completes or fails, this is signalled to an injected completion
 * handler.
 *
 * An asynchronous operation started by {@code execute} should be registered by calling
 * {@code setCancellable}. Only then will the template be able to cancel the asynchronous
 * operations. Alternatively a subclass may override {@code executeWithCancellable} and
 * return the Cancellable.
 *
 * {@code execute} may register Closeable resources by calling {@code autoclose}.
 * These are guaranteed to be closed once this {@code TryCatchTemplate} completes.
 * Failure to close any resource is propagated as a failure of the template, unless the
 * template has already failed (in that case the failure to close a resource is added
 * as a suppressed throwable).
 *
 * Any exceptions thrown by {@code execute} are caught and will result in failure
 * of the template.
 *
 * A cancellable registered through {@code setCancellable} will be cancelled if
 * {@code execute} throws an exception or calls {@code failed}, or if the template
 * is cancelled.
 *
 * A subclass may override {@code onSuccess} and {@code onFailure} to add additional
 * processing when the template completes or fails. These are only to be used for
 * light-weight non-blocking operations. The thread on which these are called is
 * unpredictable.
 * */
public abstract class TryCatchTemplate<V, A> implements Cancellable, CompletionHandler<V, A>
{
    private final Closer _closer = Closer.create();
    private final CompletionHandler<V, A> _completionHandler;
    private volatile Cancellable _cancellable;

    public TryCatchTemplate(CompletionHandler<V, A> completionHandler)
    {
        _completionHandler = completionHandler;
        try {
            execute();
        } catch (Throwable t) {
            failed(t, null);
        }
    }

    @Override
    public void cancel(String explanation)
    {
        if (_cancellable != null) {
            _cancellable.cancel(explanation);
        }
    }

    @Override
    public final void completed(V result, A attachment)
    {
        try {
            _closer.close();
            onSuccess(result, attachment);
            _completionHandler.completed(result, attachment);
        } catch (Throwable t) {
            fail(t, attachment);
        }
    }

    @Override
    public final void failed(Throwable exc, A attachment)
    {
        /* This weird looking code is to fulfill the contract of Closer.
         * It ensures that suppressed exceptions from the Closeables are
         * handled correctly.
         */
        try {
            try {
                throw _closer.rethrow(exc, Exception.class);
            } finally {
                _closer.close();
            }
        } catch (Throwable t) {
            fail(t, attachment);
        }
    }

    private void fail(Throwable t, A attachment)
    {
        try {
            onFailure(t, attachment);
        } catch (Exception replacement) {
            if (replacement.getCause() == t) {
                t = replacement;
            } else if (replacement != t) {
                t.addSuppressed(replacement);
            }
        } catch (Throwable suppressed) {
            t.addSuppressed(t);
        }
        _completionHandler.failed(t, attachment);
    }

    /**
     * Registers the given {@code closeable} to be closed when this {@code TryCatchTemplate} completes.
     *
     * @return the given {@code closeable}
     */
    protected <C extends Closeable> C autoclose(C closeable)
    {
        return _closer.register(closeable);
    }

    /**
     * Registers the given {@code cancellable}. When this tempalte is cancelled so is {@code cancellable}.
     *
     * @throws IllegalStateException if called more than once
     */
    protected void setCancellable(Cancellable cancellable)
    {
        checkState(_cancellable == null);
        _cancellable = checkNotNull(cancellable);
    }

    /**
     * Called by the constructor to execute the template.
     *
     * Either {@code completed}, {@code failed}, or {@code setCancellable} should be called.
     *
     * A subclass is expected to override either {@code execute} or {@code executeWithResult}.
     */
    protected void execute() throws Exception
    {
        setCancellable(executeWithCancellable());
    }

    /**
     * Like {@code execute} but allows a cancellable to be returned.
     *
     * An implementation should not call {@code completed} or {@code setCancellable} from within
     * {@code executeWithCancellable}.
     */
    protected Cancellable executeWithCancellable() throws Exception
    {
        return null;
    }

    /**
     * Invoked with the result of the execution when it is successful.
     *
     * If an {@code Exception} is thrown, the template fails. Otherwise the template succeeds.
     *
     * Must not call {@code completed} or {@code failed}.
     */
    protected void onSuccess(V result, A attachment)
            throws Exception
    {
    }

    /**
     * Invoked when the execution fails or is canceled.
     *
     * If an {@code Exception} is thrown of which {@code t} is the cause, that exception
     * is used to fail this template. Thus an implementation may replace the reason the
     * template fails by throwing a new exception with {@code t} set as the cause.
     *
     * Otherwise the template fails with {@code t}. Any other exception thrown by this
     * method is suppressed.
     *
     * Must not call {@code completed} or {@code failed}.
     */
    protected void onFailure(Throwable t, A attachment)
            throws Exception
    {
    }
}
