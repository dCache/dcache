package org.dcache.cells;

import com.google.common.base.Function;
import com.google.common.collect.ObjectArrays;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.CacheExceptionFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Stub class for common cell communication patterns. An instance
 * of the template class encapsulates properties such as the
 * destination and timeout for communication.
 *
 * Operations are aware of the dCache Message class and the
 * CacheException class, and are able to interpret dCache error
 * messages.
 */
public class CellStub
    implements CellMessageSender
{
    private CellEndpoint _endpoint;
    private CellPath _destination;
    private long _timeout = 30000;
    private TimeUnit _timeoutUnit = MILLISECONDS;
    private CellEndpoint.SendFlag[] _flags = {};
    private volatile Semaphore _concurrency = new UnlimitedSemaphore();
    private volatile RateLimiter _rateLimiter = RateLimiter.create(Double.POSITIVE_INFINITY);

    public CellStub()
    {
    }

    public CellStub(CellEndpoint endpoint)
    {
        setCellEndpoint(endpoint);
    }

    public CellStub(CellEndpoint endpoint, CellPath destination)
    {
        this(endpoint);
        setDestinationPath(destination);
    }

    public CellStub(CellEndpoint endpoint, CellPath destination, long timeout)
    {
        this(endpoint, destination, timeout, MILLISECONDS);
    }

    public CellStub(CellEndpoint endpoint, CellPath destination, long timeout, TimeUnit unit)
    {
        this(endpoint, destination);
        setTimeout(timeout);
        setTimeoutUnit(unit);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
    }

    public void setDestination(String destination)
    {
        setDestinationPath(new CellPath(destination));
    }

    public void setDestinationPath(CellPath destination)
    {
        _destination = destination;
    }

    public CellPath getDestinationPath()
    {
        return _destination;
    }

    /**
     * Sets the communication timeout of the stub.
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(long timeout)
    {
        _timeout = timeout;
    }

    /**
     * Returns the communication timeout of the stub.
     */
    public long getTimeout()
    {
        return _timeout;
    }

    public void setTimeoutUnit(TimeUnit unit)
    {
        _timeoutUnit = unit;
    }

    public TimeUnit getTimeoutUnit()
    {
        return _timeoutUnit;
    }

    /**
     * Returns the communication timeout in milliseconds of the stub.
     */
    public long getTimeoutInMillis()
    {
        return _timeoutUnit.toMillis(_timeout);
    }

    public void setFlags(CellEndpoint.SendFlag... flags)
    {
        _flags = flags;
    }

    /**
     * Sets a limit on the number of concurrent requests to issue.
     *
     * Once the limit is reached, further attempts to send a message will block
     * until a reply is received for an earlier request or the earlier request
     * times out.
     *
     * Notification requests (those for which no reply is expected) are not subject
     * to the limit.
     *
     * Note that setting the limit will reset the counter, temporarily allowing a
     * higher number of outstanding requests.
     *
     * @param limit maximum number of concurrent requests
     */
    public void setConcurrencyLimit(Integer limit)
    {
        _concurrency = (limit == null) ? new UnlimitedSemaphore() : new Semaphore(limit);
    }

    /**
     * Sets a limit on the request rate.
     *
     * Places an upper bound on the number of requests issues per second on this
     * CellStub.
     *
     * In contrast to limiting the number of concurrent requests, the rate limiter
     * also applies to notification messages.
     *
     * @param requestsPerSecond maximum requests per second
     */
    public void setRate(Double requestsPerSecond)
    {
        /* We create a new rate limiter as it seems setting the rate to infinity once causes
         * an infinite number of permits to accumulate that will never be spent, thus
         * making it impossible to lower the rate. Should probably be filed as a bug against
         * Guava.
         */
        setRateLimiter(RateLimiter.create((requestsPerSecond == null) ? Double.POSITIVE_INFINITY : requestsPerSecond));
    }

    public Double getRate()
    {
        return _rateLimiter.getRate();
    }

    /**
     * Sets a limiter on the request rate.
     *
     * @param rateLimiter rate limiter
     */
    public void setRateLimiter(RateLimiter rateLimiter)
    {
        _rateLimiter = checkNotNull(rateLimiter);
    }

    public RateLimiter getRateLimiter()
    {
        return _rateLimiter;
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  msg     the message object to send
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(T msg, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return getMessage(send(msg, flags));
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  msg     the message object to send
     * @param  timeout in milliseconds to wait for a reply
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(T msg, long timeout, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return getMessage(send(msg, timeout, flags));
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(CellPath path, T msg, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return getMessage(send(path, msg, flags));
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T> T sendAndWait(Serializable msg, Class<T> type, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return get(send(msg, type, flags));
    }


    public <T> T sendAndWait(CellPath path, Serializable msg, Class<T> type, CellEndpoint.SendFlag... flags)
       throws CacheException, InterruptedException
    {
       return get(send(path, msg, type, flags));
    }


    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  timeout the time to wait for the reply
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T> T sendAndWait(Serializable msg, Class<T> type, long timeout, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return get(send(msg, type, timeout, flags));
    }



    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the same type as the message
     * object that was sent, and the return code of that message is
     * expected to be zero. If either is not the case, an exception is
     * thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @param  timeout the time to wait for the reply
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(CellPath path, T msg, long timeout, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return getMessage(send(path, msg, timeout, flags));
    }


    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  timeout the time to wait for the reply
     * @param  flags   flags affecting how the message is sent
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T> T sendAndWait(CellPath path, Serializable msg, Class<T> type, long timeout, CellEndpoint.SendFlag... flags)
        throws CacheException, InterruptedException
    {
        return get(send(path, msg, type, timeout, flags));
    }

    public <T extends Message> ListenableFuture<T> send(T message, CellEndpoint.SendFlag... flags)
    {
        return send(_destination, message, flags);
    }

    public <T extends Message> ListenableFuture<T> send(T message, long timeout, CellEndpoint.SendFlag... flags)
    {
        return send(_destination, message, timeout, flags);
    }

    public <T extends Message> ListenableFuture<T> send(CellPath destination, T message, CellEndpoint.SendFlag... flags)
    {
        return send(destination, message, getTimeoutInMillis(), flags);
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> ListenableFuture<T> send(CellPath destination, T message, long timeout, CellEndpoint.SendFlag... flags)
    {
        message.setReplyRequired(true);
        return send(destination, message, (Class<T>) message.getClass(), timeout, flags);
    }

    public <T> ListenableFuture<T> send(Serializable message, Class<T> type, CellEndpoint.SendFlag... flags)
    {
        return send(_destination, message, type, flags);
    }

    public <T> ListenableFuture<T> send(
            CellPath destination, Serializable message, Class<T> type, CellEndpoint.SendFlag... flags)
    {
        return send(destination, message, type, getTimeoutInMillis(), flags);
    }

    public <T> ListenableFuture<T> send(
            Serializable message, Class<T> type, long timeout, CellEndpoint.SendFlag... flags)
    {
        return send(_destination, message, type, timeout, flags);
    }

    public <T> ListenableFuture<T> send(
            CellPath destination, Serializable message, Class<T> type, long timeout, CellEndpoint.SendFlag... flags)
    {
        CellMessage envelope = new CellMessage(checkNotNull(destination), checkNotNull(message));
        Semaphore concurrency = _concurrency;
        CallbackFuture<T> future = new CallbackFuture<>(type, concurrency);
        concurrency.acquireUninterruptibly();
        _rateLimiter.acquire();
        _endpoint.sendMessage(envelope, future, MoreExecutors.directExecutor(), timeout, mergeFlags(_flags, flags));
        return future;
    }

    private CellEndpoint.SendFlag[] mergeFlags(CellEndpoint.SendFlag[] a, CellEndpoint.SendFlag[] b)
    {
        return (a.length == 0) ? b : (b.length == 0) ? a : ObjectArrays.concat(a, b, CellEndpoint.SendFlag.class);
    }

    /**
     * Sends <code>message</code> to <code>destination</code>.
     */
    public void notify(Serializable message)
    {
        if (_destination == null) {
            throw new IllegalStateException("Destination must be specified");
        }
        notify(_destination, message);
    }

    /**
     * Sends <code>message</code> to <code>destination</code>.
     */
    public void notify(CellPath destination, Serializable message)
    {
        _rateLimiter.acquire();
        _endpoint.sendMessage(new CellMessage(destination, message));
    }

    @Override
    public String toString()
    {
        CellPath path = getDestinationPath();
        return (path != null) ? path.toString() : super.toString();
    }

    /**
     * Registers a callback to be run when the {@code Future}'s computation is
     * {@linkplain java.util.concurrent.Future#isDone() complete} or, if the
     * computation is already complete, immediately.
     *
     * <p>There is no guaranteed ordering of execution of callbacks, but any
     * callback added through this method is guaranteed to be called once the
     * computation is complete.
     *
     * <p>Note: For fast, lightweight listeners that would be safe to execute in
     * any thread, consider {@link MoreExecutors#directExecutor}. For heavier
     * listeners, {@code directExecutor()} carries some caveats. See {@link
     * ListenableFuture#addListener} for details.
     *
     * <p>In is important the the executor isn't blocked by tasks waiting for
     * the callback; such tasks could lead to a deadlock.
     *
     * <p>If not using {@code directExecutor()}, it is advisable to use a
     * CDC preserving executor.
     *
     * @param future The future attach the callback to.
     * @param callback The callback to invoke when {@code future} is completed.
     * @param executor The executor to run {@code callback} when the future
     *    completes.
     * @see com.google.common.util.concurrent.Futures#addCallback
     * @see ListenableFuture#addListener
     */
    public static <T extends Message> void addCallback(
            final ListenableFuture<T> future, final MessageCallback<? super T> callback, Executor executor)
    {
        future.addListener(() -> {
            try {
                T reply = getUninterruptibly(future);
                callback.setReply(reply);
                if (reply.getReturnCode() != 0) {
                    callback.failure(reply.getReturnCode(), reply.getErrorObject());
                } else {
                    callback.success();
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutCacheException) {
                    callback.timeout(cause.getMessage());
                } else if (cause instanceof CacheException) {
                    CacheException cacheException = (CacheException) cause;
                    callback.failure(cacheException.getRc(), cacheException.getMessage());
                } else if (cause instanceof NoRouteToCellException) {
                    callback.noroute(((NoRouteToCellException) cause).getDestinationPath());
                } else {
                    callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, cause);
                }
            }
        }, executor);
    }

    /**
     * Returns the result of {@link java.util.concurrent.Future#get()}, converting most exceptions
     * and error conditions to a CacheException.
     *
     * Like CellStub#get, but also checks the return code of the Message reply. If non-zero, it
     * is rethrown as a CacheException matching the return code.
     *
     * @see CellStub#get
     */
    public static <T extends Message> T getMessage(Future<T> future)
            throws CacheException, InterruptedException
    {
        T reply = get(future);
        if (reply.getReturnCode() != 0) {
            throw CacheExceptionFactory.exceptionOf(reply);
        }
        return reply;
    }

    /**
     * Returns a new {@code ListenableFuture} whose result is asynchronously
     * derived from the message result of the given {@code Future}. More precisely,
     * the returned {@code Future} takes its result from a {@code Future} produced
     * by applying the given {@code Function} to the result of the original
     * {@code Future}.
     *
     * If the original {@code Future} returns a message indicating an error, the
     * returned {@code Future} will fail with the corresponding CacheException. This
     * distinguishes this method from {@link Futures#transform}.
     */
    public static <T extends Message, V> ListenableFuture<V> transform(
            ListenableFuture<T> future, Function<T, V> f)
    {
        return Futures.transformAsync(future,
                                      msg -> {
                                          if (msg.getReturnCode() != 0) {
                                              throw CacheExceptionFactory.exceptionOf(msg);
                                          }
                                          return Futures.immediateFuture(f.apply(msg));
                                      });
    }

    /**
     * Returns a new {@code ListenableFuture} whose result is asynchronously
     * derived from the message result of the given {@code Future}. More precisely,
     * the returned {@code Future} takes its result from a {@code Future} produced
     * by applying the given {@code AsyncFunction} to the result of the original
     * {@code Future}.
     *
     * If the original {@code Future} returns a message indicating an error, the
     * returned {@code Future} will fail with the corresponding CacheException. This
     * distinguishes this method from {@link Futures#transform}.
     */
    public static <T extends Message, V> ListenableFuture<V> transform(
            ListenableFuture<T> future, AsyncFunction<T, V> f)
    {
        return Futures.transformAsync(future,
                                      msg -> {
                                          if (msg.getReturnCode() != 0) {
                                              throw CacheExceptionFactory.exceptionOf(msg);
                                          }
                                          return f.apply(msg);
                                      });
    }

    /**
     * Returns the result of {@link java.util.concurrent.Future#get()}, converting most exceptions
     * to a CacheException.
     *
     * <p>Exceptions from {@code Future.get} are treated as follows:
     * <ul>
     * <li>Any {@link ExecutionException} has its <i>cause</i> unwrapped. Any CacheException is
     *     propagated untouched. A NoRouteToCellException is wrapped in a TimeoutCacheException.
     *     Other exceptions are wrapped in a CachException with error code UNEXPECTED_SYSTEM_EXCEPTION.
     * <li>Any {@link InterruptedException} is propagated untouched.
     * <li>Any {@link java.util.concurrent.CancellationException} is propagated untouched, as is any
     *     other {@link RuntimeException}.
     * </ul>
     */
    public static <T> T get(Future<T> future)
            throws CacheException, InterruptedException
    {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CacheException) {
                throw (CacheException) cause;
            } else if (cause instanceof NoRouteToCellException) {
                throw new TimeoutCacheException(cause.getMessage(), cause);
            } else {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, cause.getMessage(), cause);
            }
        }
    }

    /**
     * Adapter class to turn a CellMessageAnswerable callback into a ListenableFuture.
     */
    static class CallbackFuture<T> extends FutureCellMessageAnswerable<T>
    {
        private final Semaphore _concurrency;

        CallbackFuture(Class<? extends T> type, Semaphore concurrency)
        {
            super(type);
            _concurrency = concurrency;
        }

        @Override
        protected boolean set(T value)
        {
            boolean result = super.set(value);
            if (result) {
                _concurrency.release();
            }
            return result;
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            boolean result = super.setException(throwable);
            if (result) {
                _concurrency.release();
            }
            return result;
        }

    }

    /** NOP semaphore. Internal to CellStub; not a complete implementation. */
    private static class UnlimitedSemaphore extends Semaphore
    {
        private static final long serialVersionUID = -9165031174889707659L;

        UnlimitedSemaphore()
        {
            super(0);
        }

        @Override
        public void acquire() throws InterruptedException
        {
        }

        @Override
        public void acquireUninterruptibly()
        {
        }

        @Override
        public void release()
        {
        }
    }
}
