package org.dcache.cells;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
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
    private boolean _retryOnNoRouteToCell;
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

    /**
     * Set the value of the retryOnNoRouteCell property, which
     * determines whether to retry on failure to route the message to
     * the destination.
     *
     * If set to false, failure to send the message will cause a
     * TimeoutCacheException to be reported right away. If set to
     * true, failure to send the message to the destination cell will
     * be retried until the timeout has been reached. Once the timeout
     * is reached, a TimeoutCacheException is thrown. This is useful
     * for destinations for which communication failure is known to be
     * temporary.
     *
     * Limitations: This property currently only has an effect on the
     * sendAndWait method. Asynchronous message delivery always
     * reports a no route error in case of communication failure.
     */
    public void setRetryOnNoRouteToCell(boolean retry)
    {
        _retryOnNoRouteToCell = retry;
    }

    /**
     * Returns the value of the retryOnNoRouteCell property, which
     * determines whether to retry on failure to route the message to
     * the destination.
     */
    public boolean getRetryOnNoRouteToCell()
    {
        return _retryOnNoRouteToCell;
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
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(T msg)
        throws CacheException, InterruptedException
    {
        return getMessage(send(msg));
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
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(T msg, long timeout)
        throws CacheException, InterruptedException
    {
        return getMessage(send(msg, timeout));
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
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(CellPath path, T msg)
        throws CacheException, InterruptedException
    {
        return getMessage(send(path, msg));
    }

    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T> T sendAndWait(Serializable msg, Class<T> type)
        throws CacheException, InterruptedException
    {
        return get(send(msg, type));
    }


    public <T> T sendAndWait(CellPath path, Serializable msg, Class<T> type)
       throws CacheException, InterruptedException
    {
       return get(send(path, msg, type));
    }


    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  timeout  the time to wait for the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T> T sendAndWait(Serializable msg, Class<T> type, long timeout)
        throws CacheException, InterruptedException
    {
        return get(send(msg, type, timeout));
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
     * @param  timeout  the time to wait for the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    public <T extends Message> T sendAndWait(CellPath path, T msg, long timeout)
        throws CacheException, InterruptedException
    {
        return getMessage(send(path, msg, timeout));
    }


    /**
     * Sends a message and waits for the reply. The reply is expected
     * to contain a message object of the specified type. If this is
     * not the case, an exception is thrown.
     *
     * @param  path    the destination cell
     * @param  msg     the message object to send
     * @param  type    the expected type of the reply
     * @param  timeout  the time to wait for the reply
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occurred, or the object in the reply was of the
     *       wrong type.
     */
    public <T> T sendAndWait(CellPath path, Serializable msg, Class<T> type, long timeout)
        throws CacheException, InterruptedException
    {
        return get(send(path, msg, type, timeout));
    }

    public <T extends Message> ListenableFuture<T> send(T message)
    {
        return send(_destination, message);
    }

    public <T extends Message> ListenableFuture<T> send(T message, long timeout)
    {
        return send(_destination, message, timeout);
    }

    public <T extends Message> ListenableFuture<T> send(CellPath destination, T message)
    {
        return send(destination, message, getTimeoutInMillis());
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> ListenableFuture<T> send(CellPath destination, T message, long timeout)
    {
        message.setReplyRequired(true);
        return send(destination, message, (Class<T>) message.getClass(), timeout);
    }

    public <T> ListenableFuture<T> send(Serializable message, Class<T> type)
    {
        return send(_destination, message, type);
    }

    public <T> ListenableFuture<T> send(
            CellPath destination, Serializable message, Class<T> type)
    {
        return send(destination, message, type, getTimeoutInMillis());
    }

    public <T> ListenableFuture<T> send(
            Serializable message, Class<T> type, long timeout)
    {
        return send(_destination, message, type, timeout);
    }

    public <T> ListenableFuture<T> send(
            CellPath destination, Serializable message, Class<T> type, long timeout)
    {
        CellMessage envelope = new CellMessage(checkNotNull(destination), checkNotNull(message));
        Semaphore concurrency = _concurrency;
        CallbackFuture<T> future = new CallbackFuture<>(type, concurrency);
        concurrency.acquireUninterruptibly();
        _rateLimiter.acquire();
        if (_retryOnNoRouteToCell) {
            _endpoint.sendMessageWithRetryOnNoRouteToCell(envelope, future, MoreExecutors.sameThreadExecutor(), timeout);
        } else {
            _endpoint.sendMessage(envelope, future, MoreExecutors.sameThreadExecutor(), getTimeoutInMillis());
        }
        return future;
    }

    /**
     * Sends <code>message</code> to <code>destination</code>.
     */
    public void notify(Serializable message)
        throws NoRouteToCellException
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
        throws NoRouteToCellException
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
     * any thread, consider {@link MoreExecutors#sameThreadExecutor}. For heavier
     * listeners, {@code sameThreadExecutor()} carries some caveats. See {@link
     * ListenableFuture#addListener} for details.
     *
     * <p>In is important the the executor isn't blocked by tasks waiting for
     * the callback; such tasks could lead to a deadlock.
     *
     * <p>If not using {@code sameThreadExecutor()}, it is advisable to use a
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
        future.addListener(new Runnable()
        {
            @Override
            public void run()
            {
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
    public static <T extends Message> T getMessage(ListenableFuture<T> future)
            throws CacheException, InterruptedException
    {
        T reply = get(future);
        if (reply.getReturnCode() != 0) {
            throw CacheExceptionFactory.exceptionOf(reply);
        }
        return reply;
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
    public static <T> T get(ListenableFuture<T> future)
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
    static class CallbackFuture<T> extends AbstractFuture<T> implements CellMessageAnswerable
    {
        private final Class<? extends T> _type;
        private final Semaphore _concurrency;

        CallbackFuture(Class<? extends T> type, Semaphore concurrency)
        {
            _type = type;
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

        @Override
        public void answerArrived(CellMessage request, CellMessage answer)
        {
            Object o = answer.getMessageObject();
            if (_type.isInstance(o)) {
                set(_type.cast(o));
            } else if (o instanceof Exception) {
                exceptionArrived(request, (Exception) o);
            } else {
                setException(new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                                "Unexpected reply: " + o));
            }
        }

        @Override
        public void answerTimedOut(CellMessage request)
        {
            setException(new TimeoutCacheException("Request to " + request.getDestinationPath() + " timed out."));
        }

        @Override
        public void exceptionArrived(CellMessage request, Exception exception)
        {
            if (exception.getClass() == CacheException.class) {
                CacheException e = (CacheException) exception;
                exception = CacheExceptionFactory.exceptionOf(e.getRc(), e.getMessage(), e);
            }
            setException(exception);
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
