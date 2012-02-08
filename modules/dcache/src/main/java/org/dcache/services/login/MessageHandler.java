package org.dcache.services.login;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.dcache.cells.CellMessageReceiver;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.LoginReply;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.CellMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageHandler
    implements CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(MessageHandler.class);

    /**
     * Maximum TTL adjustment in milliseconds.
     */
    private static final int TTL_BUFFER_MAXIMUM = 10000;

    /**
     * Maximum TTL adjustment as a fraction of TTL.
     */
    private static final float TTL_BUFFER_FRACTION = 0.10f;

    private LoginStrategy _loginStrategy;
    private ScheduledExecutorService _executor;

    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        _loginStrategy = loginStrategy;
    }

    public LoginStrategy getLoginStrategy()
    {
        return _loginStrategy;
    }

    public void setTimeoutExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    public ScheduledExecutorService getTimeoutExecutor()
    {
        return _executor;
    }

    /**
     * Returns the adjusted TTL of a message. The adjusted TTL is the
     * TTL with some time subtracted to allow for cell communication
     * overhead. Returns Long.MAX_VALUE if the TTL is infinite.
     */
    private long getAdjustedTtl(CellMessage envelope)
    {
        long ttl = envelope.getTtl();
        return
            (ttl == Long.MAX_VALUE)
            ? Long.MAX_VALUE
            : ttl - Math.min(TTL_BUFFER_MAXIMUM,
                             (long) (ttl * TTL_BUFFER_FRACTION));
    }

    /**
     * Throws TimeoutCacheException if request is too old.
     */
    private void failIfTooOld(CellMessage envelope)
        throws TimeoutCacheException
    {
        if (envelope.getLocalAge() > getAdjustedTtl(envelope)) {
            _log.warn("Discarding " +
                      envelope.getMessageObject().getClass().getSimpleName() +
                      " because its time to live has been exceeded.");
            throw new TimeoutCacheException("TTL exceeded");
        }
    }

    /**
     * Schedules a timeout task for the given request. The task will
     * interrupt the calling thread if the timeout is reached before
     * the task is cancelled.
     */
    private ScheduledFuture<?> scheduleTimeoutTask(CellMessage envelope)
        throws TimeoutCacheException
    {
        final Thread self = Thread.currentThread();
        Runnable timeoutTask = new Runnable() {
                public void run() {
                    self.interrupt();
                }
            };
        return _executor.schedule(timeoutTask,
                                  envelope.getTtl() - envelope.getLocalAge(),
                                  TimeUnit.MILLISECONDS);
    }

    public LoginMessage messageArrived(CellMessage envelope, LoginMessage message)
        throws CacheException
    {
        failIfTooOld(envelope);

        ScheduledFuture<?> timeoutTask = scheduleTimeoutTask(envelope);
        try {
            LoginReply login = _loginStrategy.login(message.getSubject());
            message.setSubject(login.getSubject());
            message.setLoginAttributes(login.getLoginAttributes());
        } catch(RuntimeException e) {
            _log.error("Login operation failed", e);
            throw new PermissionDeniedCacheException(e.getMessage());
        } finally {
            timeoutTask.cancel(false);
        }

        return message;
    }

    public MapMessage messageArrived(MapMessage message)
        throws CacheException
    {
        Principal principal;

        try {
            principal = _loginStrategy.map(message.getPrincipal());
        } catch(RuntimeException e) {
            _log.error("Map operation failed", e);
            principal = null;
        }

        message.setMappedPrincipal(principal);
        return message;
    }

    public ReverseMapMessage messageArrived(ReverseMapMessage message)
        throws CacheException
    {
        Set<Principal> principals;

        try {
            principals = _loginStrategy.reverseMap(message.getPrincipal());
        } catch(RuntimeException e) {
            _log.error("ReverseMap operation failed", e);
            principals = Collections.emptySet();
        }

        message.setMappedPrincipals(principals);
        return message;
    }
}
