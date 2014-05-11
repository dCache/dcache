package org.dcache.services.login;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.CellMessage;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import dmg.cells.nucleus.CellMessageReceiver;

public class MessageHandler
    implements CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(MessageHandler.class);

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
     * Schedules a timeout task for the given request. The task will
     * interrupt the calling thread if the timeout is reached before
     * the task is cancelled.
     */
    private ScheduledFuture<?> scheduleTimeoutTask(CellMessage envelope)
    {
        final Thread self = Thread.currentThread();
        Runnable timeoutTask = new Runnable() {
                @Override
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
