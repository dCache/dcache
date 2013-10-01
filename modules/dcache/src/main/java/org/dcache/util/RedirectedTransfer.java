package org.dcache.util;

import javax.security.auth.Subject;

import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;

/**
 * A transfer where the mover can send a redirect message to the door.
 */
public class RedirectedTransfer<T> extends Transfer
{
    private boolean _isRedirected;
    private T _redirectObject;

    public RedirectedTransfer(PnfsHandler pnfs, Subject namespaceSubject, Subject subject, FsPath path) {
        super(pnfs, namespaceSubject, subject, path);
    }

    public RedirectedTransfer(PnfsHandler pnfs, Subject subject, FsPath path) {
        super(pnfs, subject, path);
    }

    /**
     * Signals that the transfer is redirected.
     */
    public synchronized void redirect(T object)
    {
        _isRedirected = true;
        _redirectObject = object;
        notifyAll();
    }

    /**
     * Returns the redirect object injected through a call to
     * <code>redirect</code>, or null if <code>redirect</code> has not
     * been called.
     */
    public synchronized T getRedirect()
    {
        return _redirectObject;
    }

    /**
     * Blocks until the mover of this transfer has send a redirect
     * notification, until a timeout is reached, or until the mover
     * failed. Relies on the redirect being injected into the transfer
     * through the <code>redirect</code> method.
     *
     * @param millis The timeout in milliseconds
     * @return The redirect object
     * @throws CacheException if the mover failed
     * @throws TimeoutCacheException when the timeout was reached
     * @throws InterruptedException if the thread was interrupted
     */
    public synchronized T waitForRedirect(long millis)
        throws CacheException, InterruptedException
    {
        try {
            setStatus("Mover " + getPool() + "/" +
                      getMoverId() + ": Waiting for redirect");
            long deadline = System.currentTimeMillis() + millis;
            while (hasMover() && !_isRedirected &&
                   System.currentTimeMillis() < deadline) {
                wait(deadline - System.currentTimeMillis());
            }

            if (waitForMover(0)) {
                throw new CacheException("Mover finished without redirect");
            } else if (!_isRedirected) {
                throw new TimeoutCacheException("No redirect from mover");
            }
        } finally {
            setStatus(null);
        }
        return _redirectObject;
    }

    public synchronized T waitForRedirect(long timeout, TimeUnit unit)
            throws CacheException, InterruptedException
    {
        return waitForRedirect(unit.toMillis(timeout));
    }
}
