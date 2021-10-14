package org.dcache.util;

import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.util.TimeUtils.TimeUnitFormat;

/**
 * A transfer where the mover can send a redirect message to the door.
 */
public class RedirectedTransfer<T> extends Transfer {

    private boolean _isRedirected;
    private T _redirectObject;

    public RedirectedTransfer(PnfsHandler pnfs, Subject namespaceSubject,
          Restriction restriction, Subject subject, FsPath path) {
        super(pnfs, namespaceSubject, restriction, subject, path);
    }

    public RedirectedTransfer(PnfsHandler pnfs, Subject subject, Restriction restriction,
          FsPath path) {
        super(pnfs, subject, restriction, path);
    }

    /**
     * Signals that the transfer is redirected.
     */
    public synchronized void redirect(T object) {
        _isRedirected = true;
        _redirectObject = object;
        notifyAll();
    }

    /**
     * Returns the redirect object injected through a call to
     * <code>redirect</code>, or null if <code>redirect</code> has not
     * been called.
     */
    public synchronized T getRedirect() {
        return _redirectObject;
    }

    /**
     * Blocks until the mover of this transfer has send a redirect notification, until a timeout is
     * reached, or until the mover failed. Relies on the redirect being injected into the transfer
     * through the <code>redirect</code> method.
     *
     * @param millis The timeout in milliseconds
     * @return The redirect object
     * @throws CacheException        if the mover failed
     * @throws TimeoutCacheException when the timeout was reached
     * @throws InterruptedException  if the thread was interrupted
     */
    public synchronized T waitForRedirect(long millis)
          throws CacheException, InterruptedException {
        try {
            setStatus("Mover " + getPool() + "/" +
                  getMoverId() + ": Waiting for redirect");
            long deadline = addWithInfinity(System.currentTimeMillis(), Math.max(0, millis));
            while (hasMover() && !_isRedirected &&
                  System.currentTimeMillis() < deadline) {
                wait(subWithInfinity(deadline, System.currentTimeMillis()));
            }

            if (waitForMover(0)) {
                throw new CacheException("Mover finished without redirect");
            } else if (!_isRedirected) {
                StringBuilder sb = new StringBuilder("No redirect from mover on pool ")
                      .append(getPool()).append(" after ");
                TimeUtils.appendDuration(sb, millis, TimeUnit.MILLISECONDS, TimeUnitFormat.SHORT);
                throw new TimeoutCacheException(sb.toString());
            }
        } finally {
            setStatus(null);
        }
        return _redirectObject;
    }

    public synchronized T waitForRedirect(long timeout, TimeUnit unit)
          throws CacheException, InterruptedException {
        return waitForRedirect(unit.toMillis(timeout));
    }
}
