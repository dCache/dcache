package org.dcache.util;

import javax.security.auth.Subject;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolMoverKillMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

/**
 * A transfer where the mover can send a redirect message to the door
 * asynchronously.
 *
 * The transfer startup phase is blocking and identical to a regular Transfer,
 * however notification of redirect and transfer completion is done
 * asynchronously. Subclasses are to implement onQueued, onRedirect, onFinish and
 * onFailure. The class deals with out of order notifications and guarantees
 * that:
 *
 *  - a mover ID is known before onQueued is called
 *  - onQueued is always called before onRedirect
 *  - onRedirect is always called before onFinish
 *  - that onRedirect and onFinish are not called once onFailure was called
 *
 *  The class implements automatic killing of the mover in case the transfer is
 *  aborted.
 */
public abstract class AsynchronousRedirectedTransfer<T> extends Transfer
{
    private T _redirectObject;
    private boolean _isRedirected;
    private boolean _isFinished;
    private boolean _isDone;

    public AsynchronousRedirectedTransfer(PnfsHandler pnfs, Subject namespaceSubject, Subject subject, FsPath path) {
        super(pnfs, namespaceSubject, subject, path);
    }

    public AsynchronousRedirectedTransfer(PnfsHandler pnfs, Subject subject, FsPath path) {
        super(pnfs, subject, path);
    }

    @Override
    public void startMover(String queue, long timeout) throws CacheException, InterruptedException
    {
        super.startMover(queue, timeout);
        doQueued();
    }

    protected synchronized void doQueued()
    {
        if (_isDone) {
            doKill();
        } else {
            onQueued();
            doRedirect();
        }
    }

    protected synchronized void doRedirect()
    {
        if (!_isDone && getMoverId() != null && _isRedirected) {
            onRedirect(_redirectObject);
            doFinish();
        }
    }

    protected synchronized void doFinish()
    {
        if (!_isDone && getMoverId() != null  && _isRedirected && _isFinished) {
            onFinish();
            _isDone = true;
        }
    }

    protected synchronized void doAbort(Exception exception)
    {
        if (!_isDone) {
            doKill();
            onFailure(exception);
            _isDone = true;
        }
    }

    protected synchronized void doKill()
    {
        if (hasMover()) {
            Integer moverId = getMoverId();
            String pool = getPool();
            CellAddressCore poolAddress = getPoolAddress();
            try {
                PoolMoverKillMessage message =
                        new PoolMoverKillMessage(pool, moverId);
                message.setReplyRequired(false);
                _pool.notify(new CellPath(poolAddress), message);
            } catch (NoRouteToCellException e) {
                _log.error("Failed to kill mover " + pool + "/" + moverId + ": " + e.getMessage());
            }
        }
    }

    /**
     * Signals that the transfer is redirected.
     */
    public synchronized void redirect(T object)
    {
        _redirectObject = object;
        _isRedirected = true;
        doRedirect();
    }

    @Override
    public synchronized void finished(CacheException error)
    {
        super.finished(error);
        if (error != null) {
            doAbort(error);
        } else {
            _isFinished = true;
            doFinish();
        }
    }

    protected abstract void onQueued();

    protected abstract void onRedirect(T object);

    protected abstract void onFinish();

    protected abstract void onFailure(Exception exception);

}
