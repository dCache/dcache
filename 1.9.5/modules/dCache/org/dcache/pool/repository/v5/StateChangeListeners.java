package org.dcache.pool.repository.v5;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.pool.repository.EntryChangeEvent;

class StateChangeListeners
{
    private static final Logger _log =
        Logger.getLogger(StateChangeListeners.class);

    private final List<StateChangeListener> _listeners =
        new CopyOnWriteArrayList<StateChangeListener>();

    /**
     * Background thread for event processing. It is on purpose that
     * this is a single thread: The point is not to process events
     * quickly but rather to process them sequentially and
     * independently from the thread that triggered the event.
     */
    private final ExecutorService _executorService =
        Executors.newSingleThreadExecutor();

    private Executor _executor;

    public StateChangeListeners()
    {
        _executor = _executorService;
    }

    public void setSynchronousNotification(boolean value)
    {
        _executor = value ? new DirectExecutor() : _executorService;
    }

    public void add(StateChangeListener listener)
    {
        _listeners.add(listener);
    }

    public void remove(StateChangeListener listener)
    {
        _listeners.remove(listener);
    }

    public void stateChanged(final StateChangeEvent event)
    {
        _executor.execute(new Runnable() {
                public void run() {
                    for (StateChangeListener listener: _listeners) {
                        try {
                            listener.stateChanged(event);
                        } catch (RuntimeException e) {
                            /* State change notifications are
                             * important for proper functioning of the
                             * pool and we cannot risk a problem in an
                             * event handler causing other event
                             * handlers not to be called. We therefore
                             * catch, log and ignore these problems.
                             */
                            _log.error("Unexpected failure during state change notification", e);
                        }
                    }
                }
            });
    }

    public void accessTimeChanged(final EntryChangeEvent event)
    {
        _executor.execute(new Runnable() {
                public void run() {
                    for (StateChangeListener listener: _listeners) {
                        try {
                            listener.accessTimeChanged(event);
                        } catch (RuntimeException e) {
                            /* State change notifications are
                             * important for proper functioning of the
                             * pool and we cannot risk a problem in an
                             * event handler causing other event
                             * handlers not to be called. We therefore
                             * catch, log and ignore these problems.
                             */
                            _log.error("Unexpected failure during state change notification", e);
                        }
                    }
                }
            });
    }

    public void stickyChanged(final StickyChangeEvent event)
    {
        _executor.execute(new Runnable() {
                public void run() {
                    for (StateChangeListener listener: _listeners) {
                        try {
                            listener.stickyChanged(event);
                        } catch (RuntimeException e) {
                            /* State change notifications are
                             * important for proper functioning of the
                             * pool and we cannot risk a problem in an
                             * event handler causing other event
                             * handlers not to be called. We therefore
                             * catch, log and ignore these problems.
                             */
                            _log.error("Unexpected failure during state change notification", e);
                        }
                    }
                }
            });
    }

    public void stop()
        throws InterruptedException
    {
        _executorService.shutdown();
    }

    private static class DirectExecutor implements Executor
    {
        public void execute(Runnable r)
        {
            r.run();
        }
    }
}
