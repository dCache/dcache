package org.dcache.pool.repository.v5;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;

class StateChangeListeners
{
    private static final Logger _log =
        Logger.getLogger(StateChangeListeners.class);

    private final List<StateChangeListener> _listeners =
        new CopyOnWriteArrayList<StateChangeListener>();
    private final BlockingQueue<StateChangeEvent> _notifications =
        new LinkedBlockingQueue<StateChangeEvent>();
    private final Thread _workerThread = new WorkerThread();

    public StateChangeListeners()
    {
        _workerThread.start();
    }

    public void add(StateChangeListener listener)
    {
        _listeners.add(listener);
    }

    public void remove(StateChangeListener listener)
    {
        _listeners.remove(listener);
    }

    public void notify(StateChangeEvent event)
    {
        if (!_notifications.offer(event))
            throw new IllegalStateException("Failed to add event to queue");
    }

    public void stop()
    {
        _workerThread.interrupt();
    }

    private class WorkerThread extends Thread
    {
        public void run()
        {
            try {
                while (true) {
                    StateChangeEvent event = _notifications.take();
                    for (StateChangeListener listener : _listeners) {
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
            } catch (InterruptedException e) {
                // Normal shutdown notification
            }
        }
    }
}
