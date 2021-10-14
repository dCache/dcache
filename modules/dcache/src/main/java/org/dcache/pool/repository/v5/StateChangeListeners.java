package org.dcache.pool.repository.v5;

import com.google.common.util.concurrent.MoreExecutors;
import dmg.cells.nucleus.CDC;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StickyChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StateChangeListeners {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(StateChangeListeners.class);

    private final List<StateChangeListener> _listeners =
          new CopyOnWriteArrayList<>();

    /**
     * Background thread for event processing. It is on purpose that this is a single thread: The
     * point is not to process events quickly but rather to process them sequentially and
     * independently from the thread that triggered the event.
     */
    private final ExecutorService _executorService =
          Executors.newSingleThreadExecutor();

    private Executor _executor;

    public StateChangeListeners() {
        _executor = _executorService;
    }

    public void setSynchronousNotification(boolean value) {
        _executor = value ? MoreExecutors.directExecutor() : _executorService;
    }

    public void add(StateChangeListener listener) {
        _listeners.add(listener);
    }

    public void remove(StateChangeListener listener) {
        _listeners.remove(listener);
    }

    public void stateChanged(final StateChangeEvent event) {
        CDC callingContext = new CDC();
        try {
            _executor.execute(() -> {
                try (CDC oldContext = callingContext.restore()) {
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
                            LOGGER.error("Unexpected failure during state change notification", e);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // Happens when executor is already shut down
            LOGGER.debug("Dropping repository state change notification: {}", e.getMessage());
        }
    }

    public void accessTimeChanged(final EntryChangeEvent event) {
        CDC callingContext = new CDC();
        try {
            _executor.execute(() -> {
                try (CDC oldContext = callingContext.restore()) {
                    for (StateChangeListener listener : _listeners) {
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
                            LOGGER.error("Unexpected failure during state change notification", e);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // Happens when executor is already shut down
            LOGGER.debug("Dropping repository access time change notification: {}", e.getMessage());
        }
    }

    public void stickyChanged(final StickyChangeEvent event) {
        CDC callingContext = new CDC();
        try {
            _executor.execute(() -> {
                try (CDC oldContext = callingContext.restore()) {
                    for (StateChangeListener listener : _listeners) {
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
                            LOGGER.error("Unexpected failure during state change notification", e);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // Happens when executor is already shut down
            LOGGER.debug("Dropping repository stick flag change notification: {}", e.getMessage());
        }
    }

    public void stop() {
        _executorService.shutdown();
        try {
            _executorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
