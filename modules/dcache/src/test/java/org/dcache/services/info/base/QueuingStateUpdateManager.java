package org.dcache.services.info.base;

import java.util.LinkedList;
import java.util.Queue;

import static org.junit.Assert.fail;

/**
 * This Class provides a implementation of the StateUpdateManager interface where
 * all StateUpdate requests are held in a queue and are never processed.
 * The queue of pending StateUpdates can be accessed via a QueuingStateUpdateManager
 * specific method {@link getQueue}.
 */
public class QueuingStateUpdateManager implements StateUpdateManager {

    private Queue<StateUpdate> _queue = new LinkedList<>();

    @Override
    public int countPendingUpdates() {
        return _queue.size();
    }

    @Override
    public void enqueueUpdate( StateUpdate pendingUpdate) {
        _queue.add( pendingUpdate);
    }

    @Override
    public void shutdown()
    {
        fail( "QueuingStateUpdateManager.shutdown() not implemented");
    }

    public Queue<StateUpdate> getQueue() {
        return _queue;
    }
}
