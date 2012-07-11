package org.dcache.services.info.base;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Queue;

import org.junit.Before;
import org.junit.Test;

public class QueuingStateUpdateManagerTests {

    QueuingStateUpdateManager _sum;

    @Before
    public void setUp()
    {
        _sum = new QueuingStateUpdateManager();
    }

    @Test
    public void testEmptyQueue() {
        assertEquals( "Number of pendingUpdates is wrong.", 0, _sum.countPendingUpdates());
        assertTrue( "Queue is not empty", _sum.getQueue().isEmpty());
    }

    @Test
    public void testAddSingleUpdate() {
        StateUpdate update = new StateUpdate();

        _sum.enqueueUpdate( update);

        assertEquals( "Number of pendingUpdates is wrong.", 1, _sum.countPendingUpdates());

        Queue<StateUpdate> queuedUpdates = _sum.getQueue();

        assertFalse( "Queue is empty", queuedUpdates.isEmpty());
        assertEquals( "Queue has wrong number of StateUpdates", 1, queuedUpdates.size());
        assertSame( "Queue has wrong StateUpdate as first item", update, queuedUpdates.peek());
    }

    @Test
    public void testAddTwoUpdates() {
        StateUpdate update1 = new StateUpdate();
        StateUpdate update2 = new StateUpdate();

        _sum.enqueueUpdate( update1);
        _sum.enqueueUpdate( update2);

        assertEquals( "Number of pendingUpdates is wrong.", 2, _sum.countPendingUpdates());

        Queue<StateUpdate> queuedUpdates = _sum.getQueue();

        assertFalse( "Queue is empty", queuedUpdates.isEmpty());
        assertEquals( "Queue has wrong number of StateUpdates", 2, queuedUpdates.size());

        StateUpdate item = queuedUpdates.poll();
        assertSame( "Queue has wrong StateUpdate as first item", update1, item);

        item = queuedUpdates.poll();
        assertSame( "Queue has wrong StateUpdate as first item", update2, item);
    }

}
