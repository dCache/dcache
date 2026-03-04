package org.dcache.pool.classic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import diskCacheV111.util.PnfsId;
import org.dcache.pool.movers.Mover;
import org.dcache.util.IoPriority;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

public class IoQueueManagerTest {

    private IoQueueManager ioQueueManager;
    private PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

    @Before
    public void setUp() {
        ioQueueManager = new IoQueueManager();
    }

    @Test
    public void shouldSumRequestsAcrossQueuesExcludingP2P() throws Exception {
        // Add a regular mover to the default queue
        addMover(IoQueueManager.DEFAULT_QUEUE, false);

        // Add a P2P mover to the p2p queue
        addMover(IoQueueManager.P2P_QUEUE_NAME, true);

        // Add another regular mover to a custom queue
        ioQueueManager.setQueues(new String[]{"custom"});
        addMover("custom", false);

        // Total should be 2 (1 from default, 1 from custom, 0 from p2p)
        assertEquals(2, ioQueueManager.numberOfRequestsFor(pnfsId));
    }

    private void addMover(String queueName, boolean isP2P) throws Exception {
        Mover mover = mock(Mover.class);
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        when(mover.getFileAttributes()).thenReturn(attributes);
        when(mover.isPoolToPoolTransfer()).thenReturn(isP2P);

        MoverSupplier supplier = mock(MoverSupplier.class);
        when(supplier.createMover()).thenReturn(mover);

        ioQueueManager.getOrCreateMover(queueName, "door-" + queueName + "-" + System.nanoTime(), supplier, IoPriority.REGULAR);
    }
}
