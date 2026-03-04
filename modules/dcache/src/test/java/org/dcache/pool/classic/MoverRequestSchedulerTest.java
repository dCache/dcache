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
import java.util.concurrent.atomic.AtomicInteger;

public class MoverRequestSchedulerTest {

    private MoverRequestScheduler scheduler;
    private PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

    @Before
    public void setUp() {
        scheduler = new MoverRequestScheduler("test-queue", 0, MoverRequestScheduler.Order.FIFO);
    }

    @Test
    public void shouldCountNonP2PRequests() throws Exception {
        addMover(pnfsId, false);
        addMover(pnfsId, false);

        assertEquals(2, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldNotCountP2PRequests() throws Exception {
        addMover(pnfsId, true);
        addMover(pnfsId, false);

        assertEquals(1, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldReturnZeroWhenNoRequests() {
        assertEquals(0, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldNotCountRequestsForDifferentPnfsId() throws Exception {
        PnfsId otherPnfsId = new PnfsId("000000000000000000000000000000000002");
        addMover(otherPnfsId, false);

        assertEquals(0, scheduler.numberOfRequestsFor(pnfsId));
    }

    private void addMover(PnfsId pnfsId, boolean isP2P) throws Exception {
        Mover mover = mock(Mover.class);
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        when(mover.getFileAttributes()).thenReturn(attributes);
        when(mover.isPoolToPoolTransfer()).thenReturn(isP2P);

        MoverSupplier supplier = mock(MoverSupplier.class);
        when(supplier.createMover()).thenReturn(mover);

        scheduler.getOrCreateMover(supplier, "door-" + System.nanoTime(), IoPriority.REGULAR);
    }
}
