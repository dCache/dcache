package org.dcache.pool.movers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import diskCacheV111.vehicles.PoolDeliverFileMessage;
import dmg.cells.nucleus.CellPath;
import java.nio.channels.CompletionHandler;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.junit.Before;
import org.junit.Test;

public class MoverQueueTimeTest {

    private MoverProtocolMover mover;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        mover = new MoverProtocolMover(mock(ReplicaDescriptor.class),
              mock(PoolDeliverFileMessage.class), new CellPath("door"),
              mock(TransferService.class), mock(MoverProtocol.class));
    }

    @Test
    public void shouldReportNoQueueTimeBeforeExecution() {
        assertEquals(0L, mover.getQueueTime());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReportTimeBetweenCreationAndExecution() throws Exception {
        Thread.sleep(50);

        mover.execute(mock(CompletionHandler.class));

        assertThat(mover.getQueueTime(), greaterThanOrEqualTo(40L));
        assertThat(mover.getQueueTime(), lessThan(10_000L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotCountTimeAfterExecutionStarted() throws Exception {
        mover.execute(mock(CompletionHandler.class));
        long queueTime = mover.getQueueTime();

        Thread.sleep(50);

        assertEquals(queueTime, mover.getQueueTime());
    }
}
