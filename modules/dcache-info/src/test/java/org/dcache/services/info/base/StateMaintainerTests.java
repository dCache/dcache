package org.dcache.services.info.base;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.*;

/**
 * A set of tests to check that the StateMaintainer works as expected.
 */
public class StateMaintainerTests
{
    private ExpungeAwareStateMaintainer _maintainer;
    private StateCaretaker _caretaker;

    /**
     * A StateUpdate that allows waiting for an update to be
     * processed.
     */
    private class AwaitableStateUpdate extends StateUpdate
    {
        private final CountDownLatch _latch = new CountDownLatch(1);

        @Override
        public void updateComplete()
        {
            _latch.countDown();
        }

        public void await() throws InterruptedException
        {
            _latch.await();
        }
    }

    /**
     * A StateMaintainer that allows awaiting for at least one expunge
     * cycle.
     */
    private class ExpungeAwareStateMaintainer extends StateMaintainer
    {
        private final CountDownLatch _latch = new CountDownLatch(1);

        @Override
        public void expungeCompleted()
        {
            _latch.countDown();
        }

        public void awaitExpunge() throws InterruptedException
        {
            _latch.await();
        }
    }

    @Before
    public void setUp()
    {
        _caretaker = mock(StateCaretaker.class);

        _maintainer = new ExpungeAwareStateMaintainer();
        _maintainer.setCaretaker(_caretaker);
        _maintainer.setExecutor(Executors.newSingleThreadScheduledExecutor());
    }

    @After
    public void tearDown()
    {
        _maintainer.shutdown();
    }

    @Test
    public void shouldBeInitiallyEmptyQueueSize() throws InterruptedException
    {
        assertThat(_maintainer.countPendingUpdates(), is(0));
    }

    @Test(timeout = 10_000)
    public void shouldIncrementAfterSubmittingUpdate() throws InterruptedException
    {
        willAnswer(i -> {wait(); return null;}).given(_caretaker).processUpdate(anyObject());

        _maintainer.enqueueUpdate(new StateUpdate());

        assertThat(_maintainer.countPendingUpdates(), is(1));
    }

    @Test(timeout = 10_000)
    public void shouldIncreaseAfterSubmittingUpdateWhenQueuedUpdate() throws InterruptedException
    {
        willAnswer(i -> {wait(); return null;}).given(_caretaker).processUpdate(anyObject());

        _maintainer.enqueueUpdate(new StateUpdate());
        _maintainer.enqueueUpdate(new StateUpdate());

        assertThat(_maintainer.countPendingUpdates(), is(2));
    }

    @Test(timeout = 10_000)
    public void shouldDecreaseAfterProcessed() throws InterruptedException
    {
        AwaitableStateUpdate update = new AwaitableStateUpdate();
        _maintainer.enqueueUpdate(update);
        update.await();

        assertThat(_maintainer.countPendingUpdates(), is(0));
    }

    @Test(timeout = 10_000)
    public void shouldDecreaseAfterProcessingTwoUpdates() throws InterruptedException
    {
        AwaitableStateUpdate update = new AwaitableStateUpdate();

        _maintainer.enqueueUpdate(new StateUpdate());
        _maintainer.enqueueUpdate(update);

        update.await();

        assertThat(_maintainer.countPendingUpdates(), is(0));
    }

    @Test(timeout = 10_000)
    public void shouldCheckExpiring() throws InterruptedException
    {
        // We need an update to trigger the update of expire date.
        AwaitableStateUpdate update = new AwaitableStateUpdate();
        _maintainer.enqueueUpdate(update);
        update.await();

        verify(_caretaker).getEarliestMetricExpiryDate();
        verify(_caretaker, never()).removeExpiredMetrics();
    }

    @Test(timeout = 10_000)
    public void shouldExpireAfterDelay() throws InterruptedException
    {
        given(_caretaker.getEarliestMetricExpiryDate()).willReturn(new Date());

        // We need to enqueue something to trigger the update of expire date.
        AwaitableStateUpdate update = new AwaitableStateUpdate();
        _maintainer.enqueueUpdate(update);
        update.await();

        _maintainer.awaitExpunge();

        verify(_caretaker, atLeast(1)).getEarliestMetricExpiryDate();
        verify(_caretaker, atLeast(1)).removeExpiredMetrics();
    }
}
