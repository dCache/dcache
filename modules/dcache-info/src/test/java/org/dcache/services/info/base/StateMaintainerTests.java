package org.dcache.services.info.base;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A set of tests to check that the StateMaintainer works as expected.
 */
public class StateMaintainerTests extends InfoBaseTestHelper {

    static final StatePath METRIC_PATH = StatePath.parsePath( "branch-1.metric");

    /**
     * Implement a StateCaretaker that is slow at processing StateUpdates. It
     * always reports that no metrics should be expunged.
     */
    private static class SlowCaretaker implements StateCaretaker {
        final long _delay;
        long _count;

        /**
         * Create a new SlowCaretaker that delays all processUpdate() calls
         * by some fixed delay
         *
         * @param delay the delay in milliseconds
         */
        public SlowCaretaker( long delay) {
            _delay = delay;
        }

        @Override
        public void processUpdate( StateUpdate update) {
            synchronized( this) {
                _count++;
                notifyAll();
            }

            try {
                Thread.sleep( _delay);
            } catch (InterruptedException e) {
                // Do nothing ..
            }
        }

        @Override
        public Date getEarliestMetricExpiryDate() {
            return null;
        }

        @Override
        public void removeExpiredMetrics() {
            // Do nothing.
        }
    }

    /**
     * A dummy StateCaretaker implementation that provides simulation of two
     * orthogonal aspects: expiring metrics and accepting updates.
     * <p>
     * By default, the class will simulate state with no expiring metrics. If
     * {@link #setMetricExpiryDate(Date)} is invoked then it will simulate
     * state with a single mortal metric. The {@link #waitForMetricExpiry()}
     * method allows tests to check metric expiration without any race
     * condition.
     * <p>
     * The objects of this Class will accept any number of StateUpdates from
     * some {@link StateUpdateManager}; and a test may check these later
     * using the {@link getUpdates} method. The expected number of
     * StateUpdates may be set using {@link #setProcessUpdateCount(int)},
     * which allows tests to use the {@link #waitForUpdates()} method to
     * remove race conditions.
     */
    private static class DummyCaretaker implements StateCaretaker {

        final private List<StateUpdate> _updates = new LinkedList<>();

        private boolean _metricExpired;
        private Date _metricExpiryDate;
        private CountDownLatch _updatesLatch;

        /*
         * METHODS THAT IMPLEMENT StateCaretaker
         */

        @Override
        public Date getEarliestMetricExpiryDate() {
            return _metricExpiryDate;
        }

        @Override
        public void processUpdate( StateUpdate update) {
            _updates.add( update);

            if( _updatesLatch != null) {
                _updatesLatch.countDown();
            }
        }

        @Override
        public void removeExpiredMetrics() {
            Date date = _metricExpiryDate;
            if (date == null) {
                fail("removeExpiredMetrics when no expiry data was set");
            }

            synchronized (date) {
                _metricExpired = true;
                date.notifyAll();
            }
        }

        /*
         * METHODS THAT ALTER BEHAVOUR
         */

        /**
         * Specify when this StateCaretaker should claim is the earliest that
         * a metric will expire. If this method is not called then the
         * StateCaretaker will claim to have no expiring metrics.
         *
         * @see waitForMetricExpiry
         * @param metricExpiryDate
         */
        public void setMetricExpiryDate( Date metricExpiryDate) {
            _metricExpiryDate = metricExpiryDate;
        }

        /**
         * Specify an expected number of
         * {@link StateCaretaker#processUpdate(StateUpdate)} calls. This is
         * used by the {@link #waitForUpdates()} method.
         *
         * @param count
         *            the number of processUpdate calls that are expected.
         */
        public void setProcessUpdateCount( int count) {
            if( _updatesLatch != null) {
                throw new IllegalStateException(
                        "Cannot set the count more than once");
            }

            _updatesLatch = new CountDownLatch( count);
        }

        /*
         * METHODS THAT BLOCK ON StateMaintainer ACTIVITY
         */

        /**
         * Wait until the expected number of calls to
         * {@link #processUpdate(StateUpdate)} method have happened.
         *
         * @throws InterruptedException
         *             if calling Thread was interrupted whilst waiting for
         *             updates to complete.
         * @see setExpectedProcessUpdateCount
         */
        public void waitForUpdates() throws InterruptedException {
            if( _updatesLatch == null) {
                throw new IllegalStateException(
                        "Cannot block as no expected count has been set yet");
            }

            _updatesLatch.await();
        }

        /**
         * Wait until the StateMaintainer has called
         * {@link removeExpiredMetrics}. If this has already happened then
         * this method will return straight away.
         *
         * @throws InterruptedException
         *             if calling Thread was interrupted whilst waiting for
         *             removeExpiredMetrics to be called.
         * @see setMetricExpiryDate
         */
        public void waitForMetricExpiry() throws InterruptedException {
            Date date = _metricExpiryDate;
            if (date == null) {
                throw new IllegalStateException(
                        "Cannot block without first setting an expected metric expiry date.");
            }

            synchronized (date) {
                if( !_metricExpired) {
                    date.wait();
                }
            }
        }
    }

    StateMaintainer _maintainer;
    DummyCaretaker _caretaker;

    @Before
    public void setUp()
    {
        _caretaker = new DummyCaretaker();
        _maintainer = new StateMaintainer();
        _maintainer.setCaretaker(_caretaker);
        _maintainer.setExecutor(Executors.newSingleThreadScheduledExecutor());
    }

    @After
    public void tearDown()
    {
        _maintainer.shutdown();
    }

    @Test
    public void testInitiallyEmptyQueue() throws InterruptedException {
        assertEquals( "pendingUpdates not zero", 0,
                      _maintainer.countPendingUpdates());
    }

    @Test
    public void testUpdateState() throws InterruptedException {
        _maintainer.setStateCaretaker( new SlowCaretaker( 1));

        _maintainer.enqueueUpdate( new StateUpdate());

        assertEquals( "pendingUpdates didn't increase after update", 1,
                      _maintainer.countPendingUpdates());
    }

    @Test(timeout = 1000)
    public void testCountPendingUpdates() throws InterruptedException {
        // Create a caretaker that will take 10s to process each request.
        SlowCaretaker slowCaretaker = new SlowCaretaker( 1000);
        _maintainer.setStateCaretaker( slowCaretaker);

        assertEquals( "initial state", 0, _maintainer.countPendingUpdates());

        // Start an initial update.  This will take 10s to complete.
        _maintainer.enqueueUpdate( new StateUpdate());

        // Active updates are still counted; so we have 10s to process this line
        assertEquals( "count after first update is being processed", 1,
                      _maintainer.countPendingUpdates());

        _maintainer.enqueueUpdate( new StateUpdate());

        // Again we have 10s to get to this point; vanishingly small risk of false-positive result.
        assertEquals( "count with an update being processed and second update queued", 2,
                      _maintainer.countPendingUpdates());
    }

    @Test(timeout = 1000)
    public void testUpdateProcessed() throws InterruptedException {
        _caretaker.setProcessUpdateCount( 1);

        _maintainer.enqueueUpdate( new StateUpdate());

        _caretaker.waitForUpdates();

        /**
         * NB there is a race-condition here between the StateMaintainer
         * decreasing the count and the test code here querying the number of
         * pending updates
         */
        Thread.sleep( 10);

        assertEquals( "count after one update processed", 0,
                      _maintainer.countPendingUpdates());
    }

    @Test(timeout = 1000)
    public void testTwoUpdateProcessed() throws InterruptedException {
        _caretaker.setProcessUpdateCount( 2);

        _maintainer.enqueueUpdate( new StateUpdate());
        _maintainer.enqueueUpdate( new StateUpdate());

        _caretaker.waitForUpdates();

        /**
         * NB there is a race-condition here between the StateMaintainer
         * final decrement of its internal count and the test code here
         * querying the number of pending updates. We sleep to try to reduce
         * the risk of triggering it but this isn't the correct solution.
         */
        Thread.sleep( 10);

        assertEquals( "count after one update processed", 0,
                      _maintainer.countPendingUpdates());
    }

    /**
     * TEST ABILITY TO REMOVE EXPIRED METRICS
     *
     * @throws InterruptedException
     *
     */

    @Test(timeout = 1000)
    public void testExpiring() throws InterruptedException {

        /**
         * Number of milliseconds into the future we simulate an expiring
         * metric.
         */
        final int delay = 1;
        _caretaker.setMetricExpiryDate( new Date( System.currentTimeMillis() +
                                                  delay));

        // We need to enqueue something to trigger the update of expire date.
        _maintainer.enqueueUpdate( new StateUpdate());

        _caretaker.waitForMetricExpiry();
    }

    // See http://rt.dcache.org/Ticket/Display.html?id=7330
    @Ignore("Broken test: depends on timing on machine")
    @Test(timeout = 1000)
    public void testExpiringDelayChanging() throws InterruptedException {

        /**
         * Number of milliseconds into the future we simulate an expiring
         * metric.
         */
        final int initialDelay = 10;
        _caretaker.setMetricExpiryDate( new Date( System.currentTimeMillis() +
                                                  initialDelay));

        // We need to enqueue something to trigger the update of expire date.
        _maintainer.enqueueUpdate( new StateUpdate());

        final int updatedDelay = 200;
        final long whenSet = System.currentTimeMillis();

        _caretaker.setMetricExpiryDate( new Date( whenSet + updatedDelay));

        // We need to enqueue something to trigger the update of expire date.
        _maintainer.enqueueUpdate( new StateUpdate());

        _caretaker.waitForMetricExpiry();

        final int observeredDelay = (int) (System.currentTimeMillis() - whenSet);

        // We set this pretty high as we have no real control on how long
        // the delay really will be.
        final int tolerance = 100;
        assertEquals( "updated delayed", updatedDelay, observeredDelay,
                      tolerance);
    }

}
