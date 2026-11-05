package org.dcache.pool.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.vehicles.FileAttributes;
import org.junit.Test;

public class JobTest {

    private static final PnfsId PNFS_ID = new PnfsId("000000000000000000000001");

    private final Deque<Runnable> callbacks = new ArrayDeque<>();

    @Test
    public void shouldReleaseLocksAndRetryReciprocalMigrationsAfterSleeping() throws Exception {
        Pool a = new Pool("pool-a", "pool-b");
        Pool b = new Pool("pool-b", "pool-a");
        a.start();
        b.start();

        // Deliver both requests while both source tasks still hold their locks.
        a.deliverTo(b);
        b.deliverTo(a);
        runCallbacks();

        a.assertSleeping();
        b.assertSleeping();
        verify(a.pnfs).send(any(PnfsGetCacheLocationsMessage.class));
        verify(b.pnfs).send(any(PnfsGetCacheLocationsMessage.class));

        // A retry can update B's replica while B is between attempts.
        a.wake();
        a.deliverTo(b);
        runCallbacks();
        assertEquals(Job.State.FINISHED, a.job.getState());
        assertFalse(a.context.isActive(PNFS_ID));

        b.wake();
        b.deliverTo(a);
        runCallbacks();
        assertEquals(Job.State.FINISHED, b.job.getState());
        assertFalse(b.context.isActive(PNFS_ID));
        verifyNoInteractions(a.p2p, b.p2p);
    }

    @Test
    public void shouldBackOffOnOtherTransientFailures() throws Exception {
        Pool pool = new Pool("pool-a", "pool-b");
        pool.start();
        pool.fail(new TimeoutCacheException("test timeout"));

        pool.assertSleeping();
        assertEquals(10_000L, pool.retryDelayMillis);
        pool.wake();
        assertEquals(Job.State.RUNNING, pool.job.getState());
        assertTrue(pool.context.isActive(PNFS_ID));
        assertEquals(1, pool.requests.size());
    }

    @Test
    public void shouldNotResumeSuspendedJobAfterFailure() throws Exception {
        Pool pool = new Pool("pool-a", "pool-b");
        pool.start();
        pool.job.suspend();
        pool.fail(CacheException.LOCKED);

        assertEquals(Job.State.SUSPENDED, pool.job.getState());
        assertFalse(pool.context.isActive(PNFS_ID));
        assertTrue(pool.retries.isEmpty());

        pool.job.resume();
        runCallbacks();
        assertEquals(1, pool.requests.size());
    }

    @Test
    public void shouldFinishCancellationInsteadOfSleepingAfterFailure() throws Exception {
        Pool pool = new Pool("pool-a", "pool-b");
        pool.start();
        pool.job.cancel(false, "test cancellation");
        pool.fail(CacheException.LOCKED);

        assertEquals(Job.State.CANCELLED, pool.job.getState());
        assertFalse(pool.context.isActive(PNFS_ID));
        assertTrue(pool.retries.isEmpty());
    }

    @Test
    public void shouldNotRestartCancelledJobWhenRetryTimerFires() throws Exception {
        Pool pool = new Pool("pool-a", "pool-b");
        pool.start();
        pool.fail(CacheException.LOCKED);
        pool.assertSleeping();

        pool.job.cancel(false, "test cancellation");
        pool.wake();

        assertEquals(Job.State.CANCELLED, pool.job.getState());
        assertFalse(pool.context.isActive(PNFS_ID));
        assertTrue(pool.requests.isEmpty());
    }

    @Test
    public void shouldNotRestartOrExtendSleepWhenAnotherRunningTaskFails() throws Exception {
        Pool pool = new Pool("pool-a", "pool-b");
        PnfsId otherId = new PnfsId("000000000000000000000002");
        CacheEntry otherEntry = mock(CacheEntry.class);
        when(otherEntry.getPnfsId()).thenReturn(otherId);
        when(otherEntry.getState()).thenReturn(ReplicaState.CACHED);
        when(otherEntry.getStickyRecords()).thenReturn(List.of());
        when(otherEntry.getFileAttributes()).thenReturn(FileAttributes.ofPnfsId(otherId));
        when(pool.repository.getEntry(otherId)).thenReturn(otherEntry);
        when(pool.repository.iterator()).thenReturn(List.of(PNFS_ID, otherId).iterator());
        pool.job.setConcurrency(2);
        pool.job.start();
        runCallbacks();
        assertEquals(2, pool.requests.size());

        pool.fail(CacheException.LOCKED);
        assertEquals(Job.State.SLEEPING, pool.job.getState());
        assertFalse(pool.context.isActive(PNFS_ID));
        assertTrue(pool.context.isActive(otherId));

        pool.fail(CacheException.LOCKED);
        pool.assertSleeping();
        assertFalse(pool.context.isActive(otherId));

        pool.wake();
        assertEquals(2, pool.requests.size());
        assertTrue(pool.context.isActive(PNFS_ID));
        assertTrue(pool.context.isActive(otherId));
    }

    private void runCallbacks() {
        int count = 0;
        while (!callbacks.isEmpty()) {
            assertTrue("Callbacks must quiesce without an immediate retry loop", count++ < 100);
            callbacks.remove().run();
        }
    }

    /**
     * Real jobs, tasks, state machines and migration servers, with manually delivered messages and
     * timers so contention is reproducible without threads or wall-clock sleeps.
     */
    private class Pool {

        private final MigrationContextImpl context = new MigrationContextImpl();
        private final MigrationModuleServer server = new MigrationModuleServer();
        private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        private final CellStub pnfs = mock(CellStub.class);
        private final P2PClient p2p = mock(P2PClient.class);
        private final Repository repository = mock(Repository.class);
        private final Deque<PoolMigrationCopyReplicaMessage> requests = new ArrayDeque<>();
        private final Deque<SettableFuture<PoolMigrationCopyReplicaMessage>> replies = new ArrayDeque<>();
        private final Deque<Runnable> retries = new ArrayDeque<>();
        private final CacheEntry entry = mock(CacheEntry.class);
        private final Job job;
        private long retryDelayMillis;

        private Pool(String name, String target) throws Exception {
            doAnswer(invocation -> {
                callbacks.add(invocation.getArgument(0));
                return null;
            }).when(executor).execute(any(Runnable.class));
            when(executor.submit(any(Runnable.class))).thenAnswer(invocation -> {
                FutureTask<Void> task = new FutureTask<>(invocation.getArgument(0), null);
                callbacks.add(task);
                return task;
            });
            when(executor.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any()))
                  .thenReturn(mock(ScheduledFuture.class));
            when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                  .thenAnswer(invocation -> {
                      assertEquals(TimeUnit.MILLISECONDS, invocation.getArgument(2));
                      long delay = invocation.getArgument(1);
                      if (delay != 120_000L) {
                          assertTrue("Retry delay must be bounded", delay >= 10_000 && delay < 20_000);
                          retryDelayMillis = delay;
                          retries.add(invocation.getArgument(0));
                      }
                      return mock(ScheduledFuture.class);
                  });

            FileAttributes attributes = new FileAttributes();
            attributes.setPnfsId(PNFS_ID);
            when(entry.getPnfsId()).thenReturn(PNFS_ID);
            when(entry.getState()).thenReturn(ReplicaState.CACHED);
            when(entry.getFileAttributes()).thenReturn(attributes);
            when(entry.getStickyRecords()).thenReturn(List.of());
            when(repository.iterator()).thenReturn(List.of(PNFS_ID).iterator());
            when(repository.getEntry(PNFS_ID)).thenReturn(entry);
            when(repository.getState(PNFS_ID)).thenReturn(ReplicaState.CACHED);
            when(pnfs.send(any(PnfsGetCacheLocationsMessage.class))).thenAnswer(invocation -> {
                PnfsGetCacheLocationsMessage message = invocation.getArgument(0);
                message.setCacheLocations(List.of(name, target));
                return Futures.immediateFuture(message);
            });
            CellStub poolStub = mock(CellStub.class);
            when(poolStub.getTimeoutInMillis()).thenReturn(60_000L);
            when(poolStub.send(eq(new CellPath(target)), any(PoolMigrationCopyReplicaMessage.class)))
                  .thenAnswer(invocation -> {
                      requests.add(invocation.getArgument(1));
                      SettableFuture<PoolMigrationCopyReplicaMessage> reply = SettableFuture.create();
                      replies.add(reply);
                      return reply;
                  });
            CellStub pinManager = mock(CellStub.class);
            when(pinManager.getDestinationPath()).thenReturn(new CellPath("PinManager"));
            context.setCellAddress(new CellAddressCore(name));
            context.setExecutor(executor);
            context.setRepository(repository);
            context.setPoolStub(poolStub);
            context.setPnfsStub(pnfs);
            context.setPinManagerStub(pinManager);

            RefreshablePoolList sourceList = mock(RefreshablePoolList.class);
            RefreshablePoolList poolList = mock(RefreshablePoolList.class);
            when(sourceList.isValid()).thenReturn(true);
            when(poolList.isValid()).thenReturn(true);
            PoolManagerPoolInformation targetInfo = mock(PoolManagerPoolInformation.class);
            when(targetInfo.getName()).thenReturn(target);
            when(poolList.getPools()).thenReturn(ImmutableList.of(targetInfo));
            when(poolList.getOfflinePools()).thenReturn(ImmutableList.of());
            CacheEntryMode sourceMode = new CacheEntryMode(CacheEntryMode.State.SAME, List.of());
            CacheEntryMode targetMode = new CacheEntryMode(CacheEntryMode.State.CACHED, List.of());
            job = new Job(context, new JobDefinition(e -> true, sourceMode, targetMode,
                  mock(PoolSelectionStrategy.class), null, sourceList, poolList, 60_000,
                  false, false, false, 1, false, false, true, null, null, false, false));

            server.setMigrationModule(new MigrationModule(context));
            server.setPoolMode(new PoolV2Mode());
            server.setRepository(repository);
            server.setExecutor(executor);
            server.setPPClient(p2p);
        }

        private void start() {
            job.start();
            runCallbacks();
            assertEquals(Job.State.RUNNING, job.getState());
            assertTrue(context.isActive(PNFS_ID));
            assertEquals(1, requests.size());
        }

        private void deliverTo(Pool target) throws Exception {
            PoolMigrationCopyReplicaMessage request = requests.remove();
            SettableFuture<PoolMigrationCopyReplicaMessage> reply = replies.remove();
            CellEndpoint endpoint = mock(CellEndpoint.class);
            doAnswer(invocation -> {
                CellMessage envelope = invocation.getArgument(0);
                callbacks.add(() -> job.messageArrived(
                      (PoolMigrationCopyFinishedMessage) envelope.getMessageObject()));
                return null;
            }).when(endpoint).sendMessage(any(CellMessage.class));
            target.server.setCellEndpoint(endpoint);
            CellMessage envelope = new CellMessage(new CellPath(target.context.getPoolName()), request);
            envelope.addSourceAddress(new CellAddressCore(context.getPoolName()));
            try {
                target.server.messageArrived(envelope, request);
                reply.set(request);
            } catch (CacheException e) {
                assertEquals(CacheException.LOCKED, e.getRc());
                reply.setException(e);
            }
        }

        private void fail(int rc) {
            fail(new CacheException(rc, "test failure"));
        }

        private void fail(CacheException failure) {
            requests.remove();
            replies.remove().setException(failure);
            runCallbacks();
        }

        private void assertSleeping() {
            assertEquals(Job.State.SLEEPING, job.getState());
            assertFalse(context.isActive(PNFS_ID));
            assertTrue(requests.isEmpty());
            assertEquals(1, retries.size());
        }

        private void wake() {
            retries.remove().run();
            runCallbacks();
        }
    }
}
