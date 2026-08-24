package org.dcache.pool.migration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

/**
 * Reproduces the scenario where a target pool rejects a
 * {@link PoolMigrationCopyReplicaMessage} with {@link CacheException#LOCKED} because it
 * currently has a local migration task active for the same file (see
 * MigrationModuleServer#messageArrived(CellMessage, PoolMigrationCopyReplicaMessage), line 121).
 * <p>
 * This is expected to happen routinely for "hot" files that are concurrently being replicated
 * to/from several pools: two pools may simultaneously try to migrate the same file to each
 * other, and each check will reject the other's request with LOCKED to avoid a race condition.
 * <p>
 * When a task in the {@code UpdatingExistingFile} state receives such a LOCKED failure while
 * updating one known existing replica location, it should behave exactly like every other
 * failure and try the next known location before giving up (see the {@code hasMoreLocations()}
 * fallback in Task.sm). Instead, the LOCKED case is special-cased to fail the task immediately,
 * without trying any of the other known locations. Since the job simply requeues and retries the
 * whole task again after a fixed delay - reselecting the same locations - a persistently locked
 * target causes the migration job to spin forever in RUNNING/SLEEPING, repeatedly failing against
 * the same target instead of falling back to an alternative that might succeed.
 */
public class MigrationModuleLockedTargetTest {

    private TaskCompletionHandler handler;
    private CellStub poolStub;
    private ScheduledExecutorService executor;
    private RefreshablePoolList poolList;
    private List<SettableFuture<PoolMigrationCopyReplicaMessage>> sentRequests;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        handler = mock(TaskCompletionHandler.class);
        poolStub = mock(CellStub.class);
        executor = mock(ScheduledExecutorService.class);
        poolList = mock(RefreshablePoolList.class);
        sentRequests = new ArrayList<>();

        // Run everything inline so the test is fully deterministic.
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(any(Runnable.class));

        PoolManagerPoolInformation pool1 = mock(PoolManagerPoolInformation.class);
        when(pool1.getName()).thenReturn("pool1");
        PoolManagerPoolInformation pool2 = mock(PoolManagerPoolInformation.class);
        when(pool2.getName()).thenReturn("pool2");

        when(poolList.getPools()).thenReturn(ImmutableList.of(pool1, pool2));
        when(poolList.getOfflinePools()).thenReturn(ImmutableList.of());

        when(poolStub.send(any(CellPath.class), any(PoolMigrationCopyReplicaMessage.class),
              any(CellEndpoint.SendFlag[].class)))
              .thenAnswer(invocation -> {
                  SettableFuture<PoolMigrationCopyReplicaMessage> future = SettableFuture.create();
                  sentRequests.add(future);
                  return future;
              });
    }

    @Test
    public void taskShouldTryNextLocationWhenTargetReportsLocked() {
        PnfsId pnfsId = new PnfsId("000000000000000000000001");

        FileAttributes fileAttributes = new FileAttributes();
        fileAttributes.setLocations(List.of("pool1", "pool2"));
        fileAttributes.setPnfsId(pnfsId);

        TaskParameters parameters = new TaskParameters(
              poolStub,
              mock(CellStub.class),
              mock(CellStub.class),
              executor,
              mock(PoolSelectionStrategy.class),
              poolList,
              false, // isEager
              false, // isMetaOnly
              false, // computeChecksumOnUpdate
              false, // forceSourceMode
              false, // maintainAtime
              1,     // replicas
              false  // waitForTargets
        );

        Task task = new Task(parameters, handler, "sourcePool", pnfsId,
              ReplicaState.CACHED, Collections.emptyList(), Collections.emptyList(),
              fileAttributes, 0L);

        task.run();

        // The task should have contacted the first known location (pool1).
        assertRequestCount(1);

        // Simulate pool1 rejecting the request because it currently has a local migration
        // task active for this file (MigrationModuleServer throws LockedCacheException).
        sentRequests.get(0).setException(
              new LockedCacheException("Target file is busy"));

        // There is another known location (pool2) that has not been tried yet, so the task
        // should fall back to it instead of failing outright - exactly as it would for any
        // other transient copy failure.
        assertRequestCount(2);
        verify(handler, never()).taskFailed(any(), anyInt(), anyString());
        verify(handler, never()).taskFailedPermanently(any(), anyInt(), anyString());
    }

    private void assertRequestCount(int expected) {
        if (sentRequests.size() != expected) {
            throw new AssertionError(
                  "Expected " + expected + " copy request(s) to have been sent, but "
                        + sentRequests.size() + " were sent. Known target locations: pool1, pool2. "
                        + "A LOCKED failure must not cause the task to give up while other "
                        + "known locations remain untried.");
        }
    }
}
