package org.dcache.pool.migration;

import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.Repository;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;

public class MigrationModuleSpinTest {

    private MigrationContext context;
    private ScheduledExecutorService executor;
    private RefreshablePoolList poolList;
    private RefreshablePoolList sourceList;
    private JobDefinition definition;
    private Repository repository;
    private Predicate<CacheEntry> filter;

    private JobDefinition createJobDefinition(boolean waitForTargets) {
        CacheEntryMode mode = new CacheEntryMode(CacheEntryMode.State.SAME,
              Collections.emptyList());
        return new JobDefinition(
              filter, // filter
              mode, // sourceMode
              mode, // targetMode
              mock(PoolSelectionStrategy.class), // selectionStrategy
              mock(Comparator.class), // comparator
              sourceList, // sourceList
              poolList, // poolList
              0, // refreshPeriod
              false, // isPermanent
              false, // isEager
              false, // isMetaOnly
              1, // replicas
              false, // mustMovePins
              false, // computeChecksumOnUpdate
              false, // maintainAtime
              null, // pauseWhen
              null, // stopWhen
              false, // forceSourceMode
              waitForTargets // waitForTargets
        );
    }

    @Before
    public void setUp() {
        context = mock(MigrationContext.class);
        executor = mock(ScheduledExecutorService.class);
        poolList = mock(RefreshablePoolList.class);
        sourceList = mock(RefreshablePoolList.class);
        repository = mock(Repository.class);
        filter = mock(Predicate.class);

        when(context.getExecutor()).thenReturn(executor);
        when(context.getPoolStub()).thenReturn(mock(CellStub.class));
        when(context.getPnfsStub()).thenReturn(mock(CellStub.class));

        CellStub pinManagerStub = mock(CellStub.class);
        CellAddressCore cellAddress = new CellAddressCore("PinManager");
        CellPath cellPath = new CellPath(cellAddress);
        when(pinManagerStub.getDestinationPath()).thenReturn(cellPath);
        when(context.getPinManagerStub()).thenReturn(pinManagerStub);
        when(context.lock(any(PnfsId.class))).thenReturn(true);

        when(context.getRepository()).thenReturn(repository);
        when(repository.iterator()).thenReturn(Collections.emptyIterator());

        definition = createJobDefinition(false);

        when(poolList.isValid()).thenReturn(true);
        when(sourceList.isValid()).thenReturn(true);

        // Mock pool list to return empty list (no pools available)
        when(poolList.getPools()).thenReturn(ImmutableList.of());
        when(poolList.getOfflinePools()).thenReturn(ImmutableList.of());

        FileAttributes attributes = mock(FileAttributes.class);
        when(attributes.isDefined(FileAttribute.LOCATIONS)).thenReturn(true);
        when(attributes.getLocations()).thenReturn(Collections.emptyList());

        // We need to make sure any CacheEntry returns these attributes
        // But CacheEntry is mocked in test methods.
        // I'll add a helper or just set it up in test methods.
    }

    @Test(timeout = 5000)
    public void testSpin() throws Exception {
        // Setup a job with some queued tasks
        PnfsId pnfsId = new PnfsId("000000000000000000000001");
        CacheEntry entry = mock(CacheEntry.class);
        when(entry.getPnfsId()).thenReturn(pnfsId);

        FileAttributes attributes = new FileAttributes();
        attributes.setLocations(Collections.emptyList());
        when(entry.getFileAttributes()).thenReturn(attributes);

        // Mock repository to return the entry
        when(repository.iterator()).thenReturn(Collections.singletonList(pnfsId).iterator());
        when(repository.getEntry(pnfsId)).thenReturn(entry);

        // Mock filter to accept the entry
        when(filter.test(entry)).thenReturn(true);

        Job job = new Job(context, definition);

        // Start the job
        job.start();

        // Capture and run the initialization task
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).submit(captor.capture());
        captor.getValue().run();

        // If the bug is fixed, the job should finish without sleeping/spinning
        verify(executor, times(0)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = 5000)
    public void testInsufficientPools_WaitForTargetsFalse() throws Exception {
        // Setup a job with some queued tasks
        PnfsId pnfsId = new PnfsId("000000000000000000000001");
        CacheEntry entry = mock(CacheEntry.class);
        when(entry.getPnfsId()).thenReturn(pnfsId);

        // Mock repository to return the entry
        when(repository.iterator()).thenReturn(Collections.singletonList(pnfsId).iterator());
        when(repository.getEntry(pnfsId)).thenReturn(entry);

        FileAttributes attributes = new FileAttributes();
        attributes.setLocations(Collections.emptyList());
        when(entry.getFileAttributes()).thenReturn(attributes);

        // Mock pool list to be valid but empty
        when(poolList.isValid()).thenReturn(true);
        when(poolList.getPools()).thenReturn(ImmutableList.of());

        // Mock filter to accept the entry
        when(filter.test(entry)).thenReturn(true);

        Job job = new Job(context, definition);
        job.start();

        // Capture and run the initialization task
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).submit(captor.capture());
        captor.getValue().run();

        // If the fix works, the task should complete (with insufficient replicas) and NOT retry.
        // So getPools() should be called at least once.
        verify(poolList, atLeast(1)).getPools();

        // Verify that Job did NOT sleep (schedule was not called for sleeping)
        verify(executor, times(0)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = 5000)
    public void testInsufficientPools_WaitForTargetsTrue() throws Exception {
        // Setup a job with some queued tasks
        PnfsId pnfsId = new PnfsId("000000000000000000000001");
        CacheEntry entry = mock(CacheEntry.class);
        when(entry.getPnfsId()).thenReturn(pnfsId);

        // Mock repository to return the entry
        when(repository.iterator()).thenReturn(Collections.singletonList(pnfsId).iterator());
        when(repository.getEntry(pnfsId)).thenReturn(entry);

        FileAttributes attributes = new FileAttributes();
        attributes.setLocations(Collections.emptyList());
        when(entry.getFileAttributes()).thenReturn(attributes);

        // Mock pool list to be valid but empty
        when(poolList.isValid()).thenReturn(true);
        when(poolList.getPools()).thenReturn(ImmutableList.of());

        // Mock filter to accept the entry
        when(filter.test(entry)).thenReturn(true);

        // Create definition with waitForTargets = true
        definition = createJobDefinition(true);

        Job job = new Job(context, definition);
        job.start();

        // Capture and run the initialization task
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).submit(captor.capture());
        captor.getValue().run();

        // Task should NOT start. Job should sleep immediately.

        // Verify that Job scheduled a sleep
        verify(executor, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // Verify that execute was NOT called (Task was not started)
        verify(executor, times(0)).execute(any(Runnable.class));
    }
}
