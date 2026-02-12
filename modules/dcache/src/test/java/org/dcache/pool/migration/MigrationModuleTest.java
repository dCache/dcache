package org.dcache.pool.migration;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.json.MigrationData;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;

public class MigrationModuleTest {

    private CacheEntry entry;
    private CellPath cellPath;
    private CellStub pinManagerStub;
    private CellStub pnfsStub;
    private CellStub poolManagerStub;
    private CellStub poolStub;
    private FileAttributes fileAttributes;
    private ListenableFuture listenableFuture;
    private MigrationContext context;
    private MigrationModule module;
    private PoolIoFileMessage message;
    private Repository repository;
    private ScheduledExecutorService executor;
    private ScheduledFuture scheduledFuture;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        cellPath = new CellPath(new CellAddressCore("mockCell@mockDomain"));
        context = mock(MigrationContext.class);
        entry = mock(CacheEntry.class);
        executor = mock(ScheduledExecutorService.class);
        scheduledFuture = mock(ScheduledFuture.class);
        fileAttributes = mock(FileAttributes.class);
        listenableFuture = mock(ListenableFuture.class);
        message = mock(PoolIoFileMessage.class);
        pinManagerStub = mock(CellStub.class);
        pnfsStub = mock(CellStub.class);
        poolManagerStub = mock(CellStub.class);
        poolStub = mock(CellStub.class);
        repository = mock(Repository.class);

        when(context.getExecutor()).thenReturn(executor);
        when(context.getPinManagerStub()).thenReturn(pinManagerStub);
        when(context.getPnfsStub()).thenReturn(pnfsStub);
        when(context.getPoolManagerStub()).thenReturn(poolManagerStub);
        when(context.getPoolName()).thenReturn("testPool");
        when(context.getPoolStub()).thenReturn(poolStub);
        when(context.getRepository()).thenReturn(repository);
        when(entry.getState()).thenReturn(ReplicaState.CACHED);
        when(executor.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any())).thenReturn(
              scheduledFuture);
        when(executor.submit(any(Runnable.class))).thenAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        });

        // Mock CellStub.send to return a mock ListenableFuture for any generic type
        when((ListenableFuture) pnfsStub.send(any(), any())).thenReturn(listenableFuture);
        when((ListenableFuture) poolManagerStub.send(any(), any())).thenReturn(listenableFuture);
        // Mock addListener to do nothing
        doNothing().when(listenableFuture).addListener(any(Runnable.class), any());

        // Make sure that we return a real CellPath when requested.
        when(pinManagerStub.getDestinationPath()).thenReturn(cellPath);

        module = new MigrationModule(context);
        module.afterStart();
    }

    @Test
    public void testReportFileRequestBelowThreshold() throws Exception {
        PnfsId pnfsId = new PnfsId("0000A1B2C3D4E5F6");
        when(message.getPnfsId()).thenReturn(pnfsId);
        module.setThreshold(5L);
        module.reportFileRequest(pnfsId, 1L); // below threshold
        // Should not create a migration job
        assertFalse(module.hasJob("hotfile-" + pnfsId));
    }

    @Test
    public void testReportFileRequestAboveThreshold() throws Exception {
        PnfsId pnfsId = new PnfsId("0000A1B2C3D4E5F6");
        when(entry.getFileAttributes()).thenReturn(fileAttributes);
        when(entry.getLastAccessTime()).thenReturn(0L);
        when(entry.getPnfsId()).thenReturn(pnfsId);
        when(message.getPnfsId()).thenReturn(pnfsId);
        when(repository.getEntry(pnfsId)).thenReturn(entry);
        module.setThreshold(5L);
        module.reportFileRequest(pnfsId, 10L); // above threshold
        // Should create a migration job
        assertTrue(module.hasJob("hotfile-" + pnfsId));
    }

    @Test
    public void testHotfileJobHousekeeping() throws Exception {
        SerializablePoolMonitor poolMonitor = mock(SerializablePoolMonitor.class);
        CostModule costModule = mock(CostModule.class);
        when(poolMonitor.getCostModule()).thenReturn(costModule);

        when(poolManagerStub.sendAndWait(any(PoolManagerGetPoolMonitor.class), anyLong()))
              .thenAnswer(inv -> {
                  PoolManagerGetPoolMonitor msg = (PoolManagerGetPoolMonitor) inv.getArguments()[0];
                  msg.setPoolMonitor(poolMonitor);
                  msg.setSucceeded();
                  return msg;
              });

        when(poolManagerStub.sendAndWait(not(isA(PoolManagerGetPoolMonitor.class)), anyLong()))
              .thenAnswer(inv -> {
                  Object msg = inv.getArguments()[0];
                  try {
                      msg.getClass().getMethod("setSucceeded").invoke(msg);
                  } catch (Exception e) {
                  }
                  return msg;
              });

        module.setThreshold(0L); // Always trigger

        // Setup FileAttributes for hot file migration
        when(entry.getFileAttributes()).thenReturn(fileAttributes);
        when(entry.getLastAccessTime()).thenReturn(0L);

        // Create 50 jobs
        for (int i = 0; i < 50; i++) {
            PnfsId pnfsId = new PnfsId(String.format("0000000000000000000000%02d", i));
            when(message.getPnfsId()).thenReturn(pnfsId);
            when(repository.getEntry(pnfsId)).thenReturn(entry);
            when(entry.getPnfsId()).thenReturn(pnfsId);
            module.reportFileRequest(pnfsId, 1L);
            Thread.sleep(1); // Ensure unique timestamps
        }

        // Cancel all to make them "completed"
        module.cancelAll();

        // Verify we have 50 jobs
        assertEquals(50, module.getDataObject().getJobInfo().length);

        // Create 10 more jobs
        for (int i = 50; i < 60; i++) {
            PnfsId pnfsId = new PnfsId(String.format("0000000000000000000000%02d", i));
            when(message.getPnfsId()).thenReturn(pnfsId);
            when(repository.getEntry(pnfsId)).thenReturn(entry);
            when(entry.getPnfsId()).thenReturn(pnfsId);
            when(repository.iterator()).thenReturn(Collections.singletonList(pnfsId).iterator());
            module.reportFileRequest(pnfsId, 1L);
            Thread.sleep(1);
        }

        // Verify we still have 60 jobs (50 cancelled + 10 new running)
        // The 10 new jobs are running, so they are not candidates for pruning.
        // The 50 cancelled jobs are terminal, but since the running jobs don't count towards the limit,
        // the terminal count is exactly 50, so no pruning happens.
        MigrationData data = module.getDataObject();
        String[] jobInfo = data.getJobInfo();
        assertEquals(60, jobInfo.length);
    }

    @Test
    public void testHotfileJobHousekeepingExclusions() throws Exception {
        SerializablePoolMonitor poolMonitor = mock(SerializablePoolMonitor.class);
        CostModule costModule = mock(CostModule.class);
        when(poolMonitor.getCostModule()).thenReturn(costModule);

        when(poolManagerStub.sendAndWait(any(PoolManagerGetPoolMonitor.class), anyLong()))
              .thenAnswer(inv -> {
                  PoolManagerGetPoolMonitor msg = (PoolManagerGetPoolMonitor) inv.getArguments()[0];
                  msg.setPoolMonitor(poolMonitor);
                  msg.setSucceeded();
                  return msg;
              });

        when(poolManagerStub.sendAndWait(not(isA(PoolManagerGetPoolMonitor.class)), anyLong()))
              .thenAnswer(inv -> {
                  Object msg = inv.getArguments()[0];
                  try {
                      msg.getClass().getMethod("setSucceeded").invoke(msg);
                  } catch (Exception e) {
                  }
                  return msg;
              });

        module.setThreshold(0L);

        // Setup FileAttributes for hot file migration
        when(entry.getFileAttributes()).thenReturn(fileAttributes);
        when(entry.getLastAccessTime()).thenReturn(0L);

        // 1. Create 50 jobs and cancel them (Terminal).
        for (int i = 0; i < 50; i++) {
            PnfsId pnfsId = new PnfsId(String.format("0000000000000000000001%02d", i));
            when(message.getPnfsId()).thenReturn(pnfsId);
            when(repository.getEntry(pnfsId)).thenReturn(entry);
            when(entry.getPnfsId()).thenReturn(pnfsId);
            module.reportFileRequest(pnfsId, 1L);
            Thread.sleep(1);
        }
        module.cancelAll();
        assertEquals(50, module.getDataObject().getJobInfo().length);

        // 2. Create 5 jobs and leave them RUNNING.
        for (int i = 0; i < 5; i++) {
            PnfsId pnfsId = new PnfsId(String.format("0000000000000000000002%02d", i));
            when(message.getPnfsId()).thenReturn(pnfsId);
            when(repository.getEntry(pnfsId)).thenReturn(entry);
            when(entry.getPnfsId()).thenReturn(pnfsId);
            when(repository.iterator()).thenReturn(Collections.singletonList(pnfsId).iterator());
            module.reportFileRequest(pnfsId, 1L);
            Thread.sleep(1);
        }
        // Total: 55 (50 Finished, 5 Running).
        // Pruning checks Terminal count (50). Limit 50. No prune.
        assertEquals(55, module.getDataObject().getJobInfo().length);

        // 3. Create 1 more job and cancel it (Terminal).
        // This pushes Terminal count to 51.
        PnfsId pnfsId = new PnfsId("000000000000000000000300");
        when(message.getPnfsId()).thenReturn(pnfsId);
        when(repository.getEntry(pnfsId)).thenReturn(entry);
        when(entry.getPnfsId()).thenReturn(pnfsId);
        module.reportFileRequest(pnfsId, 1L);
        // Cancel this specific job
        MigrationModule.MigrationCancelCommand cmd = module.new MigrationCancelCommand();
        cmd.id = "hotfile-" + pnfsId;
        cmd.force = true;
        cmd.call();

        // 4. Create 1 more job to trigger pruning
        PnfsId pnfsId2 = new PnfsId("000000000000000000000301");
        when(message.getPnfsId()).thenReturn(pnfsId2);
        when(repository.getEntry(pnfsId2)).thenReturn(entry);
        when(entry.getPnfsId()).thenReturn(pnfsId2);
        when(repository.iterator()).thenReturn(Collections.singletonList(pnfsId2).iterator());
        module.reportFileRequest(pnfsId2, 1L);

        // Analysis:
        // Start: 50 Terminal, 5 Running.
        // Step 3: Add J1. Cancel J1. -> 51 Terminal, 5 Running.
        // Step 4: Add J2. Pruning runs.
        //         Terminal count = 51. Limit = 50.
        //         Removes 1 oldest Terminal.
        //         Remaining: 50 Terminal, 5 Running, 1 New (J2).
        //         Total = 56.
        assertEquals(56, module.getDataObject().getJobInfo().length);
    }
}
