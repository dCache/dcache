package org.dcache.pool.migration;

import com.google.common.util.concurrent.ListenableFuture;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.Repository;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        cellPath = new CellPath(new CellAddressCore("mockCell@mockDomain"));
        context = mock(MigrationContext.class);
        entry = mock(CacheEntry.class);
        executor = mock(ScheduledExecutorService.class);
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

        // Mock CellStub.send to return a mock ListenableFuture for any generic type
        when((ListenableFuture) pnfsStub.send(any(), any())).thenReturn(listenableFuture);
        when((ListenableFuture) poolManagerStub.send(any(), any())).thenReturn(listenableFuture);
        // Mock addListener to do nothing
        doNothing().when(listenableFuture).addListener(any(Runnable.class), any());

        // Make sure that we return a real CellPath when requested.
        when(pinManagerStub.getDestinationPath()).thenReturn(cellPath);

        module = new MigrationModule(context);
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
}
