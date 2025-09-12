package org.dcache.pool.migration;

import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.Repository;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

public class HotFileReplicatorTest {
    private CacheEntry entry;
    private CellStub pinManagerStub;
    private CellStub pnfsStub;
    private CellStub poolManagerStub;
    private CellStub poolStub;
    private FileAttributes fileAttributes;
    private HotFileReplicator replicator;
    private ListenableFuture listenableFuture;
    private MigrationContext context;
    private PoolIoFileMessage message;
    private PoolSelectionStrategy selectionStrategy;
    private RefreshablePoolList poolList;
    private Repository repository;
    private ScheduledExecutorService executor;
    private TaskParameters taskParameters;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        context = mock(MigrationContext.class);
        entry = mock(CacheEntry.class);
        executor = mock(ScheduledExecutorService.class);
        fileAttributes = mock(FileAttributes.class);
        listenableFuture = mock(ListenableFuture.class);
        message = mock(PoolIoFileMessage.class);
        pinManagerStub = mock(CellStub.class);
        pnfsStub = mock(CellStub.class);
        poolList = mock(RefreshablePoolList.class);
        poolManagerStub = mock(CellStub.class);
        poolStub = mock(CellStub.class);
        repository = mock(Repository.class);
        selectionStrategy = mock(PoolSelectionStrategy.class);

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

        replicator = new HotFileReplicator(context);
    }

    @Test
    public void testReportFileRequestBelowThreshold() throws Exception {
        PnfsId pnfsId = new PnfsId("0000A1B2C3D4E5F6");
        when(message.getPnfsId()).thenReturn(pnfsId);
        replicator.reportFileRequest(pnfsId, 1L); // below default threshold
        // Should not trigger repository.getEntry
        verify(repository, never()).getEntry(any());
    }

    @Test
    public void testReportFileRequestAboveThreshold() throws Exception {
        PnfsId pnfsId = new PnfsId("0000A1B2C3D4E5F6");
        when(message.getPnfsId()).thenReturn(pnfsId);
        when(repository.getEntry(pnfsId)).thenReturn(entry);
        when(entry.getPnfsId()).thenReturn(pnfsId);
        when(entry.getFileAttributes()).thenReturn(fileAttributes);
        when(entry.getLastAccessTime()).thenReturn(0L);
        replicator.reportFileRequest(pnfsId, 10L); // above default threshold
        verify(repository, times(1)).getEntry(pnfsId);
    }
}
