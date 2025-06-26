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
    private MigrationContext context;
    private Repository repository;
    private HotFileReplicator replicator;
    private PoolIoFileMessage message;
    private CacheEntry entry;
    private ScheduledExecutorService executor;
    private RefreshablePoolList poolList;
    private FileAttributes fileAttributes;
    private CellStub pnfsStub;
    private CellStub poolStub;
    private CellStub pinManagerStub;
    private PoolSelectionStrategy selectionStrategy;
    private TaskParameters taskParameters;
    private ListenableFuture listenableFuture;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        context = mock(MigrationContext.class);
        repository = mock(Repository.class);
        message = mock(PoolIoFileMessage.class);
        entry = mock(CacheEntry.class);
        executor = mock(ScheduledExecutorService.class);
        poolList = mock(RefreshablePoolList.class);
        fileAttributes = mock(FileAttributes.class);
        pnfsStub = mock(CellStub.class);
        poolStub = mock(CellStub.class);
        pinManagerStub = mock(CellStub.class);
        selectionStrategy = mock(PoolSelectionStrategy.class);
        listenableFuture = mock(ListenableFuture.class);

        when(context.getRepository()).thenReturn(repository);
        when(context.getPoolManagerStub()).thenReturn(null);
        when(context.getPoolName()).thenReturn("testPool");
        when(context.getPoolStub()).thenReturn(poolStub);
        when(context.getPnfsStub()).thenReturn(pnfsStub);
        when(context.getPinManagerStub()).thenReturn(pinManagerStub);
        when(context.getExecutor()).thenReturn(executor);

        // Mock CellStub.send to return a mock ListenableFuture for any generic type
        when((ListenableFuture) pnfsStub.send(any(), any())).thenReturn(listenableFuture);
        // Mock addListener to do nothing
        doNothing().when(listenableFuture).addListener(any(Runnable.class), any());

        // Use a real TaskParameters object
        taskParameters = new TaskParameters(
                poolStub,
                pnfsStub,
                pinManagerStub,
                executor,
                selectionStrategy,
                poolList,
                false, false, false, false, true, 1, false
        );

        replicator = new HotFileReplicator(context, poolList, taskParameters);
    }

    @Test
    public void testMaybeReplicateBelowThreshold() throws Exception {
        PnfsId pnfsId = new PnfsId("0000A1B2C3D4E5F6");
        when(message.getPnfsId()).thenReturn(pnfsId);
        replicator.maybeReplicate(message, 1L); // below default threshold
        // Should not trigger repository.getEntry
        verify(repository, never()).getEntry(any());
    }

    @Test
    public void testMaybeReplicateAboveThreshold() throws Exception {
        PnfsId pnfsId = new PnfsId("0000A1B2C3D4E5F6");
        when(message.getPnfsId()).thenReturn(pnfsId);
        when(repository.getEntry(pnfsId)).thenReturn(entry);
        when(entry.getPnfsId()).thenReturn(pnfsId);
        when(entry.getFileAttributes()).thenReturn(fileAttributes);
        when(entry.getLastAccessTime()).thenReturn(0L);
        replicator.maybeReplicate(message, 10L); // above default threshold
        verify(repository, times(1)).getEntry(pnfsId);
    }
}
