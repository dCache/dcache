package org.dcache.pool.repository;

import static org.dcache.pool.repository.AllocatorAwareRepositoryChannel.SPACE_INC;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.PnfsId;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

/**
 *
 */
public class AllocatorAwareRepositoryChannelTest {

    private static final PnfsId ID = new PnfsId("000000000000000000000000000000000000");

    private RepositoryChannel inner;
    private AllocatorAwareRepositoryChannel allocatorChannel;
    private Allocator allocator;
    private Repository repository;

    @Before
    public void setUp() throws IOException {
        inner = mock(RepositoryChannel.class);
        allocator = mock(Allocator.class);
        repository = mock(Repository.class);
        allocatorChannel = new AllocatorAwareRepositoryChannel(inner, repository,
              ID, allocator);
    }

    @Test
    public void shouldAllocateOnWrite() throws Exception {
        allocatorChannel.write(ByteBuffer.allocate(512));
        verify(allocator).allocate(ID, SPACE_INC);
    }

    @Test
    public void shouldNotCallAllocateIfFirtAllocationIsEnouth() throws Exception {
        allocatorChannel.write(ByteBuffer.allocate(512));
        allocatorChannel.write(ByteBuffer.allocate(512));
        verify(allocator, times(1)).allocate(ID, SPACE_INC);
    }

    @Test
    public void shouldAllocateOnWriteIfOffsetbBehindAllocation() throws Exception {
        allocatorChannel.write(ByteBuffer.allocate(512));
        allocatorChannel.write(ByteBuffer.allocate(512), SPACE_INC);
        verify(allocator, atLeast(2)).allocate(ID, SPACE_INC);
    }

    @Test
    public void shouldFreeOnCloseIfOverAllocated() throws Exception {
        allocatorChannel.write(ByteBuffer.allocate(512));
        when(inner.size()).thenReturn(512L);
        allocatorChannel.close();
        verify(allocator).free(ID, SPACE_INC - 512);
    }

    @Test
    public void shouldAllocateOnCloseIfUnderAllocated() throws Exception {
        allocatorChannel.write(ByteBuffer.allocate(512));
        when(inner.size()).thenReturn(SPACE_INC + 1);
        allocatorChannel.close();

        InOrder order = inOrder(allocator);
        order.verify(allocator).allocate(ID, SPACE_INC);
        order.verify(allocator).allocate(ID, 1);
    }

    @Test
    public void shouldAllocateSpaceOnTruncateIfGrow() throws Exception {
        allocatorChannel.truncate(512);
        verify(allocator).allocate(ID, SPACE_INC);
    }

    @Test
    public void shouldKeepAllocatedSpaceOnTruncateIfShrink() throws Exception {
        allocatorChannel.truncate(512);
        allocatorChannel.truncate(0);
        verify(allocator, atMost(1)).allocate(ID, SPACE_INC);
    }

}
