package org.dcache.pool.classic;

import diskCacheV111.vehicles.ProtocolInfo;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.MoverProtocolMover;
import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertFalse;

public class AbstractMoverProtocolTransferServiceTest {

    private AbstractMoverProtocolTransferService transferService;
    private InterruptAwareRepositoryChannel channel;
    private MoverProtocolMover moverProtocolMover;

    @Before
    public void setUp() throws Exception {
        channel = new InterruptAwareRepositoryChannel();
        moverProtocolMover = mock(MoverProtocolMover.class);
        when(moverProtocolMover.openChannel()).thenReturn(channel);

        transferService = new AbstractMoverProtocolTransferService() {
            @Override
            protected MoverProtocol createMoverProtocol(ProtocolInfo info) throws Exception {
                return null;
            }
        };
    }

    @Test
    public void testMoverInterruption() throws Exception {

        BlockingCompletionHandler completionHandler = new BlockingCompletionHandler();

        when(moverProtocolMover.getMover()).thenReturn(new ThreadInterruptingMover());

        transferService.executeMover(moverProtocolMover, completionHandler);
        completionHandler.await();
        assertFalse("Interrupted thread passed to RepositoryChannel", channel.isInterrupted());
        assertTrue("Exception is not propagated", completionHandler.getThrowable().isPresent());
    }

    @Test
    public void testMoverThrowingInterrupedException() throws Exception {

        BlockingCompletionHandler completionHandler = new BlockingCompletionHandler();
        when(moverProtocolMover.getMover()).thenReturn(new InterruptedThrowingMover());

        transferService.executeMover(moverProtocolMover, completionHandler);
        completionHandler.await();
        assertFalse("Interrupted thread passed to RepositoryChannel", channel.isInterrupted());
        assertTrue("Exception is not propagated", completionHandler.getThrowable().isPresent());
    }

    @Test
    public void testMoverThrowingInterrupedIOException() throws Exception {

        BlockingCompletionHandler completionHandler = new BlockingCompletionHandler();
        when(moverProtocolMover.getMover()).thenReturn(new InterruptedIOThrowingMover());

        transferService.executeMover(moverProtocolMover, completionHandler);
        completionHandler.await();
        assertFalse("Interrupted thread passed to RepositoryChannel", channel.isInterrupted());
        assertTrue("Exception is not propagated", completionHandler.getThrowable().isPresent());
    }

    @Test
    public void testMoverCancel() throws Exception {

        BlockingCompletionHandler completionHandler = new BlockingCompletionHandler();

        when(moverProtocolMover.getMover()).thenReturn(new ThreadBlockingMover());

        transferService.executeMover(moverProtocolMover, completionHandler).cancel("bang-bang!");
        completionHandler.await();

        assertFalse("Interrupted thread passed to RepositoryChannel", channel.isInterrupted());
        assertTrue("Exception is not propagated", completionHandler.getThrowable().isPresent());
    }


    private static class InterruptAwareRepositoryChannel extends ForwardingRepositoryChannel {

        private volatile boolean isInterrupted;
        private final RepositoryChannel inner = mock(RepositoryChannel.class);

        @Override
        protected RepositoryChannel delegate() {
            return inner;
        }

        @Override
        public void close() throws IOException {
            isInterrupted = Thread.currentThread().isInterrupted();
        }

        public boolean isInterrupted() {
            return isInterrupted;
        }
    }

    private static class ThreadInterruptingMover implements MoverProtocol {

        @Override
        public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile, ProtocolInfo protocol, Set<? extends OpenOption> access) throws Exception {
            Thread.currentThread().interrupt();
        }

        @Override
        public long getBytesTransferred() {
            return 0;
        }

        @Override
        public long getTransferTime() {
            return 0;
        }

        @Override
        public long getLastTransferred() {
            return 0;
        }
    }

    private static class InterruptedIOThrowingMover implements MoverProtocol {

        @Override
        public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile, ProtocolInfo protocol, Set<? extends OpenOption> access) throws Exception {
            // IO is interrupted, flag is set and  InterruptedIOException is thrown
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }

        @Override
        public long getBytesTransferred() {
            return 0;
        }

        @Override
        public long getTransferTime() {
            return 0;
        }

        @Override
        public long getLastTransferred() {
            return 0;
        }
    }

    private static class InterruptedThrowingMover implements MoverProtocol {

        @Override
        public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile, ProtocolInfo protocol, Set<? extends OpenOption> access) throws Exception {
            throw new InterruptedException();
        }

        @Override
        public long getBytesTransferred() {
            return 0;
        }

        @Override
        public long getTransferTime() {
            return 0;
        }

        @Override
        public long getLastTransferred() {
            return 0;
        }
    }

    private static class ThreadBlockingMover implements MoverProtocol {

        @Override
        public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile, ProtocolInfo protocol, Set<? extends OpenOption> access) throws Exception {
            Thread.sleep(5000);
        }

        @Override
        public long getBytesTransferred() {
            return 0;
        }

        @Override
        public long getTransferTime() {
            return 0;
        }

        @Override
        public long getLastTransferred() {
            return 0;
        }
    }

    private static class BlockingCompletionHandler implements CompletionHandler<Void, Void> {
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile Throwable t;

        @Override
        public void completed(Void result, Void attachment) {
            countDownLatch.countDown();
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            t = exc;
            countDownLatch.countDown();
        }

        public void await() throws InterruptedException {
            countDownLatch.await();
        }

        public Optional<Throwable> getThrowable() {
            return Optional.ofNullable(t);
        }
    }

}