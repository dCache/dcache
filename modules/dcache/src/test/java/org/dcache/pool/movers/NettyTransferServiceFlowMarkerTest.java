package org.dcache.pool.movers;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import diskCacheV111.vehicles.ProtocolInfo;
import io.netty.channel.local.LocalAddress;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.security.auth.Subject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that flow markers of a Netty mover describe the connection the client makes to the pool,
 * not the client's connection to the door.
 */
public class NettyTransferServiceFlowMarkerTest {

    private static final InetSocketAddress CLIENT_DATA_CONNECTION =
          new InetSocketAddress("203.0.113.20", 47742);
    private static final InetSocketAddress POOL_DATA_CONNECTION =
          new InetSocketAddress("198.51.100.55", 47548);

    private ScheduledExecutorService scheduler;
    private TransferLifeCycle transferLifeCycle;
    private NettyMover<ProtocolInfo> mover;
    private ProtocolInfo protocolInfo;
    private Subject subject;
    private NettyTransferService<ProtocolInfo>.NettyMoverChannel channel;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        transferLifeCycle = mock(TransferLifeCycle.class);
        protocolInfo = mock(ProtocolInfo.class);
        subject = new Subject();

        mover = mock(NettyMover.class);
        given(mover.getProtocolInfo()).willReturn(protocolInfo);
        given(mover.getSubject()).willReturn(subject);

        NettyTransferService<ProtocolInfo> service = new NettyTransferService<>("test") {
            @Override
            protected void sendAddressToDoor(NettyMover<ProtocolInfo> mover,
                  InetSocketAddress localEndpoint) {
            }

            @Override
            protected UUID createUuid(ProtocolInfo protocolInfo) {
                return UUID.randomUUID();
            }
        };
        service.setTransferLifeCycle(transferLifeCycle);

        Field timeoutScheduler = NettyTransferService.class.getDeclaredField("timeoutScheduler");
        timeoutScheduler.setAccessible(true);
        timeoutScheduler.set(service, scheduler);

        channel = service.new NettyMoverChannel(UUID.randomUUID(), mover,
              mock(MoverChannel.class), 60_000, mock(CompletionHandler.class),
              type -> {}, checksum -> {});
    }

    @After
    public void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    public void shouldSendStartMarkerForTheConnectionTheClientMadeToThePool() {
        channel.connected(CLIENT_DATA_CONNECTION, POOL_DATA_CONNECTION);

        verify(transferLifeCycle).onStart(CLIENT_DATA_CONNECTION, POOL_DATA_CONNECTION,
              protocolInfo, subject);
    }

    @Test
    public void shouldRecordEndpointsOfDataConnectionOnMoverForEndMarker() {
        channel.connected(CLIENT_DATA_CONNECTION, POOL_DATA_CONNECTION);

        verify(mover).setRemoteEndpoint(CLIENT_DATA_CONNECTION);
        verify(mover).setLocalEndpoint(POOL_DATA_CONNECTION);
    }

    @Test
    public void shouldNotSendStartMarkerBeforeClientConnects() {
        verify(transferLifeCycle, never()).onStart(any(), any(), any(), any());
        verify(mover, never()).setRemoteEndpoint(any());
    }

    @Test
    public void shouldSendOnlyOneStartMarkerWhenFurtherConnectionsAttach() {
        channel.connected(CLIENT_DATA_CONNECTION, POOL_DATA_CONNECTION);
        channel.connected(CLIENT_DATA_CONNECTION, POOL_DATA_CONNECTION);
        channel.connected(new InetSocketAddress("203.0.113.20", 50000), POOL_DATA_CONNECTION);

        verify(transferLifeCycle, times(1)).onStart(any(), any(), any(), any());
        verify(mover, times(1)).setRemoteEndpoint(CLIENT_DATA_CONNECTION);
    }

    @Test
    public void shouldIgnoreConnectionsThatAreNotIpSockets() {
        channel.connected(new LocalAddress("client"), new LocalAddress("pool"));

        verify(transferLifeCycle, never()).onStart(any(), any(), any(), any());
        verify(mover, never()).setRemoteEndpoint(any());
    }

    @Test
    public void nettyMoverShouldBePassive() {
        NettyMover<ProtocolInfo> realMover = mock(NettyMover.class,
              org.mockito.Mockito.CALLS_REAL_METHODS);

        assertTrue(realMover.isPassive());
    }
}
