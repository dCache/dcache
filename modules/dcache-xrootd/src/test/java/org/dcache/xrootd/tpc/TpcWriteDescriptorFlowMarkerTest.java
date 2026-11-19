package org.dcache.xrootd.tpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.pool.XrootdTransferService;
import org.dcache.xrootd.tpc.protocol.messages.InboundReadResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that, when the pool is the destination of an xrootd third-party copy, the flow marker
 * describes the connection over which the file arrives from the source.
 */
public class TpcWriteDescriptorFlowMarkerTest {

    private static final InetSocketAddress SOURCE =
          new InetSocketAddress("203.0.113.30", 1094);
    private static final InetSocketAddress REDIRECTED_SOURCE =
          new InetSocketAddress("203.0.113.31", 21468);
    private static final InetSocketAddress POOL =
          new InetSocketAddress("198.51.100.55", 44034);

    private ScheduledExecutorService executor;
    private NettyTransferService<XrootdProtocolInfo>.NettyMoverChannel moverChannel;
    private TpcWriteDescriptor descriptor;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();

        moverChannel = mock(NettyTransferService.NettyMoverChannel.class);
        given(moverChannel.getProtocolInfo()).willReturn(mock(XrootdProtocolInfo.class));

        XrootdTransferService service = mock(XrootdTransferService.class);
        given(service.getThirdPartyShutdownExecutor()).willReturn(executor);
        given(service.getTpcClientChunkSize()).willReturn(1 << 20);
        given(service.getTpcClientPlugins()).willReturn(Collections.emptyList());

        XrootdTpcInfo info = mock(XrootdTpcInfo.class);
        given(info.getSrcHost()).willReturn("127.0.0.1");
        given(info.getSrcPort()).willReturn(1094);

        descriptor = new TpcWriteDescriptor(moverChannel, false,
              mock(ChannelHandlerContext.class), service, "user", info, null);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void shouldNotReportAnyConnectionBeforeDataArrives() {
        verify(moverChannel, never()).connected(any(), any());
    }

    @Test
    public void shouldReportConnectionToSourceWhenFirstDataArrives() throws Exception {
        givenThirdPartyClientConnectedTo(SOURCE);

        descriptor.write(someData());

        verify(moverChannel).connected(SOURCE, POOL);
    }

    @Test
    public void shouldReportConnectionOnlyOnce() throws Exception {
        givenThirdPartyClientConnectedTo(SOURCE);

        descriptor.write(someData());
        descriptor.write(someData());
        descriptor.write(someData());

        verify(moverChannel, times(1)).connected(any(), any());
    }

    @Test
    public void shouldReportConnectionOfClientThatReceivesDataAfterRedirect() throws Exception {
        givenThirdPartyClientConnectedTo(SOURCE);
        givenThirdPartyClientConnectedTo(REDIRECTED_SOURCE);

        descriptor.write(someData());

        verify(moverChannel).connected(REDIRECTED_SOURCE, POOL);
        verify(moverChannel, never()).connected(SOURCE, POOL);
    }

    @Test
    public void shouldNotFailWhenClientHasNoChannel() throws Exception {
        setClient(mock(XrootdTpcClient.class));

        descriptor.write(someData());

        verify(moverChannel, never()).connected(any(), any());
    }

    private void givenThirdPartyClientConnectedTo(InetSocketAddress remote) throws Exception {
        Channel channel = mock(Channel.class);
        given(channel.remoteAddress()).willReturn(remote);
        given(channel.localAddress()).willReturn(POOL);
        ChannelFuture future = mock(ChannelFuture.class);
        given(future.channel()).willReturn(channel);
        XrootdTpcClient client = mock(XrootdTpcClient.class);
        given(client.getChannelFuture()).willReturn(future);
        setClient(client);
    }

    private void setClient(XrootdTpcClient client) throws Exception {
        Field field = TpcWriteDescriptor.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(descriptor, client);
    }

    private static InboundReadResponse someData() {
        InboundReadResponse response = mock(InboundReadResponse.class);
        given(response.getWriteOffset()).willReturn(0L);
        given(response.toByteBuffers()).willReturn(new java.nio.ByteBuffer[0]);
        return response;
    }
}
