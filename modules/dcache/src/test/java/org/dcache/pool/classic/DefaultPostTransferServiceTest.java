package org.dcache.pool.classic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.TransferLifeCycle;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

public class DefaultPostTransferServiceTest {

    private static final PnfsId PNFSID = new PnfsId("000000000000000000000000000000000001");
    private static final InetSocketAddress REMOTE_FROM_DOOR =
          new InetSocketAddress("192.0.2.10", 443);
    private static final InetSocketAddress REMOTE_CONNECTED =
          new InetSocketAddress("192.0.2.20", 24100);
    private static final InetSocketAddress LOCAL = new InetSocketAddress("198.51.100.5", 40000);

    private DefaultPostTransferService service;
    private TransferLifeCycle transferLifeCycle;
    private Mover<IpProtocolInfo> mover;
    private MoverInfoMessage moverInfo;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        transferLifeCycle = mock(TransferLifeCycle.class);

        service = new DefaultPostTransferService();
        service.setPoolName("pool_a");
        service.setTransferLifeCycle(transferLifeCycle);
        service.setCellEndpoint(mock(CellEndpoint.class));
        service.init();

        IpProtocolInfo protocolInfo = mock(IpProtocolInfo.class);
        when(protocolInfo.getProtocol()).thenReturn("RemoteHttpsDataTransfer");
        when(protocolInfo.getSocketAddress()).thenReturn(REMOTE_FROM_DOOR);

        mover = mock(Mover.class);
        when(mover.getProtocolInfo()).thenReturn(protocolInfo);
        when(mover.getFileAttributes()).thenReturn(FileAttributes.ofPnfsId(PNFSID));
        when(mover.getPathToDoor()).thenReturn(new CellPath("door"));
        when(mover.getLocalEndpoint()).thenReturn(Optional.empty());
        when(mover.getRemoteEndpoint()).thenReturn(Optional.empty());

        moverInfo = new MoverInfoMessage(new CellAddressCore("pool_a", "poolDomain"), PNFSID);
    }

    @Test
    public void shouldSendNoEndMarkerForActiveMoverThatNeverConnected() {
        // e.g. an HTTP-TPC pull that failed on the DNS lookup of the source
        when(mover.isActive()).thenReturn(true);

        service.sendFinished(mover, moverInfo);

        verify(transferLifeCycle, never()).onEnd(any(), any(), any());
    }

    @Test
    public void shouldSendEndMarkerForConnectionOfActiveMover() {
        when(mover.isActive()).thenReturn(true);
        when(mover.getLocalEndpoint()).thenReturn(Optional.of(LOCAL));
        when(mover.getRemoteEndpoint()).thenReturn(Optional.of(REMOTE_CONNECTED));

        service.sendFinished(mover, moverInfo);

        verify(transferLifeCycle).onEnd(REMOTE_CONNECTED, LOCAL, moverInfo);
    }

    @Test
    public void shouldSendNoEndMarkerForPassiveMoverNobodyConnectedTo() {
        when(mover.isPassive()).thenReturn(true);
        when(mover.getLocalEndpoint()).thenReturn(Optional.of(LOCAL));

        service.sendFinished(mover, moverInfo);

        verify(transferLifeCycle, never()).onEnd(any(), any(), any());
    }

    @Test
    public void shouldSendEndMarkerForPassiveMoverWithClient() {
        when(mover.isPassive()).thenReturn(true);
        when(mover.getLocalEndpoint()).thenReturn(Optional.of(LOCAL));
        when(mover.getRemoteEndpoint()).thenReturn(Optional.of(REMOTE_CONNECTED));

        service.sendFinished(mover, moverInfo);

        verify(transferLifeCycle).onEnd(REMOTE_CONNECTED, LOCAL, moverInfo);
    }
}
