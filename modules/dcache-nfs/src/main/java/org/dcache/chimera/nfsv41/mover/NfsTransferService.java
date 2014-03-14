package org.dcache.chimera.nfsv41.mover;

import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.Args;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;
import org.dcache.xdr.OncRpcException;

/**
 * Factory and transfer service for NFS movers.
 *
 * @since 1.9.11
 */
public class NfsTransferService extends AbstractCellComponent
        implements MoverFactory, TransferService<NfsMover>, CellCommandListener
{
    private static final Logger _log = LoggerFactory.getLogger(NfsTransferService.class);
    private NFSv4MoverHandler _nfsIO;
    private boolean _withGss;
    private InetSocketAddress[] _localSocketAddresses;
    private CellStub _door;
    private PostTransferService _postTransferService;
    private FaultListener _faultListener;
    private final long _bootVerifier = System.currentTimeMillis();

    public void init() throws IOException, GSSException, OncRpcException {

        String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");
        PortRange portRange;
        if (dcachePorts != null) {
            portRange = PortRange.valueOf(dcachePorts);
        } else {
            portRange = new PortRange(0);
        }

        _nfsIO = new NFSv4MoverHandler(portRange, _withGss, getCellName());
        _localSocketAddresses =
                localSocketAddresses(NetworkUtils.getLocalAddresses(), _nfsIO.getLocalAddress().getPort());

        _door = new CellStub(getCellEndpoint());
    }

    @Required
    public void setFaultListener(FaultListener faultListener) {
        _faultListener = faultListener;
    }

    @Required
    public void setPostTransferService(PostTransferService postTransferService) {
        _postTransferService = postTransferService;
    }

    public void shutdown() throws IOException {
        _nfsIO.shutdown();
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor) throws CacheException
    {
        return new NfsMover(handle, message, pathToDoor, this, _postTransferService);
    }

    @Override
    public Cancellable execute(final NfsMover mover, final CompletionHandler<Void,Void> completionHandler) {
        try {

            final Cancellable cancellableMover = mover.enable(completionHandler);

            CellPath directDoorPath = new CellPath(mover.getPathToDoor().getDestinationAddress());
            final org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateId = mover.getProtocolInfo().stateId();
            _door.notify(directDoorPath,
                         new PoolPassiveIoFileMessage<>(getCellName(), _localSocketAddresses, legacyStateId,
                                                        _bootVerifier));

            /* An NFS mover doesn't complete until it is cancelled (the door sends a mover kill
             * message when the file is closed).
             */
            return cancellableMover;
        } catch (DiskErrorCacheException e) {
            _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED,
                    e.getMessage(), e));
            completionHandler.failed(e, null);
        } catch (NoRouteToCellException e) {
            completionHandler.failed(e, null);
        }
        return null;
    }

    public NFSv4MoverHandler getNfsMoverHandler() {
        return _nfsIO;
    }

    public void setEnableGss(boolean withGss) {
        _withGss = withGss;
    }

    private InetSocketAddress[] localSocketAddresses(List<InetAddress> addresses, int port) {
        InetSocketAddress[] socketAddresses = new InetSocketAddress[addresses.size()];
        int i = 0;
        for(InetAddress address: addresses) {
            socketAddresses[i] = new InetSocketAddress(address, port);
            i++;
        }
        return socketAddresses;
    }

    public final static String fh_nfs_stats =
            "nfs stats [-c] # show nfs requests statstics\n\n" +
            "  Print nfs operation statistics.\n" +
            "    -c clear current statistics values";
    public final static String hh_nfs_stats = " [-c] # show nfs mover statstics";
    public String ac_nfs_stats(Args args) {

        RequestExecutionTimeGauges<String> gauges = _nfsIO.getNFSServer().getStatistics();
        StringBuilder sb = new StringBuilder();
        sb.append("Stats:").append("\n").append(gauges);

        if (args.hasOption("c")) {
            gauges.reset();
        }

        return sb.toString();
    }

    public final static String hh_nfs_sessions = " # show nfs sessions";
    public String ac_nfs_sessions(Args args) {

       StringBuilder sb = new StringBuilder();
        for (NFS4Client client : _nfsIO.getNFSServer().getClients()) {
            sb.append(client).append('\n');
            for (NFSv41Session session : client.sessions()) {
                sb.append("  ")
                        .append(session)
                        .append(" slots (max/used): ")
                        .append(session.getHighestSlot())
                        .append('/')
                        .append(session.getHighestUsedSlot())
                        .append('\n');
            }
        }
        return sb.toString();
    }
}
