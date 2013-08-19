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

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
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

    public void init() throws ChimeraFsException, IOException, GSSException, OncRpcException {

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

            final stateid4 stateid = mover.getStateId();
            final RepositoryChannel repositoryChannel = mover.open();
            _nfsIO.add(mover);

            CellPath directDoorPath = new CellPath(mover.getPathToDoor().getDestinationAddress());
            _door.send(directDoorPath, new PoolPassiveIoFileMessage<>(getCellName(), _localSocketAddresses, stateid));

            /* An NFS mover doesn't complete until it is cancelled (the door sends a mover kill
             * message when the file is closed).
             */
            return new Cancellable() {
                @Override
                public void cancel() {
                    _nfsIO.remove(mover);
                    try {
                        repositoryChannel.close();
                    } catch (IOException e) {
                        _log.error("failed to close RAF", e);
                    }
                    completionHandler.completed(null, null);
                }
            };
        } catch (DiskErrorCacheException e) {
            _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED,
                    e.getMessage(), e));
            completionHandler.failed(e, null);
        } catch (NoRouteToCellException e) {
            completionHandler.failed(e, null);
        }
        return null;
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

    public final static String hh_nfs_stats = " # show nfs mover statstics";
    public String ac_nfs_stats(Args args) {
        StringBuilder sb = new StringBuilder();
        sb.append("Stats:").append("\n").append(_nfsIO.getNFSServer().getStatistics());

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
