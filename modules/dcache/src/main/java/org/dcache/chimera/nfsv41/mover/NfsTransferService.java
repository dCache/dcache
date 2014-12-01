package org.dcache.chimera.nfsv41.mover;

import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.CompletionHandler;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;

import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.movers.MoverProtocolMover;
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
        implements MoverFactory, TransferService<MoverProtocolMover>, CellCommandListener
{
    private static final Logger _log = LoggerFactory.getLogger(NfsTransferService.class);
    private NFSv4MoverHandler _nfsIO;
    private boolean _withGss;
    private InetSocketAddress[] _localSocketAddresses;
    private CellStub _door;
    private PostTransferService _postTransferService;
    private boolean _sortMultipathList;

    public void init() throws ChimeraFsException, IOException, GSSException, OncRpcException {

        String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");
        PortRange portRange;
        if (dcachePorts != null) {
            portRange = PortRange.valueOf(dcachePorts);
        } else {
            portRange = new PortRange(0);
        }

        _nfsIO = new NFSv4MoverHandler(portRange, _withGss, getCellName());
        _localSocketAddresses = localSocketAddresses(NetworkUtils.getLocalAddresses(), _nfsIO.getLocalAddress().getPort());
        /*
         * we assume, that client's can't handle multipath list correctly
         * if data server has multiple IPv4 addresses. (RHEL6 and clones)
         */
        int ipv4Count = 0;
        for (InetSocketAddress addr : _localSocketAddresses) {
            if (addr.getAddress() instanceof Inet4Address) {
                ipv4Count++;
            }
        }
        _sortMultipathList = ipv4Count > 1;

        _door = new CellStub(getCellEndpoint());
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
        return new MoverProtocolMover(handle, message, pathToDoor, this, _postTransferService,
                new NFSv41ProtocolMover(getCellEndpoint()));
    }

    @Override
    public Cancellable execute(MoverProtocolMover transfer, final CompletionHandler<Void,Void> completionHandler) {
        try {
            NFS4ProtocolInfo nfs4ProtocolInfo = (NFS4ProtocolInfo) transfer.getProtocolInfo();

            /*
             * for backward compatibility with old pools, door will always send
             * legacy version of stateid. We keep internally new version, but have to
             * return back legacy one as request may come from old door.
             */
            org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid = nfs4ProtocolInfo.stateId();
            stateid4 stateid = new stateid4(legacyStateid.other, legacyStateid.seqid.value);
            final RepositoryChannel repositoryChannel = transfer.openChannel();
            final MoverBridge moverBridge = new MoverBridge((ManualMover) transfer.getMover(),
                    transfer.getFileAttributes().getPnfsId(), stateid, repositoryChannel, transfer.getIoMode(), transfer.getIoHandle());
            _nfsIO.addHandler(moverBridge);

            CellPath directDoorPath = new CellPath(transfer.getPathToDoor().getDestinationAddress());
            final InetSocketAddress[] localSocketAddresses = localSocketAddresses(nfs4ProtocolInfo.getSocketAddress().getAddress());
            _door.send(directDoorPath, new PoolPassiveIoFileMessage<>(getCellName(), localSocketAddresses, legacyStateid));

            /* An NFS mover doesn't complete until it is cancelled (the door sends a mover kill
             * message when the file is closed).
             */
            return new Cancellable() {
                @Override
                public void cancel() {
                    _nfsIO.removeHandler(moverBridge);
                    try {
                        repositoryChannel.close();
                    } catch (IOException e) {
                        _log.error("failed to close RAF", e);
                    }
                    completionHandler.completed(null, null);
                }
            };
        } catch (Throwable e) {
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

    private InetSocketAddress[] localSocketAddresses(InetAddress remote) throws SocketException {
        InetSocketAddress[] addressesToUse;
        if (_sortMultipathList) {
            addressesToUse = new InetSocketAddress[_localSocketAddresses.length + 1];
            System.arraycopy(_localSocketAddresses, 0, addressesToUse, 1, _localSocketAddresses.length);
            InetSocketAddress preferredInterface = new InetSocketAddress(
                    NetworkUtils.getLocalAddress(remote), _nfsIO.getLocalAddress().getPort());
            addressesToUse[0] = preferredInterface;
        } else {
            addressesToUse = _localSocketAddresses;
        }
        return addressesToUse;
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
