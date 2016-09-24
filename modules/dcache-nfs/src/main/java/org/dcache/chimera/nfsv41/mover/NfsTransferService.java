package org.dcache.chimera.nfsv41.mover;

import com.google.common.io.Files;

import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.net.Inet4Address;

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
    private boolean _sortMultipathList;

    private int _minTcpPort;
    private int _maxTcpPort;

    /**
     * file to store TCP port number used by pool.
     */
    private File _tcpPortFile;

    public void init() throws IOException, GSSException, OncRpcException {

        _door = new CellStub(getCellEndpoint());
        PortRange portRange;
        int minTcpPort = _minTcpPort;
        int maxTcpPort = _maxTcpPort;

        try {
            String line = Files.readFirstLine(_tcpPortFile, StandardCharsets.US_ASCII);
            int savedPort = Integer.parseInt(line);
            if (savedPort >= _minTcpPort && savedPort <= _maxTcpPort) {
                /*
                 *if saved port with in the range, then restrict range to a single port
                 * to enforce it.
                 */
                minTcpPort = savedPort;
                maxTcpPort = savedPort;
            }
        } catch (NumberFormatException e) {
            // garbage in the file.
            _log.warn("Invalid content in the port file {} : {}", _tcpPortFile, e.getMessage());
        } catch (FileNotFoundException e) {
        }

        boolean bound = false;
        int retry = 3;
        BindException bindException = null;
        do {
            retry--;
            portRange = new PortRange(minTcpPort, maxTcpPort);
            try {
                _nfsIO = new NFSv4MoverHandler(portRange, _withGss, getCellName(), _door, _bootVerifier);
                bound = true;
            } catch (BindException e) {
                bindException = e;
                minTcpPort = _minTcpPort;
                maxTcpPort = _maxTcpPort;
            }
        } while (!bound && retry > 0);

        if (!bound) {
            throw new BindException("Can't bind to a port within the rage: " + portRange + " : " + bindException);
        }

        _localSocketAddresses = localSocketAddresses(NetworkUtils.getLocalAddresses(), _nfsIO.getLocalAddress().getPort());

        // if we had a port range, then store selected port for the next time.
        if (minTcpPort != maxTcpPort) {
            _tcpPortFile.delete();
            Files.write(Integer.toString(_nfsIO.getLocalAddress().getPort()), _tcpPortFile, StandardCharsets.US_ASCII);
        }

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
    }

    @Required
    public void setFaultListener(FaultListener faultListener) {
        _faultListener = faultListener;
    }

    @Required
    public void setPostTransferService(PostTransferService postTransferService) {
        _postTransferService = postTransferService;
    }

    @Required
    public void setMinTcpPort(int minPort) {
        _minTcpPort = minPort;
    }

    @Required
    public void setMaxTcpPort(int maxPort) {
        _maxTcpPort = maxPort;
    }

    public void setTcpPortFile(File path) {
        _tcpPortFile = path;
    }

    public void shutdown() throws IOException {
        _nfsIO.shutdown();
        _nfsIO.getNFSServer().getStateHandler().shutdown();
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
            final InetSocketAddress[] localSocketAddresses = localSocketAddresses(mover);
            _door.notify(directDoorPath,
                         new PoolPassiveIoFileMessage<>(getCellName(), localSocketAddresses, legacyStateId,
                                                        _bootVerifier));

            /* An NFS mover doesn't complete until it is cancelled (the door sends a mover kill
             * message when the file is closed).
             */
            return cancellableMover;
        } catch (DiskErrorCacheException e) {
            _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED,
                    e.getMessage(), e));
            completionHandler.failed(e, null);
        } catch (NoRouteToCellException | SocketException e) {
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

    private InetSocketAddress[] localSocketAddresses(NfsMover mover) throws SocketException {

        InetSocketAddress[] addressesToUse;
        if (_sortMultipathList) {
            addressesToUse = new InetSocketAddress[_localSocketAddresses.length + 1];
            System.arraycopy(_localSocketAddresses, 0, addressesToUse, 1, _localSocketAddresses.length);

            InetSocketAddress preferredInterface = new InetSocketAddress(
                    NetworkUtils.getLocalAddress(mover.getProtocolInfo().getSocketAddress().getAddress()),
                    _nfsIO.getLocalAddress().getPort());
            addressesToUse[0] = preferredInterface;
        } else {
            addressesToUse = _localSocketAddresses;
        }

        return addressesToUse;
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
        for (NFS4Client client : _nfsIO.getNFSServer().getStateHandler().getClients()) {
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
