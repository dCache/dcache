package org.dcache.chimera.nfsv41.mover;

import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.util.List;

import diskCacheV111.vehicles.PoolPassiveIoFileMessage;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.MoverExecutorService;
import org.dcache.pool.classic.PoolIOTransfer;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;

/**
 *
 * @since 1.9.11
 */
public class NfsExcecutionService extends AbstractCellComponent implements MoverExecutorService {

    private static final Logger _log = LoggerFactory.getLogger(NfsExcecutionService.class);
    private NFSv4MoverHandler _nfsIO;
    private boolean _withGss;
    private InetSocketAddress[] _localSocketAddresses;

    public void init() throws ChimeraFsException, IOException, GSSException {

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
    }

    public void shutdown() throws IOException {
        _nfsIO.shutdown();
    }

    @Override
    public Cancellable execute(PoolIOTransfer transfer, final CompletionHandler<Void,Void> completionHandler) {
        try {
            NFS4ProtocolInfo nfs4ProtocolInfo = (NFS4ProtocolInfo) transfer.getProtocolInfo();

            stateid4 stateid = nfs4ProtocolInfo.stateId();
            ReplicaDescriptor descriptor = transfer.getIoHandle();
            String openMode = transfer.getIoMode() == IoMode.WRITE ? "rw" : "r";
            final RepositoryChannel repositoryChannel = new FileRepositoryChannel(descriptor.getFile(), openMode);

            final MoverBridge moverBridge = new MoverBridge((ManualMover) transfer.getMover(),
                    transfer.getFileAttributes().getPnfsId(), stateid, repositoryChannel, transfer.getIoMode(), descriptor);
            _nfsIO.addHandler(moverBridge);

            transfer.sendToDoor(new PoolPassiveIoFileMessage<>(getCellName(), _localSocketAddresses, stateid));

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
}
