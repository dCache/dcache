package org.dcache.chimera.nfsv41.mover;

import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.pool.classic.CompletionHandler;
import org.dcache.pool.classic.MoverExecutorService;
import org.dcache.pool.classic.PoolIORequest;
import org.dcache.pool.classic.PoolIOTransfer;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrEncodingStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @since 1.9.11
 */
public class NfsExcecutionService implements MoverExecutorService {

    private static final Logger _log = LoggerFactory.getLogger(NfsExcecutionService.class);
    private final NFSv4MoverHandler _nfsIO;

    public NfsExcecutionService() throws ChimeraFsException, OncRpcException, IOException {

            String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");
            PortRange portRange;
            if (dcachePorts != null) {
                portRange = PortRange.valueOf(dcachePorts);
            } else {
                portRange = new PortRange(0);
            }
            _nfsIO = new NFSv4MoverHandler(portRange);
    }


    public void shutdown() {
        _nfsIO.shutdown();
    }

    @Override
    public Future execute(PoolIORequest request, final CompletionHandler completionHandler) {

        try {
            NFS4ProtocolInfo nfs4ProtocolInfo = (NFS4ProtocolInfo) request.getTransfer().getProtocolInfo();
            PoolIOTransfer transfer = request.getTransfer();

            stateid4 stateid = nfs4ProtocolInfo.stateId();
            ReplicaDescriptor descriptor = transfer.getIoHandle();
            String openMode = transfer.getIoMode() == IoMode.WRITE ? "rw" : "r";
            final RandomAccessFile raf = new RandomAccessFile(descriptor.getFile(), openMode);

            final MoverBridge moverBridge = new MoverBridge((ManualMover) transfer.getMover(),
                    request.getPnfsId(), stateid, raf.getChannel(), transfer.getIoMode(), descriptor);
            _nfsIO.addHandler(moverBridge);

            InetAddress localAddress = NetworkUtils.
                    getLocalAddress(nfs4ProtocolInfo.getSocketAddress().getAddress());

            XdrEncodingStream xdr = new XdrBuffer(128);
            xdr.beginEncoding();
            stateid.xdrEncode(xdr);
            xdr.endEncoding();
            byte[] d = xdr.body().array();

            PoolPassiveIoFileMessage msg = new PoolPassiveIoFileMessage(request.getCellEndpoint().getCellInfo().getCellName(),
                    new InetSocketAddress(localAddress, _nfsIO.getLocalAddress().getPort()), d);

            CellPath cellpath = nfs4ProtocolInfo.door();
            request.getCellEndpoint().sendMessage(new CellMessage(cellpath, msg));

            return new Future() {

                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    _nfsIO.removeHandler(moverBridge);
                    try {
                        raf.close();
                    } catch (IOException e) {
                        _log.error("failed to close RAF", e);
                    }
                    completionHandler.completed(null, null);
                    return true;
                }

                @Override
                public boolean isCancelled() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                @Override
                public boolean isDone() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                @Override
                public Object get() throws InterruptedException, ExecutionException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                @Override
                public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        } catch (Throwable e) {
            completionHandler.failed(e, null);
        }
        return null;
    }
}
