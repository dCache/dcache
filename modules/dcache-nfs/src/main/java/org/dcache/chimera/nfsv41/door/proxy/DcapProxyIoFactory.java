package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import diskCacheV111.util.Base64;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.CellStub;
import org.dcache.chimera.JdbcFs;
import org.dcache.commons.util.NDC;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.StateDisposeListener;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.xdr.RpcCall;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DcapProxyIoFactory extends AbstractCell {

    private static final Logger _log = LoggerFactory.getLogger(DcapProxyIoFactory.class);

    private PnfsHandler _pnfsHandler;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private String _ioQueue;
    /**
     * Given that the timeout is pretty short, the retry period has to be rather
     * small too.
     */
    private final static long NFS_RETRY_PERIOD = 500; // In millis

    private final ConcurrentHashMap<Integer, DcapTransfer> _pendingIO =
            new ConcurrentHashMap<>();

    private TransferRetryPolicy _retryPolicy;

    private JdbcFs _fileFileSystemProvider;

    private final Cache<stateid4, ProxyIoAdapter> _prioxyIO
            = CacheBuilder.newBuilder()
            .build();

    public void setFileSystemProvider(JdbcFs fs) {
        _fileFileSystemProvider = fs;
    }

    public void setPnfsHandler(PnfsHandler pnfs) {
        _pnfsHandler = pnfs;
    }

    public void setBillingStub(CellStub stub) {
        _billingStub = stub;
    }

    public void setIoQueue(String queue) {
        _ioQueue = queue;
    }

    public void setRetryPolicy(TransferRetryPolicy retryPolicy) {
        _retryPolicy = retryPolicy;
    }

    public void setPoolManager(CellPath poolManager) {
        _poolManagerStub = new CellStub(this, poolManager);
    }

    public DcapProxyIoFactory(String cellName, String arguments) {
        super(cellName, arguments);
    }

    ProxyIoAdapter getAdapter(Inode inode, Subject subject, InetSocketAddress client, boolean isWrite) throws CacheException, InterruptedException, IOException {

        final DCapProtocolInfo protocolInfo = new DCapProtocolInfo("DCap", 3, 0, client);
        protocolInfo.door( new CellPath(new CellAddressCore(getCellName(), getCellDomainName())));
        protocolInfo.isPassive(true);

        final PnfsId  pnfsId = new PnfsId(_fileFileSystemProvider.inodeFromBytes(inode.getFileId()).toString());
	NDC.push(pnfsId.toString());
        final DcapTransfer transfer = new DcapTransfer(_pnfsHandler, subject);
        DcapTransfer.initSession();
	final int session = (int)transfer.getSessionId();
	protocolInfo.setSessionId(session);

        transfer.setProtocolInfo(protocolInfo);
        transfer.setCellName(getCellName());
        transfer.setDomainName(getCellDomainName());
        transfer.setBillingStub(_billingStub);
        transfer.setPoolStub(_poolManagerStub);
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPnfsId(pnfsId);
        transfer.setClientAddress(client);
        transfer.readNameSpaceEntry();

        _pendingIO.put(session, transfer);
        transfer.setWrite(isWrite);
        transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
        PoolPassiveIoFileMessage<byte[]> redirect = transfer.waitForRedirect(NFS_RETRY_PERIOD);

        return new DcapChannelImpl(redirect.socketAddress(), session,
                Base64.byteArrayToBase64(redirect.challange()).getBytes(Charsets.US_ASCII),
                transfer.getFileAttributes().getSize());
    }

    ProxyIoAdapter getOrCreateProxy(final Inode inode, final stateid4 stateid, final CompoundContext context, final boolean isWrite) throws ChimeraNFSException {

        try {
            ProxyIoAdapter adapter = _prioxyIO.get(stateid,
                    new Callable<ProxyIoAdapter>() {

                        @Override
                        public ProxyIoAdapter call() throws Exception {
                            final NFS4State state = context.getStateHandler().getClientIdByStateId(stateid).state(stateid);
                            final ProxyIoAdapter adapter = createIoAdapter(inode, context, isWrite);

                            state.addDisposeListener(new StateDisposeListener() {

                                @Override
                                public void notifyDisposed(NFS4State state) {
                                    tryToClose(adapter);
                                    _prioxyIO.invalidate(state.stateid());
                                }
                            });

                            return adapter;
                        }
                    });

            return adapter;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            _log.error("failed to create IO adapter: {}", t.getMessage());
            if (t instanceof ChimeraNFSException) {
                throw (ChimeraNFSException) t;
            }
            int status = nfsstat.NFSERR_IO;
            if ((t instanceof CacheException) && ((CacheException) t).getRc() != CacheException.BROKEN_ON_TAPE) {
                status = nfsstat.NFSERR_DELAY;
            }
            throw new ChimeraNFSException(status, t.getMessage());
        }
    }

    private static void tryToClose(ProxyIoAdapter adapter) {
        try {
            adapter.close();
        } catch (IOException e) {
            _log.error("failed to close io adapter: ", e.getMessage());
        }
    }

    ProxyIoAdapter createIoAdapter(final Inode inode, final CompoundContext context, boolean isWrite)
            throws CacheException, InterruptedException, IOException {

        RpcCall call = context.getRpcCall();
        return getAdapter(inode, call.getCredential().getSubject(),
                call.getTransport().getRemoteSocketAddress(), isWrite);
    }

    public void startAdapter() throws InterruptedException, ExecutionException {
        doInit();
    }

    public void messageArrived(PoolPassiveIoFileMessage<byte[]> message) {

        final int session = (int)message.getId();
        final DcapTransfer transfer = _pendingIO.remove(session);
        if (transfer != null) {
            transfer.redirect(message);
        }
    }

    public void messageArrived(DoorTransferFinishedMessage transferFinishedMessage) {
        // nop
    }

    private static class DcapTransfer extends RedirectedTransfer<PoolPassiveIoFileMessage<byte[]>> {

        public DcapTransfer(PnfsHandler pnfs, Subject ioSubject) {
            super(pnfs, Subjects.ROOT, ioSubject, new FsPath("/"));
        }
    }
}
