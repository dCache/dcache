package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.base.Charsets;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.CellStub;
import org.dcache.chimera.JdbcFs;
import org.dcache.commons.util.NDC;
import org.dcache.nfs.vfs.Inode;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.TransferRetryPolicy;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 *
 */
public class DcapProxyIoFactory extends AbstractCell {

    private PnfsHandler _pnfsHandler;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private String _ioQueue;
    /**
     * Given that the timeout is pretty short, the retry period has to be rather
     * small too.
     */
    private final static long NFS_RETRY_PERIOD = 500; // In millis
    private final AtomicInteger _session = new AtomicInteger();

    private final ConcurrentHashMap<Integer, DcapTransfer> _pendingIO =
            new ConcurrentHashMap<>();

    private TransferRetryPolicy _retryPolicy;

    private JdbcFs _fileFileSystemProvider;

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

    ProxyIoAdapter getAdapter(Inode inode, Subject subject, InetSocketAddress client) throws CacheException, InterruptedException, IOException {

        final int session = nextSession();
        final DCapProtocolInfo protocolInfo = new DCapProtocolInfo("DCap", 3, 0, client);
        protocolInfo.door( new CellPath(new CellAddressCore(getCellName(), getCellDomainName())));
        protocolInfo.setSessionId(session);
        protocolInfo.isPassive(true);

        final PnfsId  pnfsId = new PnfsId(_fileFileSystemProvider.inodeFromBytes(inode.getFileId()).toString());
	NDC.push(pnfsId.toString());
        final DcapTransfer transfer = new DcapTransfer(_pnfsHandler, subject);
        DcapTransfer.initSession();
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
        transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
        try {
            PoolPassiveIoFileMessage<byte[]> redirect = transfer.waitForRedirect(NFS_RETRY_PERIOD);

            return new DcapChannelImpl(redirect.socketAddress(), session,
                    Base64.byteArrayToBase64(redirect.challange()).getBytes(Charsets.US_ASCII),
                    transfer.getFileAttributes().getSize());
        } catch (CacheException | InterruptedException | IOException e) {
            transfer.killMover(0);
            throw e;
        }
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

    private int nextSession() {
        return _session.getAndIncrement();
    }

    private static class DcapTransfer extends RedirectedTransfer<PoolPassiveIoFileMessage<byte[]>> {

        public DcapTransfer(PnfsHandler pnfs, Subject ioSubject) {
            super(pnfs, Subjects.ROOT, new FsPath("/"));
        }
    }
}
