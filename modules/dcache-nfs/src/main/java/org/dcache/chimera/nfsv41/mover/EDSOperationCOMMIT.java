package org.dcache.chimera.nfsv41.mover;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.dcache.chimera.FsInodeType;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.COMMIT4res;
import org.dcache.nfs.v4.xdr.COMMIT4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xdr.OncRpcException;


public class EDSOperationCOMMIT extends AbstractNFSv4Operation {

    private final Map<PnfsId, NfsMover> _activeWrites;

    public EDSOperationCOMMIT(nfs_argop4 args, Map<PnfsId, NfsMover> activeMovers) {
        super(args, nfs_opnum4.OP_COMMIT);
        _activeWrites = activeMovers;
    }

    @Override
    public void process(CompoundContext cc, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

        try {
            Inode inode = cc.currentInode();
            PnfsId pnfsId = toPnfsid(inode);
            NfsMover mover = _activeWrites.get(pnfsId);
            if (mover == null) {
                throw new BadHandleException("can't find mover for pnfsid : " + pnfsId);
            }

            RepositoryChannel fc = mover.getMoverChannel();
            fc.sync();
            mover.commitFileSize(fc.size());
            final COMMIT4res res = result.opcommit;
            res.status = nfsstat.NFS_OK;
            res.resok4 = new COMMIT4resok();
            res.resok4.writeverf = mover.getBootVerifier();
        } catch (CacheException e) {
            throw new NfsIoException(e.getMessage());
        }
    }

    /*
     * this is shameless light copy-paste form JdbcFs.
     */
    private static PnfsId toPnfsid(Inode inode) throws ChimeraNFSException {
        ByteBuffer b = ByteBuffer.wrap(inode.getFileId());
        int fsid = b.get();
        int type = b.get();
        FsInodeType inodeType = FsInodeType.valueOf(type);
        if (inodeType != FsInodeType.INODE) {
            throw new BadHandleException("Not a regular pnfsid");
        }

        int idLen = b.get();
        byte[] id = new byte[idLen];
        b.get(id);
        int opaqueLen = b.get();
        if (opaqueLen > b.remaining()) {
            throw new BadHandleException("Bad pnfsid size");
        }
        return new PnfsId(id);
    }
}
