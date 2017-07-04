package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.COMMIT4res;
import org.dcache.nfs.v4.xdr.COMMIT4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.pool.repository.RepositoryChannel;

public class EDSOperationCOMMIT extends AbstractNFSv4Operation {

    private final NFSv4MoverHandler _moverHandler;

    public EDSOperationCOMMIT(nfs_argop4 args, NFSv4MoverHandler moverHandler) {
        super(args, nfs_opnum4.OP_COMMIT);
        _moverHandler = moverHandler;
    }

    @Override
    public void process(CompoundContext cc, nfs_resop4 result) throws ChimeraNFSException, IOException {

        final COMMIT4res res = result.opcommit;
        Inode inode = cc.currentInode();
        try {
            /**
             * The nfs commit operation comes without a stateid. The assumption,
             * is that for now we have only one writer and, as a result, pnfsid
             * will point only to a single mover.
             */
            NfsMover mover = _moverHandler.getPnfsIdByHandle(inode.toNfsHandle());

            RepositoryChannel fc = mover.getMoverChannel();
            fc.sync();
            mover.commitFileSize(fc.size());

            res.status = nfsstat.NFS_OK;
            res.resok4 = new COMMIT4resok();
            res.resok4.writeverf = cc.getRebootVerifier();
        } catch (AccessDeniedException e) {
            res.status = nfsstat.NFSERR_PERM;
        }
    }
}
