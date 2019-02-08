package org.dcache.chimera.nfsv41.mover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.StandardOpenOption;

import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.BadStateidException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.WRITE4res;
import org.dcache.nfs.v4.xdr.WRITE4resok;
import org.dcache.nfs.v4.xdr.count4;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.stable_how4;
import org.dcache.pool.repository.OutOfDiskException;
import org.dcache.pool.repository.RepositoryChannel;


public class EDSOperationWRITE extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationWRITE.class.getName());

    private final NFSv4MoverHandler _moverHandler;

    public EDSOperationWRITE(nfs_argop4 args, NFSv4MoverHandler moverHandler) {
        super(args, nfs_opnum4.OP_WRITE);
        _moverHandler = moverHandler;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {

        final WRITE4res res = result.opwrite;

        try {

            NfsMover mover = _moverHandler.getOrCreateMover(context.getRemoteSocketAddress(),
                    _args.opwrite.stateid,
                    context.currentInode().toNfsHandle());
            if (mover == null) {
                throw new BadStateidException("No mover associated with given stateid: " + _args.opwrite.stateid);
            }

            mover.attachSession(context.getSession());
            if(!mover.getIoMode().contains(StandardOpenOption.WRITE)) {
                throw new PermException("an attempt to write without IO mode enabled");
            }

            long offset = _args.opwrite.offset.value;

            RepositoryChannel fc = mover.getMoverChannel();

            _args.opwrite.data.rewind();
            int bytesWritten = fc.write(_args.opwrite.data, offset);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new WRITE4resok();
            res.resok4.count = new count4(bytesWritten);
            res.resok4.writeverf = context.getRebootVerifier();

            /*
             * The pool holds only the data. If client wants to sync metadata
             * as well (FILE_SYNC-like behavior), the it must send an explicit
             * LAYOUT_COMMIT to the door.
             */
            res.resok4.committed = stable_how4.DATA_SYNC4;

            _log.debug("MOVER: {}@{} written, {} requested.", bytesWritten, offset, bytesWritten);

        }catch(ChimeraNFSException he) {
            _log.debug(he.getMessage());
            res.status = he.getStatus();
        }catch (OutOfDiskException e) {
            _log.error("DSWRITE: no allocatable space left on the pool");
            res.status = nfsstat.NFSERR_NOSPC;
        }catch(IOException ioe) {
            _log.error("DSWRITE: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSWRITE: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }
}
