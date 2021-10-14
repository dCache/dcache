package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFSv4Defaults;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.pool.repository.RepositoryChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EDSOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationREAD.class.getName());

    // Bind a direct buffer to each thread.
    private static final ThreadLocal<ByteBuffer> BUFFERS = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect((int) NFSv4Defaults.NFS4_MAXIOBUFFERSIZE);
        }
    };

    private final NfsTransferService nfsTransferService;

    public EDSOperationREAD(nfs_argop4 args, NfsTransferService nfsTransferService) {
        super(args, nfs_opnum4.OP_READ);
        this.nfsTransferService = nfsTransferService;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {
        final READ4res res = result.opread;

        try {

            long offset = _args.opread.offset.value;
            int count = _args.opread.count.value;

            NfsMover mover = nfsTransferService.getMoverByStateId(context, _args.opread.stateid);

            ByteBuffer bb = BUFFERS.get();
            bb.clear().limit(count);
            RepositoryChannel fc = mover.getMoverChannel();

            bb.rewind();
            int bytesRead = fc.read(bb, offset);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new READ4resok();
            bb.flip();
            res.resok4.data = bb;
            if (bytesRead == -1 || offset + bytesRead == fc.size()) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} read, {} requested.", bytesRead, offset,
                  _args.opread.count.value);

        } catch (ChimeraNFSException he) {
            res.status = he.getStatus();
            _log.debug(he.getMessage());
        } catch (IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        } catch (Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }
}
