package org.dcache.chimera.nfsv41.mover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.pool.repository.RepositoryChannel;

public class EDSOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationREAD.class.getName());

     private final Map<stateid4, NfsMover> _activeIO;

    public EDSOperationREAD(nfs_argop4 args,  Map<stateid4, NfsMover> activeIO) {
        super(args, nfs_opnum4.OP_READ);
        _activeIO = activeIO;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {
        final READ4res res = result.opread;

        try {

            long offset = _args.opread.offset.value;
            int count = _args.opread.count.value;

            NfsMover mover = _activeIO.get(_args.opread.stateid);
            if(mover == null) {
                throw new ChimeraNFSException(nfsstat.NFSERR_BAD_STATEID,
                        "No mover associated with given stateid");
            }
            mover.attachSession(context.getSession());

            ByteBuffer bb = ByteBuffer.allocate(count);
            RepositoryChannel fc = mover.getMoverChannel();

            bb.rewind();
            int bytesReaded = fc.read(bb, offset);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new READ4resok();
            res.resok4.data = bb;

            if( offset + bytesReaded == fc.size() ) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} readed, {} requested.", bytesReaded, offset, _args.opread.count.value);

        }catch(ChimeraNFSException he) {
            res.status = he.getStatus();
            _log.debug(he.getMessage());
        }catch(IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }
}
