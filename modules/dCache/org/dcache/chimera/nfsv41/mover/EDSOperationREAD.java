package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.CompoundContext;
import org.dcache.chimera.nfs.v4.xdr.READ4res;
import org.dcache.chimera.nfs.v4.xdr.READ4resok;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;

public class EDSOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationREAD.class.getName());

     private final Map<stateid4, MoverBridge> _activeIO;

    public EDSOperationREAD(nfs_argop4 args,  Map<stateid4, MoverBridge> activeIO) {
        super(args, nfs_opnum4.OP_READ);
        _activeIO = activeIO;
    }

    @Override
    public boolean process(CompoundContext context) {
        READ4res res = new READ4res();

        try {

            long offset = _args.opread.offset.value.value;
            int count = _args.opread.count.value.value;

            MoverBridge moverBridge = _activeIO.get(_args.opread.stateid);
            if(moverBridge == null) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_BAD_STATEID,
                        "No mover associated with given stateid");
            }

            ByteBuffer bb = ByteBuffer.allocate(count);
            FileChannel fc = moverBridge.getFileChannel();

            bb.rewind();
            int bytesReaded = fc.read(bb, offset);

            moverBridge.getMover().setBytesTransferred(bytesReaded);

            res.status = nfsstat4.NFS4_OK;
            res.resok4 = new READ4resok();
            res.resok4.data = bb;

            if( offset + bytesReaded == fc.size() ) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} readed, {} requested.",
                    new Object[]{
                        bytesReaded,
                        offset,
                        _args.opread.count.value.value
                    });

        }catch(ChimeraNFSException he) {
            res.status = he.getStatus();
            _log.debug(he.getMessage());
        }catch(IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat4.NFS4ERR_IO;
        }catch(Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat4.NFS4ERR_SERVERFAULT;
        }

       _result.opread = res;

        context.processedOperations().add(_result);
        return res.status == nfsstat4.NFS4_OK;
    }
}
