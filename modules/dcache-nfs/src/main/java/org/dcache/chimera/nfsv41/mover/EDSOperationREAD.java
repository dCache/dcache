package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.oncrpc4j.grizzly.GrizzlyUtils;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.xdr.Xdr;
import org.dcache.oncrpc4j.xdr.XdrEncodingStream;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.ByteUnit;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.memory.PooledMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EDSOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationREAD.class.getName());

    // one pool with 1MB chunks (max NFS rsize)
    private final static MemoryManager<? extends Buffer> POOLED_BUFFER_ALLOCATOR =
          new PooledMemoryManager(
                ByteUnit.MiB.toBytes(1), // base chunk size
                1, // number of pools
                2, // grow facter per pool, ignored, see above
                GrizzlyUtils.getDefaultWorkerPoolSize(), // expected concurrency
                PooledMemoryManager.DEFAULT_HEAP_USAGE_PERCENTAGE,
                PooledMemoryManager.DEFAULT_PREALLOCATED_BUFFERS_PERCENTAGE,
                true  // direct buffers
          );

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
            if (mover == null) {
                res.status = nfsstat.NFSERR_BAD_STATEID;
                _log.debug("No mover associated with given stateid: ", _args.opread.stateid);
                return;
            }

            var gBuffer = POOLED_BUFFER_ALLOCATOR.allocate(count);
            gBuffer.allowBufferDispose(true);
            ByteBuffer bb = gBuffer.toByteBuffer();
            bb.clear().limit(count);

            RepositoryChannel fc = mover.getMoverChannel();
            int bytesRead = fc.read(bb, offset);

            if (bytesRead > 0) {
                // the positions of Buffer and ByteBuffer are independent, thus keep it in sync manually
                gBuffer.position(bytesRead);
            }
            gBuffer.flip();

            res.status = nfsstat.NFS_OK;
            res.resok4 = new ShallowREAD4resok(gBuffer);
            if (bytesRead == -1 || offset + bytesRead == fc.size()) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} read, {} requested.", bytesRead, offset,
                  _args.opread.count.value);

        } catch (IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        } catch (Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }

    // version of READ4resok that uses shallow encoding to avoid extra copy
    private static class ShallowREAD4resok extends READ4resok {

        private final Buffer buf;
        public ShallowREAD4resok(Buffer buf) {
            this.buf = buf;
        }

        public void xdrEncode(XdrEncodingStream xdr)
              throws OncRpcException, IOException {
            xdr.xdrEncodeBoolean(eof);
            ((Xdr)xdr).xdrEncodeShallowByteBuffer(buf);
        }
    }
}
