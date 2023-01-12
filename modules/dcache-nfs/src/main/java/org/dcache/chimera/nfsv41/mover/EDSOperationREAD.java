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

            RepositoryChannel fc = mover.getMoverChannel();
            var gBuffer = POOLED_BUFFER_ALLOCATOR.allocate(count);
            int bytesToRead = count;

            int rc = -1;
            if (gBuffer.isComposite()) {
                // composite buffers built out of array of simple buffers, thus we have to fill
                // each buffer manually
                var bArray = gBuffer.toBufferArray();
                Buffer[] bufs = bArray.getArray();
                int size = bArray.size();

                for(int i = 0; i < size; i++) {

                    ByteBuffer directChunk = bufs[i].toByteBuffer();
                    directChunk.clear().limit(Math.min(directChunk.capacity(), bytesToRead));
                    rc = fc.read(directChunk, offset + count - bytesToRead);
                    if (rc < 0) {
                        break;
                    }

                    // the positions of Buffer and ByteBuffer are independent, thus keep it in sync manually
                    gBuffer.position(gBuffer.position() + directChunk.position());
                    directChunk.flip();

                    bytesToRead -= rc;
                }
            } else {

                ByteBuffer directBuffer = gBuffer.toByteBuffer();
                directBuffer.clear().limit(count);
                rc = fc.read(directBuffer, offset);
                if (rc > 0) {
                    // the positions of Buffer and ByteBuffer are independent, thus keep it in sync manually
                    gBuffer.position(directBuffer.position());
                    bytesToRead -= rc;
                }
            }

            int bytesRead = count - bytesToRead;
            gBuffer.flip();

            res.status = nfsstat.NFS_OK;
            res.resok4 = new ShallowREAD4resok(gBuffer);
            if (rc == -1 || offset + bytesRead == fc.size()) {
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
