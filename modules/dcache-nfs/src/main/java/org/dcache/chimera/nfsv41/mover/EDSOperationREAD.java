package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.oncrpc4j.xdr.Xdr;
import org.dcache.oncrpc4j.xdr.XdrEncodingStream;
import org.dcache.pool.repository.RepositoryChannel;
import org.glassfish.grizzly.FileChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EDSOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationREAD.class.getName());

    /**
     * Empty buffer to be returned when offset is beyond the end of file.
     */
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

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
                _log.debug("No mover associated with given stateid: {}", _args.opread.stateid);
                return;
            }

            RepositoryChannel fc = mover.getMoverChannel();
            long filesize = fc.size();

            // check if offset is beyond the end of file. Return empty buffer with eof set.
            // https://datatracker.ietf.org/doc/html/rfc5661#section-18.22.3
            if (offset >= filesize) {
                res.status = nfsstat.NFS_OK;
                res.resok4 = new READ4resok();
                res.resok4.data = EMPTY_BUFFER;
                res.resok4.eof = true;
                return;
            }

            int bytesToRead = (int) Math.min(filesize - offset, count);
            var fileChunk = new ZeroCopyFileChunk(fc, offset, bytesToRead);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new ZeroCopyREAD4resok(fileChunk);
            if (offset + bytesToRead == filesize) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} read, {} requested.", bytesToRead, offset, _args.opread.count.value);

        } catch (IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        } catch (Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }

    // version of READ4resok that uses zero copy FileChunk
    private static class ZeroCopyREAD4resok extends READ4resok {

        private FileChunk fileChunk;

        public ZeroCopyREAD4resok(FileChunk fileChunk) {
            this.fileChunk = fileChunk;
        }

        public void xdrEncode(XdrEncodingStream xdr) {
            xdr.xdrEncodeBoolean(eof);
            ((Xdr)xdr).xdrEncodeFileChunk(fileChunk);
        }
    }

    /**
     * FileChunk implementation that uses zero copy transferTo method of FileChannel.
     */
    private static class ZeroCopyFileChunk implements FileChunk {

        private final RepositoryChannel channel;
        private long position;
        private long count;

        public ZeroCopyFileChunk(RepositoryChannel channel, long position, int count ) {
            this.channel = channel;
            this.position = position;
            this.count = count;
        }

        @Override
        public long writeTo(WritableByteChannel writableByteChannel) throws IOException {
            long n =  channel.transferTo(position, count, writableByteChannel);
            count -= n;
            position += n;
            return n;
        }

        @Override
        public boolean hasRemaining() {
            return count > 0;
        }

        @Override
        public int remaining() {
            return (int)count;
        }

        @Override
        public boolean release() {
            return true;
        }

        @Override
        public boolean isExternal() {
            return true;
        }
    }
}
