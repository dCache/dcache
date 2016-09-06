package org.dcache.chimera.nfsv41.mover;

import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v3.HimeraNfsUtils;
import org.dcache.nfs.v3.xdr.READ3args;
import org.dcache.nfs.v3.xdr.READ3res;
import org.dcache.nfs.v3.xdr.READ3resfail;
import org.dcache.nfs.v3.xdr.READ3resok;
import org.dcache.nfs.v3.xdr.WRITE3args;
import org.dcache.nfs.v3.xdr.WRITE3res;
import org.dcache.nfs.v3.xdr.WRITE3resfail;
import org.dcache.nfs.v3.xdr.WRITE3resok;
import org.dcache.nfs.v3.xdr.count3;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v3.xdr.stable_how;
import org.dcache.nfs.v3.xdr.uint32;
import org.dcache.nfs.v3.xdr.writeverf3;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.XdrVoid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedV3 implements RpcDispatchable {

    private static final Logger _log = LoggerFactory.getLogger(EmbeddedV3.class);
    private final writeverf3 writeVerifier =
            new writeverf3(Longs.toByteArray(System.currentTimeMillis()));

    private final NFSv4MoverHandler _moverHandler;

    public EmbeddedV3(NFSv4MoverHandler moverHandler) {
        this._moverHandler = moverHandler;
    }

    @Override
    public void dispatchOncRpcCall(RpcCall call) throws OncRpcException, IOException {
        int procedure = call.getProcedure();

        switch (procedure) {
            case nfs3_prot.NFSPROC3_NULL_3:
                call.retrieveCall(XdrVoid.XDR_VOID);
                call.reply(XdrVoid.XDR_VOID);
                break;

            case nfs3_prot.NFSPROC3_READ_3: {
                READ3args args$ = new READ3args();
                call.retrieveCall(args$);
                READ3res result$ = NFSPROC3_READ_3(call, args$);
                call.reply(result$);
                break;
            }
            case nfs3_prot.NFSPROC3_WRITE_3: {
                WRITE3args args$ = new WRITE3args();
                call.retrieveCall(args$);
                WRITE3res result$ = NFSPROC3_WRITE_3(call, args$);
                call.reply(result$);
                break;
            }
            default:
                call.failProcedureUnavailable();

        }
    }

    private READ3res NFSPROC3_READ_3(RpcCall call, READ3args args$) {

        READ3res res = new READ3res();
        try {

            long offset = args$.offset.value.value;
            int count = args$.count.value.value;
            stateid4 stateid = new stateid4(args$.file.data, 0);

            NfsMover mover = _moverHandler.getOrCreateMover(call.getTransport().getRemoteSocketAddress(), stateid, args$.file.data);
            if (mover == null) {
                /*
                 * return IO error instead of BadStateidException to avoid state recovery.
                 * The client will fall back to IO through MDS.
                 */
                throw new NfsIoException("No mover associated with given stateid: " + stateid);
            }

            ByteBuffer bb = ByteBuffer.allocate(count);
            RepositoryChannel fc = mover.getMoverChannel();

            bb.rewind();
            int bytesRead = fc.read(bb, offset);

            res.status = nfsstat.NFS_OK;
            res.resok = new READ3resok();
            res.resok.count = new count3(new uint32(bytesRead));
            res.resok.data = bb.array();
            res.resok.file_attributes = HimeraNfsUtils.defaultPostOpAttr();

            if (bytesRead == -1 || offset + bytesRead == fc.size()) {
                res.resok.eof = true;
            }

            _log.debug("MOVER: {}@{} read, {} requested.", bytesRead, offset, count);

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = new READ3resfail();
            res.resfail.file_attributes = HimeraNfsUtils.defaultPostOpAttr();
            _log.debug(e.getMessage());
        } catch (Exception e) {
            _log.error("DSREAD3: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = new READ3resfail();
            res.resfail.file_attributes = HimeraNfsUtils.defaultPostOpAttr();
            _log.error("internal server error", e);
        }
        return res;
    }

    private WRITE3res NFSPROC3_WRITE_3(RpcCall call, WRITE3args args$) {
        WRITE3res res = new WRITE3res();
        try {

            long offset = args$.offset.value.value;
            int count = args$.count.value.value;
            stateid4 stateid = new stateid4(args$.file.data, 0);

            NfsMover mover = _moverHandler.getOrCreateMover(call.getTransport().getRemoteSocketAddress(), stateid, args$.file.data);
            if (mover == null) {
                /*
                 * return IO error instead of BadStateidException to avoid state recovery.
                 * The client will fall back to IO through MDS.
                 */
                throw new NfsIoException("No mover associated with given stateid: " + stateid);
            }

            ByteBuffer bb = ByteBuffer.wrap(args$.data);
            RepositoryChannel fc = mover.getMoverChannel();

            bb.limit(count);
            int bytesWritten = fc.write(bb, offset);

            res.status = nfsstat.NFS_OK;
            res.resok = new WRITE3resok();
            res.resok.count = new count3(new uint32(bytesWritten));
            res.resok.file_wcc = HimeraNfsUtils.defaultWccData();
            res.resok.committed = stable_how.FILE_SYNC;
            res.resok.verf = writeVerifier;

            _log.debug("MOVER: {}@{} written, {} requested.", bytesWritten, offset, count);

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = new WRITE3resfail();
            res.resfail.file_wcc = HimeraNfsUtils.defaultWccData();
            _log.debug(e.getMessage());
        } catch (Exception e) {
            _log.error("DSWRIRE3: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = new WRITE3resfail();
            res.resfail.file_wcc = HimeraNfsUtils.defaultWccData();
            _log.error("internal server error", e);
        }
        return res;
    }

}
