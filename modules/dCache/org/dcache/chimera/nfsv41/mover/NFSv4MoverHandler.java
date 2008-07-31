package org.dcache.chimera.nfsv41.mover;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrVoid;
import org.acplt.oncrpc.server.OncRpcCallInformation;
import org.acplt.oncrpc.server.OncRpcDispatchable;
import org.acplt.oncrpc.server.OncRpcTcpServerTransport;
import org.apache.log4j.Logger;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
import org.dcache.chimera.nfs.v4.COMPOUND4args;
import org.dcache.chimera.nfs.v4.COMPOUND4res;
import org.dcache.chimera.nfs.v4.CompoundArgs;
import org.dcache.chimera.nfs.v4.HimeraNFS4Exception;
import org.dcache.chimera.nfs.v4.NFSv4Call;
import org.dcache.chimera.nfs.v4.NFSv4OperationResult;
import org.dcache.chimera.nfs.v4.OperationCOMMIT;
import org.dcache.chimera.nfs.v4.OperationCREATE_SESSION;
import org.dcache.chimera.nfs.v4.OperationDESTROY_SESSION;
import org.dcache.chimera.nfs.v4.OperationEXCHANGE_ID;
import org.dcache.chimera.nfs.v4.OperationGETATTR;
import org.dcache.chimera.nfs.v4.OperationILLEGAL;
import org.dcache.chimera.nfs.v4.OperationPUTFH;
import org.dcache.chimera.nfs.v4.OperationPUTROOTFH;
import org.dcache.chimera.nfs.v4.OperationSEQUENCE;
import org.dcache.chimera.nfs.v4.nfs4_prot;
import org.dcache.chimera.nfs.v4.nfs_argop4;
import org.dcache.chimera.nfs.v4.nfs_opnum4;
import org.dcache.chimera.nfs.v4.nfs_resop4;
import org.dcache.chimera.nfs.v4.nfsstat4;

import diskCacheV111.util.PnfsId;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler implements OncRpcDispatchable {

    private static final Logger _log = Logger.getLogger(NFSv4MoverHandler.class.getName());

    private JdbcFs _fs = null;

    private final Map<FsInode, FileChannel> _activeIO = new HashMap<FsInode, FileChannel>();
    private final EDSNFSv4OperationFactory _operationFactory = new EDSNFSv4OperationFactory(_activeIO);

    public NFSv4MoverHandler(int port)  {

        try {

            XMLconfig config = new XMLconfig( new File("/home/tigran/allInOne/config/chimera-config.xml") );

           _fs = new JdbcFs( config );

           final OncRpcTcpServerTransport tcpTrans = new OncRpcTcpServerTransport(
                   this, port, 100003, 4, 8192);

           tcpTrans.listen();


        }catch(Exception e){
            _log.error("jdbcfs:", e);
        }

    }

    public void dispatchOncRpcCall(OncRpcCallInformation call, int program,
            int version, int procedure) throws OncRpcException, IOException {

        if (version == 4) {
            switch (procedure) {
            case 0: {
                call.retrieveCall(XdrVoid.XDR_VOID);
                NFSPROC4_NULL_4(call);
                call.reply(XdrVoid.XDR_VOID);
                break;
            }
            case 1: {
                COMPOUND4args args$ = new COMPOUND4args();
                call.retrieveCall(args$);
                COMPOUND4res result$ = NFSPROC4_COMPOUND_4(call, args$);
                call.reply(result$);
                break;
            }
            default:
                call.failProcedureUnavailable();
            }
        } else {
            call.failProgramUnavailable();
        }

    }

    public void NFSPROC4_NULL_4(OncRpcCallInformation call$) {
        _log.debug("MOVER: PING from client: " + call$.peerAddress.getHostAddress() );
    }

    public COMPOUND4res NFSPROC4_COMPOUND_4(OncRpcCallInformation call$, COMPOUND4args args) {

        COMPOUND4res res = new COMPOUND4res();

        if(_log.isDebugEnabled() ) {
            _log.debug("MOVER: NFS COMPOUND client: " + call$.peerAddress.getHostAddress() +
                                            " tag: " + new String(args.tag.value.value) );
        }

        nfs_argop4[] op = args.argarray;

        for (int i = 0; i < op.length; i++) {
            int nfsOp = op[i].argop;
            _log.debug("      : " + NFSv4Call.toString(nfsOp) + " #" + i);
        }

        List<nfs_resop4> v = new ArrayList<nfs_resop4>();

        CompoundArgs fh = new CompoundArgs(args.minorversion.value);

        for (int i = 0; i < op.length; i++) {

            NFSv4OperationResult opRes = _operationFactory.getOperation(_fs, call$, fh, op[i]).process();
            try {
                _log.debug("MOVER: CURFH: " + fh.currentInode().toFullString());
            } catch (HimeraNFS4Exception he) {
                _log.debug("MOVER: CURFH: NULL");
            }

            v.add(opRes.getResult());
            // result status must be equivalent
            // to the status of the last operation that
            // was executed within the COMPOUND procedure
            res.status = opRes.getStatus();
            if (opRes.getStatus() != nfsstat4.NFS4_OK)
                break;

        }

        res.resarray = v.toArray(new nfs_resop4[v.size()]);
        try {
            _log.debug("MOVER: CURFH: " + fh.currentInode().toFullString());
        } catch (HimeraNFS4Exception he) {
            _log.debug("MOVER: CURFH: NULL");
        }

        res.tag = args.tag;
        if (res.status != nfsstat4.NFS4_OK) {
            _log.debug("MOVER: COMPOUND status: " + res.status);
        }
        return res;

    }


    public void addHandler(PnfsId id, FileChannel fileChannel) {
        _activeIO.put( new FsInode(_fs, id.toString()), fileChannel );
    }



    private static class EDSNFSv4OperationFactory {

        private final Map<FsInode, FileChannel> _activeIO;

        EDSNFSv4OperationFactory(Map<FsInode, FileChannel> activeIO) {
            _activeIO = activeIO;
        }


       AbstractNFSv4Operation getOperation(JdbcFs fs, OncRpcCallInformation call$, CompoundArgs fh, nfs_argop4 op) {

               switch ( op.argop ) {
                   case nfs_opnum4.OP_COMMIT:
                       return new OperationCOMMIT(fs, call$, fh, op, null);
                   case nfs_opnum4.OP_GETATTR:
                       return new OperationGETATTR(fs, call$, fh, op, null);
                   case nfs_opnum4.OP_PUTFH:
                       return new OperationPUTFH(fs, call$, fh, op, null);
                   case nfs_opnum4.OP_PUTROOTFH:
                       return new OperationPUTROOTFH(fs, call$, fh, op, null);
                   case nfs_opnum4.OP_READ:
                       return new EDSOperationREAD(fs, call$, fh, op, _activeIO , null);
                   case nfs_opnum4.OP_WRITE:
                       return new EDSOperationWRITE(fs, call$, fh, op, _activeIO , null);
                   case nfs_opnum4.OP_EXCHANGE_ID:
                       return new OperationEXCHANGE_ID(fs, call$, fh, op, nfs4_prot.EXCHGID4_FLAG_USE_PNFS_DS, null);
                   case nfs_opnum4.OP_CREATE_SESSION:
                       return new OperationCREATE_SESSION(fs, call$, fh, op, null);
                   case nfs_opnum4.OP_DESTROY_SESSION:
                       return new OperationDESTROY_SESSION(fs, call$, fh, op, null);
                   case nfs_opnum4.OP_SEQUENCE:
                       return new OperationSEQUENCE(fs, call$, fh, op, false, null);
                   case nfs_opnum4.OP_ILLEGAL:

                   }


               return new OperationILLEGAL(fs, call$, fh, op, null);
           }


   }
}
