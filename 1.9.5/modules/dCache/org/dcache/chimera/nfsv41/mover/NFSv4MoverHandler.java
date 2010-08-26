package org.dcache.chimera.nfsv41.mover;

import com.sun.grizzly.BaseSelectionKeyHandler;
import com.sun.grizzly.Controller;
import com.sun.grizzly.DefaultProtocolChain;
import com.sun.grizzly.DefaultProtocolChainInstanceHandler;
import com.sun.grizzly.ProtocolChain;
import com.sun.grizzly.ProtocolChainInstanceHandler;
import com.sun.grizzly.ProtocolFilter;
import com.sun.grizzly.TCPSelectorHandler;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.acplt.oncrpc.OncRpcException;
import org.apache.log4j.Logger;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
import org.dcache.chimera.nfs.v4.xdr.COMPOUND4args;
import org.dcache.chimera.nfs.v4.xdr.COMPOUND4res;
import org.dcache.chimera.nfs.v4.CompoundArgs;
import org.dcache.chimera.nfs.ChimeraNFSException;
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
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot_NFS4_PROGRAM_ServerStub;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.util.PortRange;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.RpcDispatcher;
import org.dcache.xdr.RpcParserProtocolFilter;
import org.dcache.xdr.RpcProtocolFilter;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler extends nfs4_prot_NFS4_PROGRAM_ServerStub {

    private static final Logger _log = Logger.getLogger(NFSv4MoverHandler.class.getName());

    private final FileSystemProvider _fs = new DummyFileSystemProvider();

    /*
     * TCP port number used by Handler
     */
    private final int _localPort;

    private final Map<stateid4, MoverBridge> _activeIO = new HashMap<stateid4, MoverBridge>();
    private final EDSNFSv4OperationFactory _operationFactory = new EDSNFSv4OperationFactory(_activeIO);

    public NFSv4MoverHandler(PortRange portRange) throws OncRpcException, IOException {

        final Map<Integer, RpcDispatchable> programs = new HashMap<Integer, RpcDispatchable>();
        programs.put(100003, this);

        final ProtocolFilter rpcFilter = new RpcParserProtocolFilter();
        final ProtocolFilter rpcProcessor = new RpcProtocolFilter();
        final ProtocolFilter rpcDispatcher = new RpcDispatcher(programs);

        final Controller controller = new Controller();
        final TCPSelectorHandler tcp_handler = new TCPSelectorHandler();
        tcp_handler.setPortRange(new com.sun.grizzly.PortRange(
                (int)portRange.getLower(), (int)portRange.getUpper()));
        tcp_handler.setSelectionKeyHandler(new BaseSelectionKeyHandler());

        controller.addSelectorHandler(tcp_handler);
        controller.setReadThreadsCount(5);

        final ProtocolChain protocolChain = new DefaultProtocolChain();
        protocolChain.addFilter(rpcFilter);
        protocolChain.addFilter(rpcProcessor);
        protocolChain.addFilter(rpcDispatcher);

        ((DefaultProtocolChain) protocolChain).setContinuousExecution(true);

        ProtocolChainInstanceHandler pciHandler = new DefaultProtocolChainInstanceHandler() {

            @Override
            public ProtocolChain poll() {
                return protocolChain;
            }

            @Override
            public boolean offer(ProtocolChain pc) {
                return false;
            }
        };

        controller.setProtocolChainInstanceHandler(pciHandler);

        new Thread(controller, "NFSv4.1 DS thread").start();
        /*
         * Wait untill grizzly is ready.
         */
        while( tcp_handler.getPort() < 0 ) {
            try {
                Thread.sleep(500);
            }catch(InterruptedException e) {
                _log.error("Grizzly initialization interrupted:" + e);
                throw new InterruptedIOException("Failed to initialize Grizzly NIO engine" + e);
            }
        }
        _localPort = tcp_handler.getPort();
        _log.debug("NFSv4MoverHandler created on port:" + tcp_handler.getPort());
    }

    /*
     * AKA RPC ping
     */
    @Override
    public void NFSPROC4_NULL_4(RpcCall call$) {
        _log.debug("MOVER: PING from client: " + call$.getTransport().getRemoteSocketAddress() );
    }

    @Override
    public COMPOUND4res NFSPROC4_COMPOUND_4(RpcCall call$, COMPOUND4args args) {

        COMPOUND4res res = new COMPOUND4res();

        if(_log.isDebugEnabled() ) {
            _log.debug("MOVER: NFS COMPOUND client: " + call$.getTransport().getRemoteSocketAddress() +
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
            } catch (ChimeraNFSException he) {
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
        } catch (ChimeraNFSException he) {
            _log.debug("MOVER: CURFH: NULL");
        }

        res.tag = args.tag;
        if (res.status != nfsstat4.NFS4_OK) {
            _log.debug("MOVER: COMPOUND status: " + res.status);
        }
        return res;

    }

    /**
     * Add specified mover into list of allowed transfers.
     * 
     * @param moverBridge
     */
    public void addHandler(MoverBridge moverBridge) {
        _log.debug("added io handler: " + moverBridge);
        _activeIO.put( moverBridge.getStateid() , moverBridge );
    }

    /**
     * Removes specified mover into list of allowed transfers.
     * 
     * @param moverBridge
     */
    public void removeHandler(MoverBridge moverBridge) {
        _log.debug("removing io handler: " + moverBridge);
        _activeIO.remove(moverBridge.getStateid());
    }

    private static class EDSNFSv4OperationFactory {

        private final Map<stateid4, MoverBridge> _activeIO;

        EDSNFSv4OperationFactory(Map<stateid4, MoverBridge> activeIO) {
            _activeIO = activeIO;
        }

       AbstractNFSv4Operation getOperation(FileSystemProvider fs, RpcCall call$, CompoundArgs fh, nfs_argop4 op) {

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

    /**
     * Get TCP port number used by handler.
     * @return port number.
     */
    public int getLocalPort(){
        return _localPort;
    }
}
