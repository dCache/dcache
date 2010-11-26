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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
import org.dcache.chimera.nfs.v4.DeviceManager;
import org.dcache.chimera.nfs.v4.NFSServerV41;
import org.dcache.chimera.nfs.v4.NFSv4OperationFactory;
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
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.util.PortRange;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.RpcDispatcher;
import org.dcache.xdr.RpcParserProtocolFilter;
import org.dcache.xdr.RpcProtocolFilter;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler {

    private static final Logger _log = LoggerFactory.getLogger(NFSv4MoverHandler.class.getName());

    private final FileSystemProvider _fs = new DummyFileSystemProvider();

    /*
     * TCP port number used by Handler
     */
    private final int _localPort;

    private final Map<stateid4, MoverBridge> _activeIO = new HashMap<stateid4, MoverBridge>();
    private final NFSv4OperationFactory _operationFactory =
            new EDSNFSv4OperationFactory(_activeIO);
    private final NFSServerV41 _embededDS;

    public NFSv4MoverHandler(PortRange portRange)
            throws IOException,
                OncRpcException,
                ChimeraFsException {

        _embededDS = new NFSServerV41(_operationFactory, new DeviceManager(), null, _fs, null);

        final Map<OncRpcProgram, RpcDispatchable> programs = new HashMap<OncRpcProgram, RpcDispatchable>();
        programs.put(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _embededDS);

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
        _log.debug("NFSv4MoverHandler created on port: {}", tcp_handler.getPort());
    }

    /**
     * Add specified mover into list of allowed transfers.
     * 
     * @param moverBridge
     */
    public void addHandler(MoverBridge moverBridge) {
        _log.debug("added io handler: {}", moverBridge);
        _activeIO.put( moverBridge.getStateid() , moverBridge );
    }

    /**
     * Removes specified mover into list of allowed transfers.
     * 
     * @param moverBridge
     */
    public void removeHandler(MoverBridge moverBridge) {
        _log.debug("removing io handler: {}", moverBridge);
        _activeIO.remove(moverBridge.getStateid());
    }

    private static class EDSNFSv4OperationFactory implements NFSv4OperationFactory {

        private final Map<stateid4, MoverBridge> _activeIO;

        EDSNFSv4OperationFactory(Map<stateid4, MoverBridge> activeIO) {
            _activeIO = activeIO;
        }

        @Override
        public AbstractNFSv4Operation getOperation(nfs_argop4 op) {

            switch (op.argop) {
                case nfs_opnum4.OP_COMMIT:
                    return new OperationCOMMIT(op);
                case nfs_opnum4.OP_GETATTR:
                    return new OperationGETATTR(op);
                case nfs_opnum4.OP_PUTFH:
                    return new OperationPUTFH(op);
                case nfs_opnum4.OP_PUTROOTFH:
                    return new OperationPUTROOTFH(op);
                case nfs_opnum4.OP_READ:
                    return new EDSOperationREAD(op, _activeIO);
                case nfs_opnum4.OP_WRITE:
                    return new EDSOperationWRITE(op, _activeIO);
                case nfs_opnum4.OP_EXCHANGE_ID:
                    return new OperationEXCHANGE_ID(op, nfs4_prot.EXCHGID4_FLAG_USE_PNFS_DS);
                case nfs_opnum4.OP_CREATE_SESSION:
                    return new OperationCREATE_SESSION(op);
                case nfs_opnum4.OP_DESTROY_SESSION:
                    return new OperationDESTROY_SESSION(op);
                case nfs_opnum4.OP_SEQUENCE:
                    return new OperationSEQUENCE(op);
                case nfs_opnum4.OP_ILLEGAL:
            }

            return new OperationILLEGAL(op);
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
