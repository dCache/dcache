package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
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
import org.dcache.chimera.nfs.v4.OperationRECLAIM_COMPLETE;
import org.dcache.chimera.nfs.v4.OperationSEQUENCE;
import org.dcache.chimera.nfs.v4.SimpleIdMap;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.util.PortRange;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.RpcLoginService;
import org.dcache.xdr.gss.GssSessionManager;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler {

    private static final Logger _log = LoggerFactory.getLogger(NFSv4MoverHandler.class.getName());

    private final FileSystemProvider _fs = new DummyFileSystemProvider();

    /**
     * RPC service
     */
    private final OncRpcSvc _rpcService;

    private final Map<stateid4, MoverBridge> _activeIO = new HashMap<stateid4, MoverBridge>();
    private final NFSv4OperationFactory _operationFactory =
            new EDSNFSv4OperationFactory(_activeIO);
    private final NFSServerV41 _embededDS;

    public NFSv4MoverHandler(PortRange portRange, boolean withGss)
            throws IOException , OncRpcException, GSSException {

        _embededDS = new NFSServerV41(_operationFactory, null, null, _fs, new SimpleIdMap(), null);
        _rpcService = new OncRpcSvc(
                new com.sun.grizzly.PortRange((int)portRange.getLower(), (int)portRange.getUpper()),
                IpProtocolType.TCP, false, "Embedded NFSv4.1 DS");

        final Map<OncRpcProgram, RpcDispatchable> programs = new HashMap<OncRpcProgram, RpcDispatchable>();
        programs.put(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _embededDS);
        _rpcService.setPrograms(programs);

        if(withGss) {
            RpcLoginService rpcLoginService = new RpcLoginService() {

                @Override
                public Subject login(Principal principal) {
                    return Subjects.NOBODY;
                }
            };
            GssSessionManager gss = new GssSessionManager(rpcLoginService);
            _rpcService.setGssSessionManager(gss);
        }
        _rpcService.start();
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
                case nfs_opnum4.OP_RECLAIM_COMPLETE:
                    return new OperationRECLAIM_COMPLETE(op);
                case nfs_opnum4.OP_ILLEGAL:
            }

            return new OperationILLEGAL(op);
        }
    }

    /**
     * Get TCP port number used by handler.
     * @return port number.
     */
    public InetSocketAddress getLocalAddress(){
        return _rpcService.getInetSocketAddress(IpProtocolType.TCP);
    }

    public void shutdown() {
        _rpcService.stop();
    }
}
