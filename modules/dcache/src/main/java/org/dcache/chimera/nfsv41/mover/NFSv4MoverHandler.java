package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.vfs.DirectoryEntry;
import org.dcache.chimera.nfs.vfs.FsStat;
import org.dcache.chimera.nfs.vfs.Inode;
import org.dcache.chimera.nfs.vfs.Stat;
import org.dcache.chimera.nfs.vfs.Stat.Type;
import org.dcache.chimera.nfs.vfs.VirtualFileSystem;
import org.dcache.util.PortRange;
import org.dcache.xdr.*;
import org.dcache.xdr.gss.GssSessionManager;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler {

    private static final Logger _log = LoggerFactory.getLogger(NFSv4MoverHandler.class.getName());

    private final VirtualFileSystem _fs = new VirtualFileSystem() {

        @Override
        public int access(Inode inode, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode create(Inode parent, Type type, String path, int uid, int gid, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public FsStat getFsStat() throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode getRootInode() throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode lookup(Inode parent, String path) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode link(Inode parent, Inode link, String path, int uid, int gid) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<DirectoryEntry> list(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode mkdir(Inode parent, String path, int uid, int gid, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void move(Inode src, String oldName, Inode dest, String newName) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode parentOf(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String readlink(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean remove(Inode parent, String path) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode symlink(Inode parent, String path, String link, int uid, int gid, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int write(Inode inode, byte[] data, long offset, int count) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Stat getattr(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setattr(Inode inode, Stat stat) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public nfsace4[] getAcl(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };

    /**
     * RPC service
     */
    private final OncRpcSvc _rpcService;

    private final Map<stateid4, MoverBridge> _activeIO = new HashMap<stateid4, MoverBridge>();
    private final NFSv4OperationFactory _operationFactory =
            new EDSNFSv4OperationFactory(_activeIO);
    private final NFSServerV41 _embededDS;

    public NFSv4MoverHandler(PortRange portRange, boolean withGss, String serverId)
            throws IOException , GSSException, OncRpcException {

        _embededDS = new NFSServerV41(_operationFactory, null, _fs, new SimpleIdMap(), null);
        _rpcService = new OncRpcSvcBuilder()
                .withMinPort(portRange.getLower())
                .withMaxPort(portRange.getUpper())
                .withTCP()
                .withoutAutoPublish()
                .withSameThreadIoStrategy()
                .build();

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

    public void shutdown() throws IOException {
        _rpcService.stop();
    }
}
