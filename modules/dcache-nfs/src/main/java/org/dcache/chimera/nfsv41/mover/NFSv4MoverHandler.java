package org.dcache.chimera.nfsv41.mover;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.chimera.nfsv41.common.StatsDecoratedOperationExecutor;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.AbstractOperationExecutor;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.OperationBIND_CONN_TO_SESSION;
import org.dcache.nfs.v4.OperationCREATE_SESSION;
import org.dcache.nfs.v4.OperationDESTROY_CLIENTID;
import org.dcache.nfs.v4.OperationDESTROY_SESSION;
import org.dcache.nfs.v4.OperationEXCHANGE_ID;
import org.dcache.nfs.v4.OperationGETATTR;
import org.dcache.nfs.v4.OperationILLEGAL;
import org.dcache.nfs.v4.OperationPUTFH;
import org.dcache.nfs.v4.OperationPUTROOTFH;
import org.dcache.nfs.v4.OperationRECLAIM_COMPLETE;
import org.dcache.nfs.v4.OperationSEQUENCE;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.util.PortRange;
import org.dcache.util.Bytes;
import org.dcache.vehicles.DoorValidateMoverMessage;
import org.dcache.oncrpc4j.rpc.IoStrategy;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.dcache.oncrpc4j.rpc.RpcLoginService;
import org.dcache.oncrpc4j.rpc.gss.GssSessionManager;
import org.dcache.oncrpc4j.rpc.OncRpcException;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler {

    private static final Logger _log = LoggerFactory.getLogger(NFSv4MoverHandler.class.getName());

    /**
     * The number of missed leases before pool will query door for mover validation.
     */
    private static final int LEASE_MISSES = 3;

    /**
     * RPC service
     */
    private final OncRpcSvc _rpcService;

    private final Map<stateid4, NfsMover> _activeIO = new ConcurrentHashMap<>();

    /**
     * NFSv4 operation executer with requests statistics.
     */
    private final StatsDecoratedOperationExecutor _operationFactory =
            new StatsDecoratedOperationExecutor(new EDSNFSv4OperationFactory());

    private final NFSServerV41 _embededDS;

    /**
     * A CellStub for communication with doors.
     */
    private final CellStub _door;

    private final ScheduledExecutorService _cleanerExecutor;
    private final long _bootVerifier;

    /**
     * Mover inactivity time before pool will query the door for mover validation.
     */
    private final Duration deadMoverIdleTime;

    public NFSv4MoverHandler(PortRange portRange, IoStrategy ioStrategy,
            boolean withGss, String serverId, CellStub door, long bootVerifier)
            throws IOException , GSSException, OncRpcException {

        _embededDS = new NFSServerV41.Builder()
                .withOperationExecutor(_operationFactory)
                .build();

        OncRpcSvcBuilder oncRpcSvcBuilder = new OncRpcSvcBuilder()
                .withMinPort(portRange.getLower())
                .withMaxPort(portRange.getUpper())
                .withTCP()
                .withoutAutoPublish()
                .withRpcService(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _embededDS);

        _log.debug("Using {} IO strategy", ioStrategy);
        if (ioStrategy == IoStrategy.SAME_THREAD) {
            oncRpcSvcBuilder.withSameThreadIoStrategy();
        } else {
            oncRpcSvcBuilder.withWorkerThreadIoStrategy();
        }

        if (withGss) {
            RpcLoginService rpcLoginService = (t, gss) -> Subjects.NOBODY;
            GssSessionManager gss = new GssSessionManager(rpcLoginService);
            oncRpcSvcBuilder.withGssSessionManager(gss);
        }

        _rpcService = oncRpcSvcBuilder.build();
        _rpcService.start();
        _door = door;
        _bootVerifier = bootVerifier;
        _cleanerExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("NFS mover validationthread")
                .build()
        );

        // Make mover validation schedule to match nfs state handler lease timeout.
        deadMoverIdleTime = Duration.ofSeconds(_embededDS.getStateHandler().getLeaseTime()).multipliedBy(LEASE_MISSES);
        _cleanerExecutor.scheduleAtFixedRate(new MoverValidator(), deadMoverIdleTime.toSeconds(), deadMoverIdleTime.toSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Add mover into list of allowed transfers.
     *
     * @param mover
     */
    public void add(NfsMover mover) {
        _log.debug("registering new mover {}", mover);
        _activeIO.put(mover.getStateId(), mover );
    }

    /**
     * Removes mover from the list of allowed transfers.
     *
     * @param mover
     */
    public void remove(NfsMover mover) {
        _log.debug("un-removing io handler for stateid {}", mover);
        _activeIO.remove(mover.getStateId());
    }


    RequestExecutionTimeGauges<String> getStatistics() {
        return _operationFactory.getStatistics();
    }

    private class EDSNFSv4OperationFactory extends AbstractOperationExecutor {

        @Override
        protected AbstractNFSv4Operation getOperation(nfs_argop4 op) {

            switch (op.argop) {
                case nfs_opnum4.OP_COMMIT:
                    return new EDSOperationCOMMIT(op, NFSv4MoverHandler.this);
                case nfs_opnum4.OP_GETATTR:
                    return new OperationGETATTR(op);
                case nfs_opnum4.OP_PUTFH:
                    return new OperationPUTFH(op);
                case nfs_opnum4.OP_PUTROOTFH:
                    return new OperationPUTROOTFH(op);
                case nfs_opnum4.OP_READ:
                    return new EDSOperationREAD(op, NFSv4MoverHandler.this);
                case nfs_opnum4.OP_WRITE:
                    return new EDSOperationWRITE(op, NFSv4MoverHandler.this);
                case nfs_opnum4.OP_EXCHANGE_ID:
                    return new OperationEXCHANGE_ID(op);
                case nfs_opnum4.OP_CREATE_SESSION:
                    return new OperationCREATE_SESSION(op);
                case nfs_opnum4.OP_DESTROY_SESSION:
                    return new OperationDESTROY_SESSION(op);
                case nfs_opnum4.OP_SEQUENCE:
                    return new OperationSEQUENCE(op);
                case nfs_opnum4.OP_RECLAIM_COMPLETE:
                    return new OperationRECLAIM_COMPLETE(op);
                case nfs_opnum4.OP_BIND_CONN_TO_SESSION:
                    return new OperationBIND_CONN_TO_SESSION(op);
                case nfs_opnum4.OP_DESTROY_CLIENTID:
                    return new OperationDESTROY_CLIENTID(op);
                case nfs_opnum4.OP_ILLEGAL:
            }

            return new OperationILLEGAL(op);
        }
    }

    NfsMover getOrCreateMover(InetSocketAddress remoteAddress, stateid4 stateid, byte[] fh) throws ChimeraNFSException {
        NfsMover mover = _activeIO.get(stateid);
        if (mover == null) {
            /*
             * a mover for the same file and the same client can be re-used.
             */
            /**
             * FIXME: this is verry fragile, as we assume that stateid
             * structure is known and contains clientid.
             */
            long clientId = Bytes.getLong(stateid.other, 0);
            return _activeIO.values().stream()
                    .filter(m -> Bytes.getLong(m.getStateId().other, 0) == clientId)
                    .filter(m -> Arrays.equals(fh, m.getNfsFilehandle()))
                    .findFirst()
                    .orElse(null);
        }
        return mover;
    }

    /**
     * Find a mover for a corresponding nfs handle.
     * @param fh file handle
     * @return a mover for a given nfs file handle
     * @throws ChimeraNFSException
     */
    NfsMover getPnfsIdByHandle(byte[] fh) throws BadHandleException {
        return _activeIO.values().stream()
                .filter(m -> Arrays.equals(fh, m.getNfsFilehandle()))
                .findAny()
                .orElseThrow(() -> new BadHandleException("No mover for found for given file handle"));
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
        _cleanerExecutor.shutdown();
    }

    NFSServerV41 getNFSServer() {
        return _embededDS;
    }

    class MoverValidator implements Runnable {

        @Override
        public void run() {
            Instant now = Instant.now();

            _activeIO.values()
                    .stream()
                    .filter(NfsMover::hasSession)
                    .filter(mover -> Instant.ofEpochMilli(mover.getLastTransferred()).plus(deadMoverIdleTime).isBefore(now))
                    .forEach(mover -> {
                        _log.debug("Verifying inactive mover {}", mover);
                        final org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateId = mover.getProtocolInfo().stateId();
                        CellStub.addCallback(_door.send(mover.getPathToDoor(),
                                new DoorValidateMoverMessage<>(-1, mover.getFileAttributes().getPnfsId(), _bootVerifier, legacyStateId)),
                                new NfsMoverValidationCallback(mover),
                                    _cleanerExecutor);
                    });
        }

    }
}
