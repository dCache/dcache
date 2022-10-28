package org.dcache.chimera.nfsv41.mover;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.chimera.nfsv41.common.StatsDecoratedOperationExecutor;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.status.BadStateidException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.AbstractOperationExecutor;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NFSv41Session;
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
import org.dcache.oncrpc4j.rpc.IoStrategy;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.dcache.oncrpc4j.rpc.RpcLoginService;
import org.dcache.oncrpc4j.rpc.gss.GssSessionManager;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;
import org.dcache.vehicles.DoorValidateMoverMessage;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Factory and transfer service for NFS movers.
 *
 * @since 1.9.11
 */
public class NfsTransferService
      implements MoverFactory, TransferService<NfsMover>, CellCommandListener, CellInfoProvider,
      CellIdentityAware {

    private static final Logger _log = LoggerFactory.getLogger(NfsTransferService.class);

    private boolean _withGss;
    private InetSocketAddress[] _localSocketAddresses;
    private CellStub _door;
    private PostTransferService _postTransferService;
    private final long _bootVerifier = System.currentTimeMillis();
    private PnfsHandler _pnfsHandler;
    private int _minTcpPort;
    private int _maxTcpPort;
    private IoStrategy _ioStrategy;

    /**
     * The number of missed leases before pool will query door for mover validation.
     */
    private static final int LEASE_MISSES = 3;

    /**
     * All registered NFS movers.
     */
    private final Map<stateid4, NfsMover> _activeIO = new ConcurrentHashMap<>();

    /**
     * NFSv4 operation executor with requests statistics.
     */
    private final StatsDecoratedOperationExecutor _operationFactory =
          new StatsDecoratedOperationExecutor(new EDSNFSv4OperationFactory());

    /**
     * The NFSv4.1 server
     */
    private NFSServerV41 _embededDS;

    /**
     * RPC service
     */
    private OncRpcSvc _rpcService;

    /**
     * Scheduler to run periodic (cleanup) tasks.
     */
    private final ScheduledExecutorService _cleanerExecutor;

    /**
     * Mover inactivity time before pool will query the door for mover validation.
     */
    private Duration deadMoverIdleTime;

    /**
     * file to store TCP port number used by pool.
     */
    private File _tcpPortFile;

    private CellAddressCore _cellAddress;

    @Override
    public void setCellAddress(CellAddressCore address) {
        _cellAddress = address;
    }

    public NfsTransferService() {
        _cleanerExecutor = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                    .setNameFormat("NFS mover validationthread")
                    .build()
        );
    }

    public void init() throws IOException, GSSException, OncRpcException {

        tryToStartRpcService();

        int localPort = _rpcService.getInetSocketAddress(IpProtocolType.TCP).getPort();
        _localSocketAddresses = localSocketAddresses(NetworkUtils.getLocalAddresses(), localPort);

        _embededDS = new NFSServerV41.Builder()
              .withOperationExecutor(_operationFactory)
              .build();
        _rpcService.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4),
              _embededDS);

        // Make mover validation schedule to match nfs state handler lease timeout.
        deadMoverIdleTime = _embededDS.getStateHandler().getLeaseTime()
              .multipliedBy(LEASE_MISSES);
        _cleanerExecutor.scheduleAtFixedRate(new MoverValidator(), deadMoverIdleTime.toSeconds(),
              deadMoverIdleTime.toSeconds(), TimeUnit.SECONDS);
        _cleanerExecutor.scheduleAtFixedRate(new MoverResendRedirect(), 30, 30, TimeUnit.SECONDS);
    }

    private void tryToStartRpcService() throws GSSException, IOException {

        PortRange portRange;
        int minTcpPort = _minTcpPort;
        int maxTcpPort = _maxTcpPort;

        try {
            List<String> lines = Files.readAllLines(_tcpPortFile.toPath(),
                  StandardCharsets.US_ASCII);
            if (!lines.isEmpty()) {
                String line = lines.get(0);

                int savedPort = Integer.parseInt(line);
                if (savedPort >= _minTcpPort && savedPort <= _maxTcpPort) {
                    /*
                     *if saved port with in the range, then restrict range to a single port
                     * to enforce it.
                     */
                    minTcpPort = savedPort;
                    maxTcpPort = savedPort;
                }
            }
        } catch (NumberFormatException e) {
            // garbage in the file.
            _log.warn("Invalid content in the port file {} : {}", _tcpPortFile, e.getMessage());
        } catch (NoSuchFileException e) {
        }

        boolean bound = false;
        int retry = 3;
        BindException bindException = null;
        do {
            retry--;
            portRange = new PortRange(minTcpPort, maxTcpPort);
            try {

                OncRpcSvcBuilder oncRpcSvcBuilder = new OncRpcSvcBuilder()
                      .withMinPort(portRange.getLower())
                      .withMaxPort(portRange.getUpper())
                      .withTCP()
                      .withoutAutoPublish();

                _log.debug("Using {} IO strategy", _ioStrategy);
                if (_ioStrategy == IoStrategy.SAME_THREAD) {
                    oncRpcSvcBuilder.withSameThreadIoStrategy();
                } else {
                    oncRpcSvcBuilder.withWorkerThreadIoStrategy();
                }

                if (_withGss) {
                    RpcLoginService rpcLoginService = (t, gss) -> Subjects.NOBODY;
                    GssSessionManager gss = new GssSessionManager(rpcLoginService);
                    oncRpcSvcBuilder.withGssSessionManager(gss);
                }

                _rpcService = oncRpcSvcBuilder.build();
                _rpcService.start();

                bound = true;
            } catch (BindException e) {
                bindException = e;
                minTcpPort = _minTcpPort;
                maxTcpPort = _maxTcpPort;
            }
        } while (!bound && retry > 0);

        if (!bound) {
            throw new BindException(
                  "Can't bind to a port within the rage: " + portRange + " : " + bindException);
        }

        int localPort = _rpcService.getInetSocketAddress(IpProtocolType.TCP).getPort();

        // if we had a port range, then store selected port for the next time.
        if (minTcpPort != maxTcpPort) {
            byte[] outputBytes = Integer.toString(localPort).getBytes(StandardCharsets.US_ASCII);
            Files.write(_tcpPortFile.toPath(), outputBytes);
        }
    }

    @Required
    public void setPostTransferService(PostTransferService postTransferService) {
        _postTransferService = postTransferService;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfsHandler) {
        _pnfsHandler = pnfsHandler;
    }

    @Required
    public void setDoorStub(CellStub cellStub) {
        _door = cellStub;
    }

    @Required
    public void setMinTcpPort(int minPort) {
        _minTcpPort = minPort;
    }

    @Required
    public void setMaxTcpPort(int maxPort) {
        _maxTcpPort = maxPort;
    }

    @Required
    public void setIoStrategy(IoStrategy ioStrategy) {
        _ioStrategy = ioStrategy;
    }

    public IoStrategy getIoStrategy() {
        return _ioStrategy;
    }

    public void setTcpPortFile(File path) {
        _tcpPortFile = path;
    }

    public void shutdown() throws IOException {
        _cleanerExecutor.shutdown();
        _embededDS.getStateHandler().shutdown();
        _rpcService.stop();
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message,
          CellPath pathToDoor) throws CacheException {
        return new NfsMover(handle, message, pathToDoor, this, _pnfsHandler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<? extends OpenOption> getChannelCreateOptions() {
        return Sets.newHashSet(StandardOpenOption.CREATE,
              Repository.OpenFlags.NONBLOCK_SPACE_ALLOCATION);
    }

    @Override
    public Cancellable executeMover(final NfsMover mover,
            final CompletionHandler<Void, Void> completionHandler)
            throws DiskErrorCacheException, InterruptedIOException {
        final Cancellable cancellableMover = mover.enable(completionHandler);
        notifyDoorWithRedirect(mover);

        /* An NFS mover doesn't complete until it is cancelled (the door sends a mover kill
         * message when the file is closed).
         */
        return cancellableMover;
    }

    public void notifyDoorWithRedirect(NfsMover mover) {
        CellPath directDoorPath = new CellPath(mover.getPathToDoor().getDestinationAddress());
        final org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateId = mover.getProtocolInfo()
              .stateId();
        _door.notify(directDoorPath,
              new PoolPassiveIoFileMessage<>(_cellAddress.getCellName(), _localSocketAddresses,
                    legacyStateId,
                    _bootVerifier));
    }

    @Override
    public void closeMover(NfsMover mover, CompletionHandler<Void, Void> completionHandler) {
        _postTransferService.execute(mover, completionHandler);
    }

    public void setEnableGss(boolean withGss) {
        _withGss = withGss;
    }

    private InetSocketAddress[] localSocketAddresses(Collection<InetAddress> addresses, int port) {
        return addresses.stream().map(address -> new InetSocketAddress(address, port))
              .toArray(InetSocketAddress[]::new);
    }

    /**
     * Add mover into list of allowed transfers.
     *
     * @param mover
     */
    public void add(NfsMover mover) {
        _log.debug("registering new mover {}", mover);
        _activeIO.put(mover.getStateId(), mover);
    }

    /**
     * Removes mover from the list of allowed transfers.
     *
     * @param mover
     * @return true, if the given mover was known to this transfer service.
     */
    public boolean remove(NfsMover mover) {
        _log.debug("un-registering io handler for stateid {}", mover);
        return _activeIO.remove(mover.getStateId()) != null;
    }

    @Nullable NfsMover getMoverByStateId(CompoundContext context, stateid4 stateid) {
        NfsMover mover = _activeIO.get(stateid);
        if (mover != null) {
            mover.attachSession(context.getSession());
        }
        return mover;
    }

    /**
     * Find a mover for a corresponding nfs handle.
     *
     * @param fh file handle
     * @return a mover for a given nfs file handle
     * @throws ChimeraNFSException
     */
    NfsMover getPnfsIdByHandle(byte[] fh) throws BadHandleException {
        return _activeIO.values().stream()
              .filter(m -> Arrays.equals(fh, m.getNfsFilehandle()))
              .findAny()
              .orElseThrow(
                    () -> new BadHandleException("No mover for found for given file handle"));
    }

    @Command(name = "nfs stats",
          hint = "show nfs requests statistics",
          description = "Displays statistics kept about NFS Client and Server activity. " +
                "Prints average/min/max execution time in ns, for example, for the following operations:\n"
                +
                "\tACCESS - Check Access Rights determines the access rights a user has " +
                "for an object,\n " +
                "EXCHANGE_ID - operation used by the client to register a particular " +
                "client owner with the server,\n" +
                "\tCREATE_SESSION - used by the client to create new session objects on " +
                "the server.\n" +
                "If the optional argument \"c\" is specified statistics is reset.")
    public class NfsStatsCommand implements Callable<String> {

        @Option(name = "c",
              usage = "Clears current statistics values.")
        boolean clearStats;

        @Override
        public String call() {
            RequestExecutionTimeGauges<String> gauges = _operationFactory.getStatistics();
            StringBuilder sb = new StringBuilder();
            sb.append("Stats:").append("\n").append(gauges.toString("ns"));
            if (clearStats) {
                gauges.reset();
            }
            return sb.toString();

        }
    }

    @Command(name = "nfs sessions",
          hint = "show nfs sessions",
          description = "Displays unique session identifier, maximum slot id" +
                " and the highest used slot id for the list of sessions created by client.")
    public class NfsSessionsCommand implements Callable<String> {

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            for (NFS4Client client : _embededDS.getStateHandler().getClients()) {
                sb.append(client).append('\n');
                for (NFSv41Session session : client.sessions()) {
                    sb.append("  ")
                          .append(session)
                          .append(" slots (max/used): ")
                          .append(session.getHighestSlot())
                          .append('/')
                          .append(session.getHighestUsedSlot())
                          .append('\n');
                }
            }
            return sb.toString();
        }
    }

    /**
     * Defines which NFSv4.1 operations can be handle by pNFS data server.
     */
    private class EDSNFSv4OperationFactory extends AbstractOperationExecutor {

        @Override
        protected AbstractNFSv4Operation getOperation(nfs_argop4 op) {

            switch (op.argop) {
                case nfs_opnum4.OP_COMMIT:
                    return new EDSOperationCOMMIT(op, NfsTransferService.this);
                case nfs_opnum4.OP_GETATTR:
                    return new OperationGETATTR(op);
                case nfs_opnum4.OP_PUTFH:
                    return new OperationPUTFH(op);
                case nfs_opnum4.OP_PUTROOTFH:
                    return new OperationPUTROOTFH(op);
                case nfs_opnum4.OP_READ:
                    return new EDSOperationREAD(op, NfsTransferService.this);
                case nfs_opnum4.OP_WRITE:
                    return new EDSOperationWRITE(op, NfsTransferService.this);
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

    class MoverValidator implements Runnable {

        @Override
        public void run() {
            Instant now = Instant.now();

            _activeIO.values()
                  .stream()
                  .filter(NfsMover::hasSession)
                  .filter(mover -> Instant.ofEpochMilli(mover.getLastTransferred())
                        .plus(deadMoverIdleTime).isBefore(now))
                  .forEach(mover -> {
                      _log.debug("Verifying inactive mover {}", mover);
                      final org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateId = mover.getProtocolInfo()
                            .stateId();
                      CellStub.addCallback(_door.send(mover.getPathToDoor(),
                                  new DoorValidateMoverMessage<>(-1,
                                        mover.getFileAttributes().getPnfsId(), _bootVerifier,
                                        legacyStateId)),
                            new NfsMoverValidationCallback(mover),
                            _cleanerExecutor);
                  });
        }
    }

    /**
     * Scans active transfers to find movers that wasn't connected by a client and re-sent the
     * redirect information.
     */
    class MoverResendRedirect implements Runnable {

        @Override
        public void run() {
            Instant now = Instant.now();

            // mover is not attached to a session (no connection from client)
            _activeIO.values()
                  .stream()
                  .filter(Predicate.not(NfsMover::hasSession))
                  .filter(mover -> Instant.ofEpochMilli(mover.getLastTransferred()).plusSeconds(5)
                        .isBefore(now))
                  .forEach(mover -> {
                      _log.warn("Re-sending mover redirect {}", mover);
                      notifyDoorWithRedirect(mover);
                  });
        }
    }
}
