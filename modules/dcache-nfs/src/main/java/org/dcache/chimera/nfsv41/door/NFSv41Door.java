package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.glassfish.grizzly.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerPublisher;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.nfsv41.door.proxy.NfsProxyIoFactory;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoFactory;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoMdsOpFactory;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.commons.util.NDC;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.ExportFile;
import org.dcache.nfs.FsExport;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.status.LayoutTryLaterException;
import org.dcache.nfs.status.LayoutUnavailableException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.status.BadStateidException;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.NFSv4Defaults;
import org.dcache.nfs.v4.RoundRobinStripingPattern;
import org.dcache.nfs.v4.StateDisposeListener;
import org.dcache.nfs.v4.StripingPattern;
import org.dcache.nfs.v4.xdr.device_addr4;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.layout4;
import org.dcache.nfs.v4.xdr.layoutiomode4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.multipath_list4;
import org.dcache.nfs.v4.xdr.netaddr4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VfsCache;
import org.dcache.nfs.vfs.VfsCacheConfig;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.vehicles.DoorValidateMoverMessage;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.gss.GssSessionManager;

import static java.util.stream.Collectors.toList;

import java.util.stream.Stream;

import static org.dcache.chimera.nfsv41.door.ExceptionUtils.asNfsException;

public class NFSv41Door extends AbstractCellComponent implements
        NFSv41DeviceManager, CellCommandListener,
        CellMessageReceiver, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(NFSv41Door.class);

    /**
     * Array if layout types supported by the door. For now, dCache supports only
     * NFS4.1 file layout type.
     */
    private static final int[] SUPPORTED_LAYOUT_TYPES = new int[]{layouttype4.LAYOUT4_NFSV4_1_FILES};

    /**
     * A mapping between pool name, nfs device id and pool's ip addresses.
     */
    private final PoolDeviceMap _poolDeviceMap = new PoolDeviceMap(new RoundRobinStripingPattern<>());

    /*
     * reserved device for IO through MDS (for pnfs dot files)
     */
    private static final deviceid4 MDS_ID = PoolDeviceMap.deviceidOf(0);

    private final Map<stateid4, NfsTransfer> _ioMessages = new ConcurrentHashMap<>();

    /**
     * The usual timeout for NFS operations is 30s. Nevertheless, as client
     * will block, we try to block as short as we can. The rule for interactive users:
     * never block longer than 10s.
     */
    private static final long NFS_REPLY_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    private static final long NFS_REQUEST_BLOCKING = 100; // in Millis

    /**
     * Given that the timeout is pretty short, the retry period has to
     * be rather small too.
     */
    private static final long NFS_RETRY_PERIOD = 500; // In millis

    /**
     * Cell communication helper.
     */
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private PnfsHandler _pnfsHandler;

    private String _ioQueue;

    /**
     * TCP port number to bind.
     */
    private int _port;

    /**
     * nfs versions to run.
     */
    private Set<String> _versions;

    private static final String V3 = "3";
    private static final String V41 = "4.1";

    /**
     * embedded nfs server
     */
    private NFSServerV41 _nfs4;

    /**
     * RPC service
     */
    private  OncRpcSvc _rpcService;

    private StrategyIdMapper _idMapper;

    private boolean _enableRpcsecGss;

    private VfsCache _vfs;

    private LoginBrokerPublisher _loginBrokerPublisher;

    private ProxyIoFactory _proxyIoFactory;

    private static final TransferRetryPolicy RETRY_POLICY =
        new TransferRetryPolicy(Integer.MAX_VALUE, NFS_RETRY_PERIOD, NFS_REPLY_TIMEOUT);

    private VfsCacheConfig _vfsCacheConfig;

    /**
     * indicates is this cell a well-known cell or not
     */
    private boolean _isWellKnown;

    public void setEnableRpcsecGss(boolean enable) {
        _enableRpcsecGss = enable;
    }

    public void setIdMapper(StrategyIdMapper idMapper)    {
        _idMapper = idMapper;
    }

    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    public void setPnfsHandler(PnfsHandler pnfs) {
        _pnfsHandler = pnfs;
    }

    private JdbcFs _fileFileSystemProvider;
    public void setFileSystemProvider(JdbcFs fs) {
        _fileFileSystemProvider = fs;
    }

    private ExportFile _exportFile;
    public void setExportFile(ExportFile export) {
        _exportFile = export;
    }

    public void setIoQueue(String ioQueue) {
        _ioQueue = ioQueue;
    }

    public void setPortNumber(int port) {
        _port = port;
    }

    public void setVersions(String[] versions) {
        _versions = Sets.newHashSet(versions);
    }

    @Required
    public void setLoginBrokerPublisher(LoginBrokerPublisher loginBrokerPublisher) {
        _loginBrokerPublisher = loginBrokerPublisher;
    }

    @Required
    public void setVfsCacheConfig(VfsCacheConfig vfsCacheConfig) {
        _vfsCacheConfig = vfsCacheConfig;
    }

    public void init() throws Exception {

        _isWellKnown = getArgs().getBooleanOption("export");

        OncRpcSvcBuilder oncRpcSvcBuilder = new OncRpcSvcBuilder()
                .withPort(_port)
                .withTCP()
                .withAutoPublish()
                .withWorkerThreadIoStrategy();
        if (_enableRpcsecGss) {
            oncRpcSvcBuilder.withGssSessionManager(new GssSessionManager(_idMapper));
        }
        _rpcService = oncRpcSvcBuilder.build();

        _vfs = new VfsCache(new ChimeraVfs(_fileFileSystemProvider, _idMapper), _vfsCacheConfig);

        MountServer ms = new MountServer(_exportFile, _vfs);
        _rpcService.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);

        for (String version : _versions) {
            switch (version) {
                case V3:
                    NfsServerV3 nfs3 = new NfsServerV3(_exportFile, _vfs);
                    _rpcService.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
                    _loginBrokerPublisher.setTags(Collections.emptyList());
                    break;
                case V41:
                    final NFSv41DeviceManager _dm = this;
                    _proxyIoFactory = new NfsProxyIoFactory(_dm);
                    _nfs4 = new NFSServerV41(new ProxyIoMdsOpFactory(_proxyIoFactory, new MDSOperationFactory()),
                            _dm, _vfs, _exportFile);
                    _rpcService.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _nfs4);
                    updateLbPaths();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported NFS version: " + version);
            }
        }

        _rpcService.start();

    }

    public void destroy() throws IOException {
        _rpcService.stop();
        if (_nfs4 != null) {
            _nfs4.getStateHandler().shutdown();
            _proxyIoFactory.shutdown();
        }
    }

    /*
     * Handle reply from the pool that mover actually started.
     *
     * If the pools is not know yet, create a mapping between pool name
     * and NFSv4.1 device id. Finally, notify waiting request that we have got
     * the reply for LAYOUTGET
     */
    public void messageArrived(PoolPassiveIoFileMessage<org.dcache.chimera.nfs.v4.xdr.stateid4> message) {

        String poolName = message.getPoolName();
        long verifier = message.getVerifier();
        InetSocketAddress[] poolAddresses = message.socketAddresses();

        _log.debug("NFS mover ready: {}", poolName);

        PoolDS device = _poolDeviceMap.getOrCreateDS(poolName, verifier, poolAddresses);

        org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid = message.challange();
        NfsTransfer transfer = _ioMessages.get(new stateid4(legacyStateid.other, legacyStateid.seqid.value));
        /*
         * We got a notification for a transfer which was not
         * started by us.
         *
         * Door reboot.
         */
        if(transfer != null) {
            transfer.redirect(device);
        }
    }

    public void messageArrived(DoorTransferFinishedMessage transferFinishedMessage) {

        NFS4ProtocolInfo protocolInfo = (NFS4ProtocolInfo)transferFinishedMessage.getProtocolInfo();
        _log.debug("Mover {} done.", protocolInfo.stateId());
        org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid = protocolInfo.stateId();
        NfsTransfer transfer = _ioMessages.remove(new stateid4(legacyStateid.other, legacyStateid.seqid.value));
        if(transfer != null) {
                transfer.finished(transferFinishedMessage);
                transfer.notifyBilling(transferFinishedMessage.getReturnCode(), "");
                _vfs.invalidateStatCache(transfer.getInode());
        }
    }

    public DoorValidateMoverMessage<org.dcache.chimera.nfs.v4.xdr.stateid4> messageArrived(DoorValidateMoverMessage<org.dcache.chimera.nfs.v4.xdr.stateid4> message) {
        org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid = message.getChallenge();
        stateid4 stateid = new stateid4(legacyStateid.other, legacyStateid.seqid.value);

        boolean isValid = false;
        try {
            NFS4Client nfsClient = _nfs4.getStateHandler().getClientIdByStateId(stateid);
            // will throw exception if state does not exists
            nfsClient.state(stateid);
            isValid = true;
        } catch (BadStateidException e) {
        } catch (ChimeraNFSException e) {
            _log.warn("Unexpected NFS exception: {}", e.getMessage() );
        }
        message.setIsValid(isValid);
        return message;
    }

    // NFSv41DeviceManager interface

    /*
    	The most important calls is LAYOUTGET, OPEN, CLOSE, LAYOUTRETURN
    	The READ, WRITE and  COMMIT goes to storage device.

    	We assume the following mapping between nfs and dcache:

    	     NFS     |  dCache
    	_____________|________________________________________
    	LAYOUTGET    : get pool, bind the answer to the client
    	OPEN         : send IO request to the pool
    	CLOSE        : sent end-of-IO to the pool, LAYOUTRECALL
    	LAYOUTRETURN : unbind pool from client

     */

    @Override
    public device_addr4 getDeviceInfo(CompoundContext context, deviceid4 deviceId) {
        /* in case of MDS access we return the same interface which client already connected to */
        if (deviceId.equals(MDS_ID)) {
            return deviceAddrOf( new RoundRobinStripingPattern<>(),
                    new InetSocketAddress[] { context.getRpcCall().getTransport().getLocalSocketAddress() } );
        }

        PoolDS ds = _poolDeviceMap.getByDeviceId(deviceId);
        if( ds == null) {
            return null;
        }
        return ds.getDeviceAddr();
    }

    /**
     * ask pool manager for a file
     *
     * On successful reply from pool manager corresponding O request will be sent
     * to the pool to start a NFS mover.
     *
     * @throws ChimeraNFSException in case of NFS friendly errors ( like ACCESS )
     * @throws IOException in case of any other errors
     */
    @Override
    public Layout layoutGet(CompoundContext context, Inode nfsInode, int layoutType, int ioMode, stateid4 stateid)
            throws IOException {

        FsInode inode = _fileFileSystemProvider.inodeFromBytes(nfsInode.getFileId());
        PnfsId pnfsId = new PnfsId(inode.getId());
        Transfer.initSession(_isWellKnown, false);
        NDC.push(pnfsId.toString());
        NDC.push(context.getRpcCall().getTransport().getRemoteSocketAddress().toString());
        try {

            if (layoutType != layouttype4.LAYOUT4_NFSV4_1_FILES) {
                _log.warn("unsupported layout type ({}) requests from");
                throw new LayoutUnavailableException("Unsuported layout type: " + layoutType);
            }

            deviceid4 deviceid;

            final NFS4Client client;
            if (context.getMinorversion() == 0) {
                /* if we need to run proxy-io with NFSv4.0 */
                client = context.getStateHandler().getClientIdByStateId(stateid);
            } else {
                client = context.getSession().getClient();
            }

            final NFS4State nfsState = client.state(stateid);

            // serialize all requests by the same stateid
            synchronized(nfsState) {

                if (inode.type() != FsInodeType.INODE || inode.getLevel() != 0) {
                    /*
                     * all non regular files ( AKA pnfs dot files ) provided by door itself.
                     */
                    deviceid = MDS_ID;
                } else {

                    final InetSocketAddress remote = context.getRpcCall().getTransport().getRemoteSocketAddress();
                    final NFS4ProtocolInfo protocolInfo = new NFS4ProtocolInfo(remote,
                                new org.dcache.chimera.nfs.v4.xdr.stateid4(stateid),
                                nfsInode.toNfsHandle()
                            );

                    NfsTransfer transfer = _ioMessages.get(stateid);
                    if (transfer == null) {
                        transfer = new NfsTransfer(_pnfsHandler, nfsInode,
                                context.getRpcCall().getCredential().getSubject());

                        transfer.setProtocolInfo(protocolInfo);
                        transfer.setCellName(this.getCellName());
                        transfer.setDomainName(this.getCellDomainName());
                        transfer.setBillingStub(_billingStub);
                        transfer.setPoolStub(_poolManagerStub);
                        transfer.setPoolManagerStub(_poolManagerStub);
                        transfer.setPnfsId(pnfsId);
                        transfer.setClientAddress(remote);
                        transfer.readNameSpaceEntry(ioMode != layoutiomode4.LAYOUTIOMODE4_READ);

                        if (transfer.isWrite()) {
                            _log.debug("looking for write pool for {}", transfer.getPnfsId());
                        } else {
                            _log.debug("looking for read pool for {}", transfer.getPnfsId());
                        }

                        /*
                         * Bind transfer to open-state.
                         * Cleanup transfer when state invalidated
                         */
                        nfsState.addDisposeListener((NFS4State state) -> {
                            Transfer t = _ioMessages.remove(stateid);
                            if (t != null) {
                                t.killMover(0);
                            }
                        });

                         _ioMessages.put(stateid, transfer);
                    }

                    PoolDS ds = transfer.getPoolDataServer(_ioQueue, NFS_REQUEST_BLOCKING);
                    deviceid = ds.getDeviceId();
                }
            }

            nfs_fh4 fh = new nfs_fh4(nfsInode.toNfsHandle());

            //  -1 is special value, which means entire file
            layout4 layout = Layout.getLayoutSegment(deviceid, NFSv4Defaults.NFS4_STRIPE_SIZE, fh, ioMode,
                    0, nfs4_prot.NFS4_UINT64_MAX);

            /*
             * on on error client will issue layout return.
             * return we need a different stateid for layout to keep
             * mover as long as file is opened.
             *
             * Well, according to spec we have to return a different
             * stateid anyway.
             */
            final NFS4State layoutStateId = client.createState();

            /*
             * as  we will never see layout return with this stateid clean it
             * when open state id is disposed
             */
            nfsState.addDisposeListener(
                    // use java7 for 2.10 backport
                    new StateDisposeListener() {

                        @Override
                        public void notifyDisposed(NFS4State state) {
                            try {
                                client.releaseState(layoutStateId.stateid());
                            }catch(ChimeraNFSException e) {
                                _log.warn("can't release layout stateid.: {}", e.getMessage() );
                            }
                        }
                    }
            );
            return new Layout(true, layoutStateId.stateid(), new layout4[]{layout});

        } catch (CacheException | TimeoutException | ExecutionException e) {
            throw asNfsException(e, LayoutTryLaterException.class);
        } catch (InterruptedException e) {
            throw new LayoutTryLaterException(e.getMessage(), e);
        } finally {
            CDC.clearMessageContext();
            NDC.pop();
            NDC.pop();
        }

    }

    @Override
    public List<deviceid4> getDeviceList(CompoundContext context) {
        return Lists.newArrayList(_poolDeviceMap.getDeviceIds());
    }


    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#releaseDevice(stateid4 stateid)
     */
    @Override
    public void layoutReturn(CompoundContext context, stateid4 stateid) throws IOException {

        _log.debug("Releasing device by stateid: {}", stateid);

        NfsTransfer transfer = _ioMessages.get(stateid);
        if (transfer == null) {
            return;
        }

        _log.debug("Sending KILL to {}@{}", transfer.getMoverId(), transfer.getPool());
        transfer.killMover(0);

        try {
            if(transfer.hasMover() && !transfer.waitForMover(500)) {
                throw new DelayException("Mover not stopped");
            }
        } catch (FileNotFoundCacheException e){
            // REVISIT: remove when pool will stop sending this exception
            _log.info("Failed removed while being open mover: {}@{} : {}",
                    transfer.getMoverId(), transfer.getPool(), e.getMessage());
        } catch (CacheException | InterruptedException e) {
            _log.info("Failed to kill mover: {}@{} : {}",
                    transfer.getMoverId(), transfer.getPool(), e.getMessage());
            throw new NfsIoException(e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.nfsv4.NFSv41DeviceManager#getLayoutTypes()
     */
    @Override
    public int[] getLayoutTypes() {
        return SUPPORTED_LAYOUT_TYPES;
    }

    public void setBillingStub(CellStub stub) {
        _billingStub = stub;
    }

    /*
     * Cell specific
     */
    @Override
    public void getInfo(PrintWriter pw) {

        pw.println("NFS (" + _versions + ") door (MDS):");
        if (_nfs4 != null) {
            pw.printf("  IO queue                : %s\n", _ioQueue);
            pw.printf("  Number of NFSv4 clients : %d\n", _nfs4.getStateHandler().getClients().size());
            pw.printf("  Total pools (DS) used   : %d\n", _poolDeviceMap.getEntries().stream().count());
            pw.printf("  Active transfers        : %d\n", _ioMessages.values().size());
            pw.printf("  Known proxy adapters    : %d\n", _proxyIoFactory.getCount());
        }
    }

    @Command(name = "kill mover", hint = "Kill mover on the pool.")
    public class KillMoverCmd implements Callable<String> {

        @Argument(index = 0, metaVar = "pool", usage = "pool name")
        String pool;

        @Argument(index = 1, metaVar = "moverid", usage = "mover id")
        int mover;

        @Override
        public String call() throws Exception {
            PoolMoverKillMessage message = new PoolMoverKillMessage(pool, mover);
            message.setReplyRequired(false);
            _poolStub.notify(new CellPath(pool), message);
            return "Done.";
        }

    }

    @Command(name = "exports reload", hint = "Re-scan export file.")
    public class ExportsReloadCmd implements Callable<String> {

        @Override
        public String call() throws IOException {
            _exportFile.rescan();
            updateLbPaths();
            return "Done.";
        }
    }

    @Command(name = "exports ls", hint = "Dump nfs exports.")
    public class ExportsLsCmd implements Callable<String> {

        @Argument(required = false)
        String host;

        @Override
        public String call() throws IOException {
            Stream<FsExport> exports;
            if (host != null) {
                InetAddress address = InetAddress.getByName(host);
                exports = _exportFile.exportsFor(address);
            } else {
                exports = _exportFile.getExports();
            }
            return exports
                    .map(Object::toString)
                    .collect(Collectors.joining("\n"));
        }
    }

    @Command(name = "kill client", hint = "kill NFS client and it's sessions",
    description = "With this command, dCache responds to " +
            "this specific client as if it was " +
            "restarted. All other NFS clients are " +
            "unaffected." +
            "\n\n" +
            "One use of this command is for an admin " +
            "to allow dCache to recover from a client " +
            "that is itself trying to recover from a " +
            "(perceived) error in dCache response, but " +
            "is failing to do so. Such behaviour can " +
            "result in the client becoming stuck." +
            "\n\n" +
            "This command should not be necessary " +
            "under normal circumstances; please " +
            "contact dCache support if you make " +
            "continued use of it.")
    public class KillClientCmd implements Callable<String> {

        @Argument(required = false, metaVar = "clientid")
        long clientid;

        @Override
        public String call() throws IOException {
            if (_nfs4 == null) {
                return "NFS4 server not running.";
            }

            NFS4Client client = _nfs4.getStateHandler().getClientByID(clientid);
            _nfs4.getStateHandler().removeClient(client);
            return "Done.";
        }
    }

    @Command(name = "show clients", hint = "show active NFSv4 clients",
            description = "Show NFSv4 clients and corresponding sessions.")
    public class ShowClientsCmd implements Callable<String> {

        @Argument(required = false, metaVar = "host")
        String host;

        @Override
        public String call() throws IOException {
            if (_nfs4 == null) {
                return "NFSv4 server not running.";
            }

            InetAddress clientAddress = InetAddress.getByName(host);
            StringBuilder sb = new StringBuilder();

            _nfs4.getStateHandler().getClients()
                    .stream()
                    .filter(c -> host == null? true : c.getRemoteAddress().getAddress().equals(clientAddress))
                    .forEach(c -> {
                        sb.append("    ").append(c).append("\n");
                        c.sessions().forEach((session) -> {
                            sb.append("        ")
                            .append(session)
                            .append(" max slot: ")
                            .append(session.getHighestSlot())
                            .append("/")
                            .append(session.getHighestUsedSlot())
                            .append("\n");
                    });
                });
            return sb.toString();
        }
    }

    @Command(name = "show pools", hint = "show pools to pNFS device mapping",
            description = "Show pools as pNFS devices, including id mapping and "
             + "known IP addresses.")
    public class ShowPoolsCmd implements Callable<String> {

        @Argument(required = false, metaVar = "pool")
        String pool;

        @Override
        public String call() throws IOException {
            return _poolDeviceMap.getEntries()
                    .stream()
                    .filter(d -> pool == null ? true : d.getKey().equals(pool))
                    .map(d -> String.format("    %s : [%s]\n", d.getKey(), d.getValue()))
                    .collect(Collectors.joining());
        }
    }

    @Command(name = "show transfers", hint = "show active transfers",
            description = "Show active transfers excluding proxy-io.")
    public class ShowTransfersCmd implements Callable<String> {

        @Override
        public String call() throws IOException {

            return _ioMessages.values()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining("\n"));
        }
    }

    @Command(name = "show proxyio", hint = "show proxy-io transfers",
            description = "Show active proxy-io transfers.")
    public class ShowProxyIoTransfersCmd implements Callable<String> {

        @Override
        public String call() throws IOException {
            if (_nfs4 == null) {
                return "NFSv4 server not running.";
            }

            StringBuilder sb = new StringBuilder();
            _proxyIoFactory.forEach(p -> {
                NfsTransfer t = _ioMessages.get(p.getStateId());
                sb.append(t).append('\n');
            });
            return sb.toString();
        }
    }

    static class PoolDS {

        private final deviceid4 _deviceId;
        private final InetSocketAddress[] _socketAddress;
        private final device_addr4 _deviceAddr;
        private final long _verifier;

        public PoolDS(deviceid4 deviceId, StripingPattern<InetSocketAddress[]> stripingPattern,
                InetSocketAddress[] ip, long verifier) {
            _deviceId = deviceId;
            _socketAddress = ip;
            _deviceAddr = deviceAddrOf(stripingPattern, ip);
            _verifier = verifier;
        }

        public deviceid4 getDeviceId() {
            return _deviceId;
        }

        public InetSocketAddress[] getInetSocketAddress() {
            return _socketAddress;
        }

        public device_addr4 getDeviceAddr() {
            return _deviceAddr;
        }

        public long getVerifier() {
            return _verifier;
        }

        @Override
        public String toString() {
            return String.format("DS: %s, InetAddress: %s",
                    _deviceId, Arrays.toString(_socketAddress));
        }
    }

    private static class NfsTransfer extends RedirectedTransfer<PoolDS> {

        private final Inode _nfsInode;

        NfsTransfer(PnfsHandler pnfs, Inode nfsInode, Subject ioSubject) {
            super(pnfs, Subjects.ROOT, ioSubject,  new FsPath("/"));
            _nfsInode = nfsInode;
        }

        @Override
        public String toString() {
            return String.format("    %s : %s %s@%s, OS=%s,cl=[%s]",
                    getPnfsId(),
                    isWrite() ? "WRITE" : "READ",
                    getMoverId(),
                    getPool(),
                    ((NFS4ProtocolInfo)getProtocolInfoForPool()).stateId(),
                    ((NFS4ProtocolInfo)getProtocolInfoForPool()).getSocketAddress().getAddress().getHostAddress());
        }

        Inode getInode() {
            return _nfsInode;
        }

        PoolDS  getPoolDataServer(String queue, long timeout) throws
                InterruptedException, ExecutionException,
                TimeoutException, CacheException {

            ListenableFuture<Void> redirectFuture;
            synchronized (this) {
                /*
                 * Check try to re-run the selection if no pool selected yet.
                 * Restart/ping mover if not running yet.
                 */
                if (getPool() == null) {
                    // we did not select a pool
                    redirectFuture = selectPoolAndStartMoverAsync(queue, RETRY_POLICY);
                } else {
                    // we may re-send the request, but pool will handle it
                    redirectFuture = startMoverAsync(queue, NFS_REQUEST_BLOCKING);
                }
            }

            Stopwatch sw = Stopwatch.createStarted();
            redirectFuture.get(NFS_REQUEST_BLOCKING, TimeUnit.MILLISECONDS);
            _log.debug("mover ready: pool={} moverid={}", getPool(), getMoverId());

            return  waitForRedirect(NFS_REQUEST_BLOCKING - sw.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /*
     * To allow the transfer monitoring in the httpd cell to recognize us
     * as a door, we have to emulate LoginManager.  To emulate
     * LoginManager we list ourselves as our child.
     */
    @Command(name = "get children", hint = "Get door's children associated with transfers.")
    public class GetDoorChildrenCmd implements Callable<Serializable> {
        @Option(name = "binary", usage = "returns binary object instead of string form")
        boolean isBinary;

        @Override
        public Serializable call() throws Exception {
            if (isBinary) {
                String[] children = new String[]{NFSv41Door.this.getCellName()};
                return new LoginManagerChildrenInfo(NFSv41Door.this.getCellName(), NFSv41Door.this.getCellDomainName(), children);
            } else {
                return NFSv41Door.this.getCellName();
            }
        }
    }

    @Command(name = "get door info", hint = "Provide information about the door and current transfers.")
    public class GetDoorInfoCmd implements Callable<Serializable> {
        @Option(name = "binary", usage = "returns binary object instead of string form")
        boolean isBinary;

        @Override
        public Serializable call() throws Exception {

            List<IoDoorEntry> entries = _ioMessages.values()
                    .stream()
                    .map(Transfer::getIoDoorEntry)
                    .collect(toList());

            IoDoorInfo doorInfo = new IoDoorInfo(NFSv41Door.this.getCellName(), NFSv41Door.this.getCellDomainName());
            doorInfo.setProtocol("NFSV4.1", "0");
            doorInfo.setOwner("");
            doorInfo.setProcess("");
            doorInfo.setIoDoorEntries(entries.toArray(new IoDoorEntry[0]));
            return isBinary ? doorInfo : doorInfo.toString();
        }
    }

    /**
     * Create a multipath based NFSv4.1 file layout address.
     *
     * @param stripingPattern of the device
     * @param deviceAddress
     * @return device address
     */
    public static device_addr4 deviceAddrOf(StripingPattern<InetSocketAddress[]> stripingPattern,
            InetSocketAddress[]... deviceAddress) {

        nfsv4_1_file_layout_ds_addr4 file_type = new nfsv4_1_file_layout_ds_addr4();

        file_type.nflda_multipath_ds_list = new multipath_list4[deviceAddress.length];

        for (int i = 0; i < deviceAddress.length; i++) {
            file_type.nflda_multipath_ds_list[i] = toMultipath(deviceAddress[i]);
        }

        file_type.nflda_stripe_indices = stripingPattern.getPattern(deviceAddress);

        XdrBuffer xdr = new XdrBuffer(128);
        try {
            xdr.beginEncoding();
            file_type.xdrEncode(xdr);
            xdr.endEncoding();
        } catch (OncRpcException e) {
            /* forced by interface, should never happen. */
            throw new RuntimeException("Unexpected OncRpcException:", e);
        } catch (IOException e) {
            /* forced by interface, should never happen. */
            throw new RuntimeException("Unexpected IOException:", e);
        }

        Buffer body = xdr.asBuffer();
        byte[] retBytes = new byte[body.remaining()];
        body.get(retBytes);

        device_addr4 addr = new device_addr4();
        addr.da_layout_type = layouttype4.LAYOUT4_NFSV4_1_FILES;
        addr.da_addr_body = retBytes;

        return addr;

    }

    private static multipath_list4 toMultipath(InetSocketAddress[] addresses)
    {
        multipath_list4 multipath = new multipath_list4();
        multipath.value = new netaddr4[addresses.length];
        for(int i = 0; i < addresses.length; i++) {
            multipath.value[i] = new netaddr4(addresses[i]);
        }
        return multipath;
    }

    @Command(name="stats", hint = "Show nfs requests statstics.")
    public class NfsStatsCmd implements Callable<String> {

        @Option(name = "c", usage = "clear current statistics values")
        boolean clean;

        @Override
        public String call() {
            RequestExecutionTimeGauges<String> gauges = _nfs4.getStatistics();
            StringBuilder sb = new StringBuilder();
            sb.append("Stats:").append("\n").append(gauges.toString("ns"));

            if (clean) {
                gauges.reset();
            }
            return sb.toString();
        }
    }

    @Command(name = "pool reset id", hint = "Reset device id associated with the pool.")
    public class PoolResetDeviceidCmd implements Callable<String> {

        @Argument(metaVar = "pool")
        String pool;

        @Override
        public String call() {
            PoolDS ds = _poolDeviceMap.remove(pool);
            if (ds != null) {
                return "Pools " + pool + " as: " + ds + " removed.";
            } else {
                return "pool " + pool + " Not Found.";
            }
        }
    }

    private void updateLbPaths() {
        List<String> exportPaths = _exportFile.getExports().map(FsExport::getPath).distinct().collect(toList());
        _loginBrokerPublisher.setReadPaths(exportPaths);
        _loginBrokerPublisher.setWritePaths(exportPaths);
    }
}
