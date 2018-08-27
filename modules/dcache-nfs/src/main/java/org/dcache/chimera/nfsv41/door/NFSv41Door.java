package org.dcache.chimera.nfsv41.door;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

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
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.nfsv41.door.proxy.NfsProxyIoFactory;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoFactory;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoMdsOpFactory;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.ExportFile;
import org.dcache.nfs.FsExport;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.status.LayoutTryLaterException;
import org.dcache.nfs.status.LayoutUnavailableException;
import org.dcache.nfs.status.BadLayoutException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.status.NoMatchingLayoutException;
import org.dcache.nfs.status.BadStateidException;
import org.dcache.nfs.status.ServerFaultException;
import org.dcache.nfs.status.StaleException;
import org.dcache.nfs.status.UnknownLayoutTypeException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.FlexFileLayoutDriver;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.NFSv4Defaults;
import org.dcache.nfs.v4.xdr.clientid4;
import org.dcache.nfs.v4.xdr.device_addr4;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.layout4;
import org.dcache.nfs.v4.xdr.layoutiomode4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.LayoutDriver;
import org.dcache.nfs.v4.NfsV41FileLayoutDriver;
import org.dcache.nfs.v4.xdr.length4;
import org.dcache.nfs.v4.xdr.offset4;
import org.dcache.nfs.v4.xdr.utf8str_mixed;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VfsCache;
import org.dcache.nfs.vfs.VfsCacheConfig;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.Glob;
import org.dcache.util.CDCScheduledExecutorServiceDecorator;
import org.dcache.util.NDC;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.DoorValidateMoverMessage;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.dcache.oncrpc4j.rpc.gss.GssSessionManager;

import static java.util.stream.Collectors.toList;

import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;

import org.dcache.auth.attributes.Restrictions;

import static org.dcache.chimera.nfsv41.door.ExceptionUtils.asNfsException;

public class NFSv41Door extends AbstractCellComponent implements
        NFSv41DeviceManager, CellCommandListener,
        CellMessageReceiver, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(NFSv41Door.class);

    /**
     * Layout type specific driver.
     */

    private final Map<layouttype4, LayoutDriver> _supportedDrivers = ImmutableMap.of(
            layouttype4.LAYOUT4_FLEX_FILES,  new FlexFileLayoutDriver(4, 1, new utf8str_mixed("17"), new utf8str_mixed("17"), lr -> {}),
            layouttype4.LAYOUT4_NFSV4_1_FILES,  new NfsV41FileLayoutDriver()
    );

    /**
     * A mapping between pool name, nfs device id and pool's ip addresses.
     */
    private final PoolDeviceMap _poolDeviceMap = new PoolDeviceMap();

    private final Map<stateid4, NfsTransfer> _ioMessages = new ConcurrentHashMap<>();

    /**
     * Maximal time the NFS request will blocked before we reply with
     * NFSERR_DELAY. The usual timeout for NFS operations is 30s. Nevertheless,
     * as client's other requests will blocked as well, we try to block as short
     * as we can. The rule for interactive users: never block longer than 10s.
     */
    private static final long NFS_REQUEST_BLOCKING = TimeUnit.SECONDS.toMillis(3);

    /**
     * Given that the timeout is pretty short, the retry period has to
     * be rather small too.
     */
    private static final long NFS_RETRY_PERIOD = 500; // In millis

    /**
     * How long stage request can hang around. As the tape system can be broken,
     * stage request may stay in a restore queue for a number of days.
     *
     * One week (7 days) is good enough to cover most of the public holidays.
     */
    private static final long STAGE_REQUEST_TIMEOUT = TimeUnit.DAYS.toMillis(7);

    /**
     * Amount of information logged by access logger.
     */
    private AccessLogMode _accessLogMode;

    /**
     * Cell communication helper.
     */
    private CellStub _poolStub;
    private PoolManagerStub _poolManagerStub;
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
    private ChimeraVfs _chimeraVfs;

    private LoginBrokerPublisher _loginBrokerPublisher;

    private ProxyIoFactory _proxyIoFactory;

    private Consumer<DoorRequestInfoMessage> _kafkaSender = (s) -> {};

    /**
     * Retry policy used for accessing files.
     */
    private static final TransferRetryPolicy POOL_SELECTION_RETRY_POLICY =
        new TransferRetryPolicy(Integer.MAX_VALUE, NFS_RETRY_PERIOD, STAGE_REQUEST_TIMEOUT);

    private VfsCacheConfig _vfsCacheConfig;

    /**
     * {@link ExecutorService} used to issue call-backs to the client.
     */
    private final ScheduledExecutorService _callbackExecutor =
            new CDCScheduledExecutorServiceDecorator<>(Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("callback-%d").build()));

    /**
     * Exception thrown by transfer if accessed after mover have finished.
     */
    private static final ChimeraNFSException DELAY = new DelayException("Mover finished, EAGAIN");

    /**
     * Exception thrown by transfer if accessed after mover have finished.
     */
    private static final ChimeraNFSException POISON = new NfsIoException("Mover finished, EIO");

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

    public void setPoolManagerStub(PoolManagerStub stub)
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

    @Required
    public void setAccessLogMode(AccessLogMode accessLogMode) {
        _accessLogMode = accessLogMode;
    }

    @Autowired(required = false)
    public void setKafkaTemplate(KafkaTemplate kafkaTemplate) {
       _kafkaSender = kafkaTemplate::sendDefault;
    }

    public void init() throws Exception {

        _chimeraVfs = new ChimeraVfs(_fileFileSystemProvider, _idMapper);
        _vfs = new VfsCache(_chimeraVfs, _vfsCacheConfig);
        MountServer ms = new MountServer(_exportFile, _vfs);

        OncRpcSvcBuilder oncRpcSvcBuilder = new OncRpcSvcBuilder()
                .withPort(_port)
                .withTCP()
                .withAutoPublish()
                .withWorkerThreadIoStrategy()
                .withRpcService(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);

        if (_enableRpcsecGss) {
            oncRpcSvcBuilder.withGssSessionManager(new GssSessionManager(_idMapper));
        }

        for (String version : _versions) {
            switch (version) {
                case V3:
                    NfsServerV3 nfs3 = new NfsServerV3(_exportFile, _vfs);
                    oncRpcSvcBuilder.withRpcService(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
                    _loginBrokerPublisher.setTags(Collections.emptyList());
                    break;
                case V41:
                    final NFSv41DeviceManager _dm = this;
                    _proxyIoFactory = new NfsProxyIoFactory(_dm);
                    _nfs4 = new NFSServerV41(new ProxyIoMdsOpFactory(_proxyIoFactory,
                            new AccessLogAwareOperationFactory(_chimeraVfs, _fileFileSystemProvider, _accessLogMode)),
                            _dm, _vfs, _exportFile);
                    oncRpcSvcBuilder.withRpcService(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _nfs4);
                    updateLbPaths();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported NFS version: " + version);
            }
        }

        _rpcService = oncRpcSvcBuilder.build();
        _rpcService.start();
    }

    public void destroy() throws IOException {
        _rpcService.stop();
        _callbackExecutor.shutdown();
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

        /*
         * there are two cases when we can get DoorTransferFinishedMessage:
         *   - triggered by client with CLOSE
         *   - triggered by pool shutdown.
         */
        stateid4 openStateId = new stateid4(legacyStateid.other, legacyStateid.seqid.value);
        NfsTransfer transfer = _ioMessages.get(openStateId);
        if (transfer == null) {
            return;
        }

        transfer.finished(transferFinishedMessage);
        Serializable error = transferFinishedMessage.getErrorObject();
        transfer.notifyBilling(transferFinishedMessage.getReturnCode(), error == null? "" : error.toString());

        // Ensure that we do not kill re-started transfer
        if(transfer.getId() == transferFinishedMessage.getId()) {
            if (transfer.isWrite()) {
                /*
                 * Inject poison to ensure that any other attempt to re-use
                 * the transfer to fail. The cleanup will happen with open-state
                 * disposal on close.
                 */
                transfer.enforceErrorIfRunning(POISON);
            } else {
                // it's ok to remove read mover as it's safe to re-create it again.
                _ioMessages.remove(openStateId);
            }
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

    public void messageArrived(PoolStatusChangedMessage message) {
        if (message.getPoolState() == PoolStatusChangedMessage.DOWN) {
            _log.info("Pool disabled: {}", message.getPoolName());
            recallLayouts(message.getPoolName());
        }
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
    public device_addr4 getDeviceInfo(CompoundContext context, deviceid4 deviceId, layouttype4 layoutType) throws ChimeraNFSException {

        LayoutDriver layoutDriver = getLayoutDriver(layoutType);

        PoolDS ds = _poolDeviceMap.getByDeviceId(deviceId);
        if( ds == null) {
            return null;
        }

        // limit addresses returned to client to the same 'type' as clients own address
        InetAddress clientAddress = context.getRemoteSocketAddress().getAddress();
        InetSocketAddress[] usableAddresses = Stream.of(ds.getDeviceAddr())
                .filter(a -> !a.getAddress().isLoopbackAddress() || clientAddress.isLoopbackAddress())
                .filter(a -> !a.getAddress().isLinkLocalAddress() || clientAddress.isLinkLocalAddress())
                .filter(a -> !a.getAddress().isSiteLocalAddress() || clientAddress.isSiteLocalAddress())
                // due to bug in linux kernel we need to filter out IPv6 addresses if client connected
                // with IPv4.
                // REVISIT: remove this workaround as soon as RHEL 7.5 is released.
                .filter(a -> clientAddress.getAddress().length >= a.getAddress().getAddress().length)
                .toArray(size -> new InetSocketAddress[size]);

        return layoutDriver.getDeviceAddress(usableAddresses);
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
    public Layout layoutGet(CompoundContext context, Inode nfsInode, layouttype4 layoutType, int ioMode, stateid4 stateid)
            throws IOException {

        LayoutDriver layoutDriver = getLayoutDriver(layoutType);

        NFS4Client client = null;
        try {

            FsInode inode = _chimeraVfs.inodeFromBytes(nfsInode.getFileId());
            PnfsId pnfsId = new PnfsId(inode.getId());
            Transfer.initSession(false, false);
            NDC.push(pnfsId.toString());
            NDC.push(context.getRpcCall().getTransport().getRemoteSocketAddress().toString());

            deviceid4[] devices;

            if (context.getMinorversion() == 0) {
                /* if we need to run proxy-io with NFSv4.0 */
                client = context.getStateHandler().getClientIdByStateId(stateid);
            } else {
                client = context.getSession().getClient();
            }

            final NFS4State openStateId = client.state(stateid).getOpenState();
            final NFS4State layoutStateId;

            // serialize all requests by the same stateid
            synchronized(openStateId) {

                if (inode.type() != FsInodeType.INODE || inode.getLevel() != 0) {
                    /*
                     * all non regular files ( AKA pnfs dot files ) provided by door itself.
                     */
                    throw new LayoutUnavailableException("special DOT file");
                }

                final InetSocketAddress remote = context.getRpcCall().getTransport().getRemoteSocketAddress();
                final NFS4ProtocolInfo protocolInfo = new NFS4ProtocolInfo(remote,
                            new org.dcache.chimera.nfs.v4.xdr.stateid4(stateid),
                            nfsInode.toNfsHandle()
                        );

                NfsTransfer transfer = _ioMessages.get(openStateId.stateid());
                if (transfer == null) {
                    transfer = new NfsTransfer(_pnfsHandler, client, openStateId, nfsInode,
                            context.getRpcCall().getCredential().getSubject(), ioMode);

                    transfer.setProtocolInfo(protocolInfo);
                    transfer.setCellAddress(getCellAddress());
                    transfer.setBillingStub(_billingStub);
                    transfer.setPoolStub(_poolStub);
                    transfer.setPoolManagerStub(_poolManagerStub);
                    transfer.setPnfsId(pnfsId);
                    transfer.setClientAddress(remote);
                    transfer.setIoQueue(_ioQueue);
                    transfer.setKafkaSender(_kafkaSender);

                    /*
                     * As all our layouts marked 'return-on-close', stop mover when
                     * open-state disposed on CLOSE.
                     */
                    final NfsTransfer t = transfer;
                    openStateId.addDisposeListener(state -> {
                        /*
                         * Cleanup transfer when state invalidated.
                         */
                        t.shutdownMover();
                        if (t.isWrite()) {
                            /* write request keep in the message map to
                             * avoid re-creates and trigger errors.
                             */
                            _ioMessages.remove(openStateId.stateid());
                        }
                    });

                     _ioMessages.put(openStateId.stateid(), transfer);
                }

                layoutStateId = transfer.getStateid();

                devices = transfer.getPoolDataServers(NFS_REQUEST_BLOCKING);
            }

            //  -1 is special value, which means entire file
            layout4 layout = new layout4();
            layout.lo_iomode = ioMode;
            layout.lo_offset = new offset4(0);
            layout.lo_length = new length4(nfs4_prot.NFS4_UINT64_MAX);
            layout.lo_content = layoutDriver.getLayoutContent(stateid, NFSv4Defaults.NFS4_STRIPE_SIZE, new nfs_fh4(nfsInode.toNfsHandle()), devices);

            layoutStateId.bumpSeqid();
            if (ioMode == layoutiomode4.LAYOUTIOMODE4_RW) {
                // in case of WRITE, invalidate vfs cache on close
                layoutStateId.addDisposeListener(state -> {
                    _vfs.invalidateStatCache(nfsInode);
                });
            }

            return new Layout(true, layoutStateId.stateid(), new layout4[]{layout});

        } catch (FileNotFoundCacheException e) {
            /*
             * The file is removed before we was able to start a mover.
             * Invalidate state as client will not send CLOSE for a stale file
             * handle.
             *
             * NOTICE: according POSIX, the opened file must be still accessible
             * after remove as long as it not closed. We violate that requirement
             * in favor of dCache shared state simplicity.
             */
            Objects.requireNonNull(client).releaseState(stateid);
            throw new StaleException("File is removed", e);
        } catch (CacheException | ChimeraFsException | TimeoutException | ExecutionException e) {
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
    public void layoutReturn(CompoundContext context, stateid4 stateid, layouttype4 layoutType, byte[] body) throws IOException {

        final NFS4Client client;
        if (context.getMinorversion() > 0) {
            client = context.getSession().getClient();
        } else {
            // v4.0 client use proxy adapter, which calls layoutreturn
            client = context.getStateHandler().getClientIdByStateId(stateid);
        }

        final NFS4State layoutState = client.state(stateid);
        final NFS4State openState = layoutState.getOpenState();

        _log.debug("Releasing layout by stateid: {}, open-state: {}", stateid,
                openState.stateid());

        NfsTransfer transfer = _ioMessages.get(openState.stateid());
        if (transfer != null) {
            transfer.shutdownMover();
        }
        // any further use of this layout-stateid must fail with NFS4ERR_BAD_STATEID
        client.releaseState(stateid);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.nfsv4.NFSv41DeviceManager#getLayoutTypes()
     */
    @Override
    public Set<layouttype4> getLayoutTypes() {
        return _supportedDrivers.keySet();
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
            pw.printf("  Supported Layout types  : %s\n", _supportedDrivers.keySet());
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
        public String call() {
            PoolMoverKillMessage message = new PoolMoverKillMessage(pool, mover,
                    "killed by door 'kill mover' command");
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

            NFS4Client client = _nfs4.getStateHandler().getClient(new clientid4(clientid));
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
                        for (NFSv41Session session : c.sessions()) {
                            sb.append("        ")
                                    .append(session)
                                    .append(" max slot: ")
                                    .append(session.getHighestSlot())
                                    .append("/")
                                    .append(session.getHighestUsedSlot())
                                    .append("\n");
                        }
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

        @Option(name = "pool", usage = "An optional pool for filtering. Glob pattern matching is supported.")
        Glob pool;

        @Option(name = "client", usage = "An optional client for filtering. Glob pattern matching is supported.")
        Glob client;

        @Option(name = "pnfsid", usage = "An optional pNFS ID for filtering. Glob pattern matching is supported.")
        Glob pnfsid;

        @Override
        public String call() throws IOException {

            return _ioMessages.values()
                    .stream()
                    .filter(d -> pool == null ? true : pool.matches(d.getPool().getName()))
                    .filter(d -> client == null ? true : client.matches(d.getClient().toString()))
                    .filter(d -> pnfsid == null ? true : pnfsid.matches(d.getPnfsId().toString()))
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
        private final long _verifier;

        public PoolDS(deviceid4 deviceId, InetSocketAddress[] ip, long verifier) {
            _deviceId = deviceId;
            _socketAddress = ip;
            _verifier = verifier;
        }

        public deviceid4 getDeviceId() {
            return _deviceId;
        }

        public InetSocketAddress[] getDeviceAddr() {
            return _socketAddress;
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

    private class NfsTransfer extends RedirectedTransfer<PoolDS> {

        private final Inode _nfsInode;
        private final NFS4State _stateid;
        private final NFS4State _openStateid;
        private ListenableFuture<Void> _redirectFuture;
        private AtomicReference<ChimeraNFSException> _errorHolder = new AtomicReference<>();
        private final NFS4Client _client;
        private final int _ioMode;

        NfsTransfer(PnfsHandler pnfs, NFS4Client client, NFS4State openStateId, Inode nfsInode, Subject ioSubject, int ioMode)
                throws ChimeraNFSException {
            super(pnfs, Subjects.ROOT, Restrictions.none(), ioSubject,  FsPath.ROOT);

            _nfsInode = nfsInode;

            // layout, or a transfer in dCache language, must have a unique stateid
            _stateid = client.createState(openStateId.getStateOwner(), openStateId);
            _openStateid = openStateId;
            _client = client;
            _ioMode = ioMode;
        }

        @Override
        public String toString() {

            ZoneId timeZone = ZoneId.systemDefault();

            String status = getStatus();
            if (status == null) {
                status = "idle";
            }

            return String.format("    %s : %s : %s %s@%s, OS=%s, cl=[%s], status=[%s]",
                    DateTimeFormatter.ISO_OFFSET_DATE_TIME
                            .format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(getCreationTime()), timeZone)),
                    getPnfsId(),
                    isWrite() ? "WRITE" : "READ",
                    getMoverId(),
                    getPool(),
                    ((NFS4ProtocolInfo)getProtocolInfoForPool()).stateId(),
                    ((NFS4ProtocolInfo)getProtocolInfoForPool()).getSocketAddress().getAddress().getHostAddress(),
                    status);
        }

        Inode getInode() {
            return _nfsInode;
        }

        /**
         * Returns an array of pNFS devices to be used for this transfer.
         * If array have more than one element, then mirror IO is desired.
         *
         * @param timeout time in milliseconds to block before error is returned.
         * @return an array of pNFS devices.
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws TimeoutException
         * @throws CacheException
         * @throws ChimeraNFSException
         */
        @GuardedBy("nfsState")
        deviceid4[] getPoolDataServers(long timeout) throws
                InterruptedException, ExecutionException,
                TimeoutException, CacheException, ChimeraNFSException {

            ChimeraNFSException error = _errorHolder.get();
            if (error != null) {
                throw  error;
            }

            if (_redirectFuture == null) {

                readNameSpaceEntry(_ioMode == layoutiomode4.LAYOUTIOMODE4_RW);

                FileAttributes attr = getFileAttributes();

                /*
                 * allow writes only into new files
                 */
                if ((_ioMode == layoutiomode4.LAYOUTIOMODE4_RW) && !attr.getStorageInfo().isCreatedOnly()) {
                    throw new PermException("Can't modify existing file");
                }

                if (!isWrite() && attr.getLocations().isEmpty()
                        && !attr.getStorageInfo().isStored()) {
                    throw new NfsIoException("lost file " + getPnfsId());
                }

                /*
                 * We start new request with an assumption, that file is available
                 * and can be directly accessed by the client, e.q. no stage
                 * or p2p is required.
                 */
                setOnlineFilesOnly(true);
                // REVISIT: this have to go into Transfer class.
                if (isWrite() && getFileAttributes().isDefined(FileAttribute.LOCATIONS) && !getFileAttributes().getLocations().isEmpty()) {

                    /*
                     * If we need to start a write-mover for a file which already has
                     * a location assigned to it, then we stick that location and by-pass
                     * any pool selection step.
                     */

                    Collection<String> locations = getFileAttributes().getLocations();
                    if (locations.size() > 1) {
                        /*
                         * Huh! We don't support mirroring (yet), thus there
                         * can't by multiple locations, unless some something
                         * went wrong!
                         */
                        throw new ServerFaultException("multiple locations for: " + getPnfsId() + " : " + locations);
                    }
                    String location = locations.iterator().next();
                    _log.debug("Using pre-existing WRITE pool {} for {}", location, getPnfsId());
                    // REVISIT: here we knoe that pool name and address are the same thing
                    setPool(new Pool(location, new CellAddressCore(location), Assumptions.none()));
                    _redirectFuture = startMoverAsync(STAGE_REQUEST_TIMEOUT);
                } else {
                    _log.debug("looking a {} pool for {}", (isWrite() ? "WRITE" : "READ"), getPnfsId());
                    _redirectFuture = selectPoolAndStartMoverAsync(POOL_SELECTION_RETRY_POLICY);
                }
            }

            /*
             * If we wait for offline file, then there is no need to block.
             * The async reply will update _redirectFuture when it's timed out or
             * ready.
             */
            if (!isWrite() && !getOnlineFilesOnly() && !_redirectFuture.isDone()) {
                throw new LayoutTryLaterException("Wating for file to become online.");
            }

            try {
                _redirectFuture.get(NFS_REQUEST_BLOCKING, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {

                /*
                 * PERMISSION_DENIED on read indicates that pool manager was not allowed to run p2p or stage.
                 */

                 // No fancy things on write.
                if(isWrite()) {
                    throw e;
                }

                Throwable t = e.getCause();
                if (!(t instanceof CacheException)) {
                    throw e;
                }

                CacheException ce = (CacheException) t;
                if (ce.getRc() != CacheException.PERMISSION_DENIED) {
                    throw e;
                }

                // kick stage/ p2p
                setOnlineFilesOnly(false);
                _redirectFuture = selectPoolAndStartMoverAsync(POOL_SELECTION_RETRY_POLICY);
                throw new LayoutTryLaterException("File is not online: stage or p2p required");
            }
            _log.debug("mover ready: pool={} moverid={}", getPool(), getMoverId());

            deviceid4 ds = waitForRedirect(NFS_REQUEST_BLOCKING).getDeviceId();
            return new deviceid4[] {ds};
        }

        /**
         * Retry transfer.
         */
        private String retry() {

            /*
             * client re-try will trigger transfer
             */
            if (_redirectFuture == null) {
                return "Nothing to do.";
            }

            /*
             * The transfer is in the middle of an action
             */
            String s = getStatus();
            if (s != null) {
                return "Can't reset transfer in action: " + s;
            }

            /*
             * Reply from pool selection is lost. It's safe to start over.
             */
            if (getPool() == null) {
                _redirectFuture = null;
                return "Restarting from pool selection";
            }

            /*
             * Mover id is lost. it's ok to start it again, as pool will start
             * mover for given transfer only once.
             */
            if (!hasMover()) {
                _redirectFuture = startMoverAsync(NFS_REQUEST_BLOCKING);
                return "Re-activating mover on: " + getPool();
            }

            /*
             * Redirect is complete.
             */
            if (getRedirect() != null) {
                return "Can't re-try complete mover.";
            }

            /**
             * Redirect is lost
             */
            // currently there are no possiblitilies to force too to re-send redirect.

            return "Lost redirect...";
        }

        public synchronized void shutdownMover() throws NfsIoException, DelayException {

            if (!hasMover()) {
                // the mover clean-up will not be called, thus we have to clean manually
                _ioMessages.remove(_openStateid.stateid());
                return;
            }

            _log.debug("Shuting down transfer: {}", this);
            killMover(0, "killed by door: returning layout");

            try {
                if (!waitForMover(NFS_REQUEST_BLOCKING)) {
                    throw new DelayException("Mover not stopped");
                }
            } catch (FileNotFoundCacheException e) {
                // REVISIT: remove when pool will stop sending this exception
                _log.info("File removed while being opened: {}@{} : {}",
                        getMoverId(), getPool(), e.getMessage());
            } catch (CacheException | InterruptedException e) {
                _log.info("Failed to kill mover: {}@{} : {}",
                        getMoverId(), getPool(), e.getMessage());
                throw new NfsIoException(e.getMessage(), e);
            }
        }

        /**
         * Set a {@link ChimeraNFSException} which will be thrown when
         * {@link #getPoolDataServer(long)} is called. An existing object will
         * not be replaced. This method is used to enforce error condition on an
         * attempt to select a pool.
         * @param e exception to be thrown.
         * @return {@code true} if new exception is set.
         */
        boolean enforceErrorIfRunning(ChimeraNFSException e) {
            return _errorHolder.compareAndSet(null, e);
        }

        NFS4State getStateid() {
            return _stateid;
        }

        NFS4Client getClient() {
            return _client;
        }

        /**
         * Recall file layout from the client. As re-calling is an async action.
         * call executed in dedicated {@code executorService}. If client can't return
         * layout right away, then recall will we re-scheduled.
         *
         * @param executorService executor service to use.
         */
        void recallLayout(ScheduledExecutorService executorService) {

            if (getRedirect() == null) {
                // client don't have a layout at all
                return;
            }

            ChimeraNFSException e = isWrite() ? POISON : DELAY;
            if (!enforceErrorIfRunning(e)) {
                // alredy recalled
                return;
            }

            if (_client.getCB() == null) {
                _log.info("Can't recall layout - no callback channel");
                return;
            }

            // bump sequence as we do a new action on layout
            _stateid.bumpSeqid();

            _log.info("Recalling layout from {}", _client);
            executorService.submit(new FireAndForgetTask(new LayoutRecallTask(this, executorService)));
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
        public Serializable call() {
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
        public Serializable call() {

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

    @Command(name = "recall layout", hint = "recall all layouts for given pool.")
    public class PoolDisableCmd implements Callable<String> {

        @Argument(metaVar = "pool")
        String pool;

        @Override
        public String call() {
            long n = recallLayouts(pool);
            return n + " layouts scheduled for recall.";
        }
    }

    @Command(name = "transfer retry", hint = "retry transfer for given open state.",
        description = "Retry pool selection or mover creation for a given transfer. "
                + "this can be necessary if components involved in selection "
                + "process were restarted before a reply was deliverd to the door.")
    public class TransferRetryCmd implements Callable<String> {

        @Argument(metaVar = "stateid")
        String os;

        @Override
        public String call() {
            stateid4 stateid = new stateid4(BaseEncoding.base16().lowerCase().decode(os), 0);
            NfsTransfer t = _ioMessages.get(stateid);
            if (t == null) {
                return "No matching transfer";
            }
            return t.retry();
        }
    }

    @Command(name = "transfer forget", hint = "remove transfer for a given open state.",
        description = "Remove transfer from the list of active transfers. If client retry the"
            + "request, then a new transfer will be created.")
    public class TransferForgetCmd implements Callable<String> {

        @Argument(metaVar = "stateid", usage = "nfs open state id assosiated with the transfer.")
        String os;

        @Option(name = "kill-mover", usage = "try to kill mover, if exists.")
        boolean killMover;

        @Override
        public String call() {
            stateid4 stateid = new stateid4(BaseEncoding.base16().lowerCase().decode(os), 0);
            NfsTransfer t = _ioMessages.remove(stateid);
            if (t == null) {
                return "No matching transfer";
            }
            if (killMover) {
                t.killMover(0, TimeUnit.SECONDS, "manual transfer termination");
            }
            return "Removed: " + t;
        }
    }

    /**
     * Handle pool disable.
     * @param pool name of the disabled pool.
     * @return number of affected transfers.
     */
    private synchronized long recallLayouts(String pool) {

        return _ioMessages.values().stream()
                .filter(t -> t.getPool() != null)
                .filter(t -> pool.equals(t.getPool().getName()))
                .filter(t -> t.getClient().getMinorVersion() > 0)
                .peek(t -> {
                    t.recallLayout(_callbackExecutor);
                })
                .count();
    }

    private void updateLbPaths() {
        List<String> exportPaths = _exportFile.getExports().map(FsExport::getPath).distinct().collect(toList());
        _loginBrokerPublisher.setReadPaths(exportPaths);
        _loginBrokerPublisher.setWritePaths(exportPaths);
    }

    private LayoutDriver getLayoutDriver(layouttype4 layoutType) throws BadLayoutException, UnknownLayoutTypeException {

        LayoutDriver layoutDriver = _supportedDrivers.get(layoutType);
        if (layoutDriver == null) {
            throw new UnknownLayoutTypeException("Layout type (" + layoutType + ") not supported");
        }
        return layoutDriver;
    }

    private static class LayoutRecallTask implements Runnable {

        private final NfsTransfer transfer;
        private final ScheduledExecutorService executorService;

        public LayoutRecallTask(NfsTransfer transfer, ScheduledExecutorService executorService) {
            this.transfer = transfer;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            try {
                transfer.getClient().getCB().cbLayoutRecallFile(new nfs_fh4(transfer.getInode().toNfsHandle()), transfer.getStateid().stateid());
            } catch (NoMatchingLayoutException e) {
                /**
                 * In case of "forgetful client model", nfs client will return
                 * NFS4ERR_NOMATCHING_LAYOUT for existing layout. We can simply
                 * kill the transfer, as client will create a new one.
                 */
                _log.debug("forgetful client model");
                transfer.killMover(0, "Pool disabled");
            } catch (DelayException e) {
                // probably we hit in the middle of IO, try again
                _log.debug("Client can't return layout: re-scheduling layout recall");
                executorService.schedule(this, 2, TimeUnit.SECONDS);
            } catch (IOException e) {
                _log.error("Failed to send call-back to the client: {}", e.getMessage());
            }
        }
    }
}
