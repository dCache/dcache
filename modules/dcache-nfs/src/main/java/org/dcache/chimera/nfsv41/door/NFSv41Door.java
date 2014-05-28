/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.glassfish.grizzly.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
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
import dmg.cells.services.login.LoginBrokerHandler;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.nfsv41.door.proxy.DcapProxyIoFactory;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoMdsOpFactory;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.commons.util.NDC;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.ExportFile;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.NFSv41Session;
import org.dcache.nfs.v4.RoundRobinStripingPattern;
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
import org.dcache.nfs.vfs.ChimeraVfs;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VfsCache;
import org.dcache.nfs.vfs.VfsCacheConfig;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.util.Args;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.utils.Bytes;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.gss.GssSessionManager;

public class NFSv41Door extends AbstractCellComponent implements
        NFSv41DeviceManager, CellCommandListener,
        CellMessageReceiver, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(NFSv41Door.class);

    /**
     * A mapping between pool name, nfs device id and pool's ip addresses.
     */
    private final PoolDeviceMap _poolDeviceMap = new PoolDeviceMap();

    /** next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);
    /*
     * reserved device for IO through MDS (for pnfs dot files)
     */
    private static final deviceid4 MDS_ID = deviceidOf(0);

    private final Map<stateid4, NfsTransfer> _ioMessages = new ConcurrentHashMap<>();

    /**
     * The usual timeout for NFS operations is 30s. Nevertheless, as client
     * will block, we try to block as short as we can. The rule for interactive users:
     * never block longer than 10s.
     */
    private final static long NFS_REPLY_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    /**
     * Given that the timeout is pretty short, the retry period has to
     * be rather small too.
     */
    private final static long NFS_RETRY_PERIOD = 500; // In millis

    /**
     * Cell communication helper.
     */
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private String _cellName;
    private String _domainName;
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

    private final static String V3 = "3";
    private final static String V41 = "4.1";

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

    private VirtualFileSystem _vfs;

    private LoginBrokerHandler _loginBrokerHandler;

    private DcapProxyIoFactory _proxyIoFactory;

    private final static TransferRetryPolicy RETRY_POLICY =
        new TransferRetryPolicy(Integer.MAX_VALUE, NFS_RETRY_PERIOD,
                                NFS_REPLY_TIMEOUT, NFS_REPLY_TIMEOUT);

    /**
     * Data striping pattern for a file.
     */
    private final StripingPattern<InetSocketAddress[]> _stripingPattern =
            new RoundRobinStripingPattern<>();

    private VfsCacheConfig _vfsCacheConfig;

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
    public void setLoginBrokerHandler(LoginBrokerHandler loginBrokerHandler) {
        _loginBrokerHandler = loginBrokerHandler;
    }

    @Required
    public void setVfsCacheConfig(VfsCacheConfig vfsCacheConfig) {
        _vfsCacheConfig = vfsCacheConfig;
    }

    public void init() throws Exception {

        _cellName = getCellName();
        _domainName = getCellDomainName();

        _rpcService = new OncRpcSvcBuilder()
                .withPort(_port)
                .withTCP()
                .withAutoPublish()
                .build();
        if (_enableRpcsecGss) {
            _rpcService.setGssSessionManager(new GssSessionManager(_idMapper));
        }

        _vfs = new VfsCache(new ChimeraVfs(_fileFileSystemProvider, _idMapper), _vfsCacheConfig);

        MountServer ms = new MountServer(_exportFile, _vfs);
        _rpcService.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);
        _rpcService.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);

        for (String version : _versions) {
            switch (version) {
                case V3:
                    NfsServerV3 nfs3 = new NfsServerV3(_exportFile, _vfs);
                    _rpcService.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
                    break;
                case V41:
                     final NFSv41DeviceManager _dm = this;
                     _proxyIoFactory = new DcapProxyIoFactory(getCellAddress().getCellName() + "-dcap-proxy", "");
                     _proxyIoFactory.setBillingStub(_billingStub);
                     _proxyIoFactory.setFileSystemProvider(_fileFileSystemProvider);
                     _proxyIoFactory.setPnfsHandler(_pnfsHandler);
                     _proxyIoFactory.setPoolManager(_poolManagerStub.getDestinationPath());
                     _proxyIoFactory.setIoQueue(_ioQueue);
                     _proxyIoFactory.setRetryPolicy(RETRY_POLICY);
                     _proxyIoFactory.startAdapter();
                    _nfs4 = new NFSServerV41(new ProxyIoMdsOpFactory(_proxyIoFactory, new MDSOperationFactory()),
                            _dm, _vfs, _idMapper, _exportFile);
                    _rpcService.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _nfs4);
                    _loginBrokerHandler.start();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported NFS version: " + version);
            }
        }


        _rpcService.start();

    }

    public void destroy() throws IOException {
        _loginBrokerHandler.stop();
        _rpcService.stop();
        if(_proxyIoFactory != null) {
            _proxyIoFactory.cleanUp();
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

        _log.debug("NFS mover ready: {}", poolName);

        InetSocketAddress[] poolAddress = message.socketAddresses();
        PoolDS device = _poolDeviceMap.getByPoolName(poolName);

        if (device == null || isPoolRestarted(device, message)) {
            /* pool is unknown yet or has been restarted so create new device and device-id */
            final int id = this.nextDeviceID();
            final deviceid4 deviceid = deviceidOf(id);
            final PoolDS newDevice = new PoolDS(deviceid, _stripingPattern, poolAddress, message.getVerifier());

            _log.debug("new mapping: {}", newDevice);
            _poolDeviceMap.add(poolName, newDevice);
            device = newDevice;
        }

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

    private boolean isPoolRestarted(PoolDS ds, PoolPassiveIoFileMessage<org.dcache.chimera.nfs.v4.xdr.stateid4> message) {
        long verifier = message.getVerifier();
        if (verifier != 0) {
            // pool supports verifier
            return ds.getVerifier() != verifier;
        }
        // pre-2.9 pool
        return !Arrays.equals(ds.getInetSocketAddress(), message.socketAddresses());
    }

    public void messageArrived(DoorTransferFinishedMessage transferFinishedMessage) {

        NFS4ProtocolInfo protocolInfo = (NFS4ProtocolInfo)transferFinishedMessage.getProtocolInfo();
        _log.debug("Mover {} done.", protocolInfo.stateId());
        org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid = protocolInfo.stateId();
        Transfer transfer = _ioMessages.remove(new stateid4(legacyStateid.other, legacyStateid.seqid.value));
        if(transfer != null) {
                transfer.finished(transferFinishedMessage);
                transfer.notifyBilling(transferFinishedMessage.getReturnCode(), "");
        }
    }

    private int nextDeviceID() {
        return _nextDeviceID.incrementAndGet();
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
            return deviceAddrOf( new RoundRobinStripingPattern<InetSocketAddress[]>(),
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
    public Layout layoutGet(CompoundContext context, Inode nfsInode, int ioMode, stateid4 stateid)
            throws IOException {

        FsInode inode = _fileFileSystemProvider.inodeFromBytes(nfsInode.getFileId());
        CDC cdc = CDC.reset(_cellName, _domainName);
        try {
            NDC.push(inode.toString());
            NDC.push(context.getRpcCall().getTransport().getRemoteSocketAddress().toString());
            deviceid4 deviceid;
            if (inode.type() != FsInodeType.INODE || inode.getLevel() != 0) {
                /*
                 * all non regular files ( AKA pnfs dot files ) provided by door itself.
                 */
                deviceid = MDS_ID;
            } else {

                final InetSocketAddress remote = context.getRpcCall().getTransport().getRemoteSocketAddress();
                final PnfsId pnfsId = new PnfsId(inode.toString());
                final NFS4ProtocolInfo protocolInfo = new NFS4ProtocolInfo(remote, new org.dcache.chimera.nfs.v4.xdr.stateid4(stateid));

                Transfer.initSession();
                NfsTransfer transfer = _ioMessages.get(stateid);
                if (transfer == null) {
                    transfer = new NfsTransfer(_pnfsHandler,
                            context.getRpcCall().getCredential().getSubject());

                    transfer.setProtocolInfo(protocolInfo);
                    transfer.setCellName(this.getCellName());
                    transfer.setDomainName(this.getCellDomainName());
                    transfer.setBillingStub(_billingStub);
                    transfer.setPoolStub(_poolManagerStub);
                    transfer.setPoolManagerStub(_poolManagerStub);
                    transfer.setPnfsId(pnfsId);
                    transfer.setClientAddress(remote);
                    transfer.readNameSpaceEntry();

                    _ioMessages.put(stateid, transfer);

                    PoolDS ds = getPool(transfer, ioMode);
                    deviceid = ds.getDeviceId();
                } else {
                    PoolDS ds = transfer.waitForRedirect(NFS_RETRY_PERIOD);
                    deviceid = ds.getDeviceId();
                }
            }

            nfs_fh4 fh = new nfs_fh4(nfsInode.toNfsHandle());

            //  -1 is special value, which means entire file
            layout4 layout = Layout.getLayoutSegment(deviceid, fh, ioMode,
                    0, nfs4_prot.NFS4_UINT64_MAX);

            return new Layout(true, stateid, new layout4[]{layout});

        } catch (FileInCacheException e) {
	    cleanStateAndKillMover(stateid);
            throw new ChimeraNFSException(nfsstat.NFSERR_IO, e.getMessage());
        } catch (CacheException e) {
	   cleanStateAndKillMover(stateid);
            /*
             * error 243: file is broken on tape.
             * can't do a much. Tell it to client.
             */
            int status = e.getRc() == CacheException.BROKEN_ON_TAPE ? nfsstat.NFSERR_IO : nfsstat.NFSERR_LAYOUTTRYLATER;
            throw new ChimeraNFSException(status, e.getMessage());
        } catch (InterruptedException e) {
	    cleanStateAndKillMover(stateid);
            throw new ChimeraNFSException(nfsstat.NFSERR_LAYOUTTRYLATER,
                    e.getMessage());
        } finally {
            cdc.close();
        }

    }

    private void cleanStateAndKillMover(stateid4 stateid) {
        Transfer t = _ioMessages.remove(stateid);
        if (t != null) {
            t.killMover(0);
        }
    }

    private PoolDS getPool(NfsTransfer transfer, int iomode)
            throws InterruptedException, CacheException
    {


        if ((iomode == layoutiomode4.LAYOUTIOMODE4_READ) || !transfer.getStorageInfo().isCreatedOnly()) {
            _log.debug("looking for read pool for {}", transfer.getPnfsId());
            transfer.setWrite(false);
        } else {
            _log.debug("looking for write pool for {}", transfer.getPnfsId());
            transfer.setWrite(true);
        }
        transfer.selectPoolAndStartMover(_ioQueue, RETRY_POLICY);

        _log.debug("mover ready: pool={} moverid={}", transfer.getPool(),
                transfer.getMoverId());

        /*
         * FIXME;
         *
         * usually RPC request will timeout in 30s.
         * We have to handle this cases and return LAYOUTTRYLATER
         * or GRACE.
         *
         */
        return transfer.waitForRedirect(NFS_REPLY_TIMEOUT);
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
            if(!transfer.waitForMover(500)) {
                throw new ChimeraNFSException(nfsstat.NFSERR_DELAY, "Mover not stopped");
            }
        } catch (CacheException | InterruptedException e) {
            _log.info("Failed to kill mover: {}@{} : {}",
                    transfer.getMoverId(), transfer.getPool(), e.getMessage());
            throw new ChimeraNFSException(nfsstat.NFSERR_IO, e.getMessage());
        }
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
            pw.printf("  IO queue: %s\n", _ioQueue);
            pw.println("  Known pools (DS):\n");
            for (Map.Entry<String, PoolDS> ioDevice : _poolDeviceMap.getEntries()) {
                pw.println(String.format("    %s : [%s]", ioDevice.getKey(), ioDevice.getValue()));
            }

            pw.println();
            pw.println("  Known movers (layouts):");
            for (NfsTransfer io : _ioMessages.values()) {
                pw.println(io);
            }

            pw.println();
            pw.println("  Known clients:");
            for (NFS4Client client : _nfs4.getClients()) {
                pw.println(String.format("    %s", client));
                for (NFSv41Session session : client.sessions()) {
                    pw.println(String.format("        %s, max slot: %d/%d",
                            session, session.getHighestSlot(), session.getHighestUsedSlot()));

                }
            }
        }
    }

    public static final String hh_kill_mover = " <pool> <moverid> # kill mover on the pool";
    public String ac_kill_mover_$_2(Args args) throws Exception {
        int mover = Integer.parseInt(args.argv(1));
        String pool = args.argv(0);

        PoolMoverKillMessage message = new PoolMoverKillMessage(pool, mover);
        message.setReplyRequired(false);
        _poolStub.notify(new CellPath(pool), message);
        return "";
    }

    public static final String fh_exports_reload = " # re-scan export file";
    public String ac_exports_reload(Args args) throws IOException {
        _exportFile.rescan();
        return "Done";
    }
    public static final String fh_exports_ls = " [host] # dump nfs exports";
    public String ac_exports_ls_$_0_1(Args args) throws UnknownHostException {
        InetAddress address = InetAddress.getByName(args.argv(0));
        if (args.argc() > 0 ) {
            return Joiner.on('\n').join(_exportFile.exportsFor(address));
        } else {
            return Joiner.on('\n').join(_exportFile.getExports());
        }
    }

    private static deviceid4 deviceidOf(int id) {
        byte[] deviceidBytes = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];
        Bytes.putInt(deviceidBytes, 0, id);

        return new deviceid4(deviceidBytes);
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

        NfsTransfer(PnfsHandler pnfs, Subject ioSubject) {
            super(pnfs, Subjects.ROOT, ioSubject,  new FsPath("/"));
        }

        @Override
        public String toString() {
            return String.format("    %s : %s@%s, OS=%s,cl=[%s]",
                    getPnfsId(),
                    getMoverId(),
                    getPool(),
                    ((NFS4ProtocolInfo)getProtocolInfoForPool()).stateId(),
                    ((NFS4ProtocolInfo)getProtocolInfoForPool()).getSocketAddress().getAddress().getHostAddress());
        }
    }

    /**
     * To allow the transfer monitoring in the httpd cell to recognize us
     * as a door, we have to emulate LoginManager.  To emulate
     * LoginManager we list ourselves as our child.
     */
    public final static String hh_get_children = "[-binary]";

    public Object ac_get_children(Args args) {
        boolean binary = args.hasOption("binary");
        if (binary) {
            String[] childrens = new String[]{this.getCellName()};
            return new LoginManagerChildrenInfo(this.getCellName(), this.getCellDomainName(), childrens);
        } else {
            return this.getCellName();
        }
    }
    public final static String hh_get_door_info = "[-binary]";
    public final static String fh_get_door_info =
            "Provides information about the door and current transfers";

    public Object ac_get_door_info(Args args) {
        List<IoDoorEntry> entries = new ArrayList<>();
        for (Transfer transfer : _ioMessages.values()) {
            entries.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(this.getCellName(), this.getCellDomainName());
        doorInfo.setProtocol("NFSV4.1", "0");
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(entries
                .toArray(new IoDoorEntry[entries.size()]));
        return args.hasOption("binary") ? doorInfo : doorInfo.toString();
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

    public final static String fh_stats =
            "stats [-c] # show nfs requests statstics\n\n" +
            "  Print nfs operation statistics.\n" +
            "    -c clear current statistics values";
    public final static String hh_stats = " [-c] # show nfs requests statstics";
    public String ac_stats(Args args) {
        RequestExecutionTimeGauges<String> gauges = _nfs4.getStatistics();
        StringBuilder sb = new StringBuilder();
        sb.append("Stats:").append("\n").append(gauges);

        if (args.hasOption("c")) {
            gauges.reset();
        }
        return sb.toString();
    }
}
