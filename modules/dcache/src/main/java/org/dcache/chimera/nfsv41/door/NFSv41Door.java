/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.glassfish.grizzly.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import dmg.util.Args;
import java.util.Set;

import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.NfsServerV3;
import org.dcache.chimera.nfs.v3.xdr.mount_prot;
import org.dcache.chimera.nfs.v3.xdr.nfs3_prot;
import org.dcache.chimera.nfs.v4.CompoundContext;
import org.dcache.chimera.nfs.v4.Layout;
import org.dcache.chimera.nfs.v4.MDSOperationFactory;
import org.dcache.chimera.nfs.v4.NFS4Client;
import org.dcache.chimera.nfs.v4.NFSServerV41;
import org.dcache.chimera.nfs.v4.NFSv41DeviceManager;
import org.dcache.chimera.nfs.v4.NFSv41Session;
import org.dcache.chimera.nfs.v4.RoundRobinStripingPattern;
import org.dcache.chimera.nfs.v4.StripingPattern;
import org.dcache.chimera.nfs.v4.xdr.device_addr4;
import org.dcache.chimera.nfs.v4.xdr.deviceid4;
import org.dcache.chimera.nfs.v4.xdr.layout4;
import org.dcache.chimera.nfs.v4.xdr.layoutiomode4;
import org.dcache.chimera.nfs.v4.xdr.layouttype4;
import org.dcache.chimera.nfs.v4.xdr.multipath_list4;
import org.dcache.chimera.nfs.v4.xdr.netaddr4;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_fh4;
import org.dcache.chimera.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.vfs.ChimeraVfs;
import org.dcache.chimera.nfs.vfs.Inode;
import org.dcache.chimera.nfs.vfs.VirtualFileSystem;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;
import org.dcache.commons.util.NDC;
import org.dcache.util.LoginBrokerHandler;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.utils.Bytes;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.gss.GssSessionManager;
import org.springframework.beans.factory.annotation.Required;

public class NFSv41Door extends AbstractCellComponent implements
        NFSv41DeviceManager, CellCommandListener,
        CellMessageReceiver, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(NFSv41Door.class);

    /** dCache-friendly NFS device id to pool name mapping */
    private Map<String, PoolDS> _poolNameToIpMap = new HashMap<>();

    /** All known devices */
    private Map<deviceid4, PoolDS> _deviceMap = new HashMap<>();

    /** next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);
    /*
     * reserved device for IO through MDS (for pnfs dot files)
     */
    private static final deviceid4 MDS_ID = deviceidOf(0);

    private final Map<stateid4, NfsTransfer> _ioMessages = new ConcurrentHashMap<>();

    /**
     * The usual timeout for NFS ops. is 30s.
     * We will use a bit shorter (27s) one to avoid retries.
     */
    private final static long NFS_REPLY_TIMEOUT = TimeUnit.SECONDS.toMillis(27);

    /**
     * Given that the timeout is pretty short, the retry period has to
     * be rather small too.
     */
    private final static long NFS_RETRY_PERIOD = TimeUnit.SECONDS.toMillis(1);

    /**
     * Cell communication helper.
     */
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

    private final static TransferRetryPolicy RETRY_POLICY =
        new TransferRetryPolicy(Integer.MAX_VALUE, NFS_RETRY_PERIOD,
                                NFS_REPLY_TIMEOUT, NFS_REPLY_TIMEOUT);

    public void setEnableRpcsecGss(boolean enable) {
        _enableRpcsecGss = enable;
    }

    public void setIdMapper(StrategyIdMapper idMapper)    {
        _idMapper = idMapper;
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

    public void init() throws Exception {

        _cellName = getCellName();
        _domainName = getCellDomainName();

        _rpcService = new OncRpcSvc(_port, IpProtocolType.TCP, true);
        if (_enableRpcsecGss) {
            _rpcService.setGssSessionManager(new GssSessionManager(_idMapper));
        }

        _vfs = new ChimeraVfs(_fileFileSystemProvider, _idMapper);

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
                    _nfs4 = new NFSServerV41(new MDSOperationFactory(),
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
    }

    /*
     * Handle reply from the pool that mover actually started.
     *
     * If the pools is not know yet, create a mapping between pool name
     * and NFSv4.1 device id. Finally, notify waiting request that we have got
     * the reply for LAYOUTGET
     */
    public void messageArrived(PoolPassiveIoFileMessage<stateid4> message) {

        String poolName = message.getPoolName();

        _log.debug("NFS mover ready: {}", poolName);

        InetSocketAddress[] poolAddress = message.socketAddresses();
        PoolDS device = _poolNameToIpMap.get(poolName);

        if (device == null || !Arrays.equals(device.getInetSocketAddress(), poolAddress)) {
            /* pool is unknown yet or has been restarted so create new device and device-id */
            int id = this.nextDeviceID();

            if (device != null) {
                /*
                 * clean stale entry
                 */
                deviceid4 oldId = device.getDeviceId();
                _deviceMap.remove(oldId);
            }
            /*
             * TODO: the PoolPassiveIoFileMessage have to be adopted to send
             * list of all interfaces
             */
            deviceid4 deviceid = deviceidOf(id);
            device = new PoolDS(deviceid, poolAddress);

            _poolNameToIpMap.put(poolName, device);
            _deviceMap.put(deviceid, device);
            _log.debug("new mapping: {}", device);
        }

        stateid4 stateid = message.challange();

        NfsTransfer transfer = _ioMessages.get(stateid);
        transfer.redirect(device);
    }

    public void messageArrived(DoorTransferFinishedMessage transferFinishedMessage) {

        NFS4ProtocolInfo protocolInfo = (NFS4ProtocolInfo)transferFinishedMessage.getProtocolInfo();
        _log.debug("Mover {} done.", protocolInfo.stateId());
        Transfer transfer = _ioMessages.remove(protocolInfo.stateId());
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

        PoolDS ds = _deviceMap.get(deviceId);
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
        try(CDC cdc = CDC.reset(_cellName, _domainName)) {
            NDC.push("pnfsid=" + inode);
            NDC.push("client=" + context.getRpcCall().getTransport().getRemoteSocketAddress());
            deviceid4 deviceid;
            if (inode.type() != FsInodeType.INODE || inode.getLevel() != 0) {
                /*
                 * all non regular files ( AKA pnfs dot files ) provided by door itself.
                 */
                deviceid = MDS_ID;
            } else {
                InetSocketAddress remote = context.getRpcCall().getTransport().getRemoteSocketAddress();
                PnfsId pnfsId = new PnfsId(inode.toString());
                Transfer.initSession();
                NfsTransfer transfer = new NfsTransfer(_pnfsHandler, Subjects.ROOT,
                        context.getRpcCall().getCredential().getSubject(),
                        remote, stateid);

                NFS4ProtocolInfo protocolInfo = transfer.getProtocolInfoForPool();
                protocolInfo.door(new CellPath(getCellAddress()));

                transfer.setCellName(this.getCellName());
                transfer.setDomainName(this.getCellDomainName());
                transfer.setBillingStub(_billingStub);
                transfer.setPoolStub(_poolManagerStub);
                transfer.setPoolManagerStub(_poolManagerStub);
                transfer.setPnfsId(pnfsId);
                transfer.setClientAddress(remote);
                transfer.readNameSpaceEntry();

                _ioMessages.put(protocolInfo.stateId(), transfer);

                PoolDS ds = getPool(transfer, protocolInfo, ioMode);
                deviceid = ds.getDeviceId();
            }

            nfs_fh4 fh = new nfs_fh4(nfsInode.toNfsHandle());

            //  -1 is special value, which means entire file
            layout4 layout = Layout.getLayoutSegment(deviceid, fh, ioMode,
                    0, nfs4_prot.NFS4_UINT64_MAX);

            return new Layout(true, stateid, new layout4[]{layout});

        } catch (FileInCacheException e) {
            throw new ChimeraNFSException(nfsstat.NFSERR_IO, e.getMessage());
        } catch (InterruptedException | CacheException e) {
            throw new ChimeraNFSException(nfsstat.NFSERR_LAYOUTTRYLATER,
                    e.getMessage());
        }

    }

    private PoolDS getPool(NfsTransfer transfer, NFS4ProtocolInfo protocolInfo, int iomode)
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
        List<deviceid4> knownDevices = new ArrayList<>();

        knownDevices.addAll(_deviceMap.keySet());

        return knownDevices;
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
            for (Map.Entry<String, PoolDS> ioDevice : _poolNameToIpMap.entrySet()) {
                pw.println(String.format("    %s : [%s]", ioDevice.getKey(), ioDevice.getValue()));
            }

            pw.println();
            pw.println("  Known movers (layouts):");
            for (Transfer io : _ioMessages.values()) {
                pw.println(String.format("    %s : %s@%s", io.getPnfsId(), io.getMoverId(), io.getPool()));
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
        sendMessage(new CellMessage(new CellPath(pool), message));
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

    private static class PoolDS {

        private final deviceid4 _deviceId;
        private final InetSocketAddress[] _socketAddress;
        private final device_addr4 _deviceAddr;

        public PoolDS(deviceid4 deviceId, InetSocketAddress[] ip) {
            _deviceId = deviceId;
            _socketAddress = ip;
            _deviceAddr = deviceAddrOf(new RoundRobinStripingPattern<InetSocketAddress[]>(), ip);
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

        @Override
        public String toString() {
            return String.format("DS: %s, InetAddress: %s",
                    _deviceId, Arrays.toString(_socketAddress));
        }
    }

    private static class NfsTransfer extends RedirectedTransfer<PoolDS> {

        private final stateid4 _stateid;
        private final NFS4ProtocolInfo _protocolInfo;

        NfsTransfer(PnfsHandler pnfs, Subject namespaceSubject, Subject ioSubject, InetSocketAddress client,
                stateid4 stateid) {
            super(pnfs, namespaceSubject, ioSubject,  new FsPath("/"));
            _stateid = stateid;
            _protocolInfo = new NFS4ProtocolInfo(client, _stateid);
        }

        @Override
        protected NFS4ProtocolInfo getProtocolInfoForPoolManager() {
            return _protocolInfo;
        }

        @Override
        protected NFS4ProtocolInfo getProtocolInfoForPool() {
            return _protocolInfo;
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

    public String ac_stats(Args args) {
        StringBuilder sb = new StringBuilder();
        sb.append("Stats:").append("\n").append(_nfs4.getStatistics());

        return sb.toString();
    }
}
