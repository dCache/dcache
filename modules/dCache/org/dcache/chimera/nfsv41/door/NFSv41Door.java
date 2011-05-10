/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;

import org.dcache.xdr.OncRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v4.DeviceManager;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.cells.CellStub;
import org.dcache.chimera.nfs.v4.NFSServerV41;
import org.dcache.chimera.nfs.v4.NFS4Client;
import org.dcache.chimera.nfs.v4.NFSv41DeviceManager;
import org.dcache.chimera.nfs.v4.xdr.device_addr4;
import org.dcache.chimera.nfs.v4.xdr.layoutiomode4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import dmg.util.Args;
import java.nio.ByteBuffer;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellInfoProvider;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.xdr.mount_prot;
import org.dcache.chimera.nfs.v4.Layout;
import org.dcache.chimera.nfs.v4.MDSOperationFactory;
import org.dcache.chimera.nfs.v4.OperationFactoryMXBeanImpl;
import org.dcache.chimera.nfs.v4.RoundRobinStripingPattern;
import org.dcache.chimera.nfs.v4.xdr.deviceid4;
import org.dcache.chimera.nfs.v4.xdr.layout4;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_fh4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.utils.Bytes;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.portmap.OncRpcEmbeddedPortmap;

public class NFSv41Door extends AbstractCellComponent implements
        NFSv41DeviceManager, CellCommandListener,
        CellMessageReceiver, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(NFSv41Door.class);

    static final int DEFAULT_PORT = 2049;

    /** dCache-friendly NFS device id to pool name mapping */
    private Map<String, PoolDS> _poolNameToIpMap = new HashMap<String, PoolDS>();

    /** All known devices */
    private Map<deviceid4, PoolDS> _deviceMap = new HashMap<deviceid4, PoolDS>();

    /** next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);
    /*
     * reserved device for IO through MDS (for pnfs dot files)
     */
    private static final deviceid4 MDS_ID = deviceidOf(0);

    private final Map<stateid4, NfsTransfer> _ioMessages = new ConcurrentHashMap<stateid4, NfsTransfer>();

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

    private PnfsHandler _pnfsHandler;

    private String _ioQueue;
    /*
     * FIXME: The acl handler have to be initialize in spring xml file
     */
    private final AclHandler _aclHandler = org.dcache.chimera.posix.UnixPermissionHandler.getInstance();

    /**
     * embedded nfs server
     */
    private NFSServerV41 _nfs4;

    /**
     * RPC service
     */
    private  OncRpcSvc _rpcService;

    private final static TransferRetryPolicy RETRY_POLICY =
        new TransferRetryPolicy(Integer.MAX_VALUE, NFS_RETRY_PERIOD,
                                NFS_REPLY_TIMEOUT, NFS_REPLY_TIMEOUT);

    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    public void setPnfsHandler(PnfsHandler pnfs) {
        _pnfsHandler = pnfs;
    }

    private FileSystemProvider _fileFileSystemProvider;
    public void setFileSystemProvider(FileSystemProvider fs) {
        _fileFileSystemProvider = fs;
    }

    private ExportFile _exportFile;
    public void setExportFile(ExportFile export) {
        _exportFile = export;
    }

    public void setIoQueue(String ioQueue) {
        _ioQueue = ioQueue;
    }

    public void init() throws Exception {


        new OncRpcEmbeddedPortmap();

        final NFSv41DeviceManager _dm = this;

        _rpcService = new OncRpcSvc(DEFAULT_PORT, IpProtocolType.TCP, true, "NFSv41 door embedded server");

        _nfs4 = new NFSServerV41( new OperationFactoryMXBeanImpl( new MDSOperationFactory() , "door"),
                _dm, _aclHandler, _fileFileSystemProvider, _exportFile);
        MountServer ms = new MountServer(_exportFile, _fileFileSystemProvider);

        _rpcService.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _nfs4);
        _rpcService.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);
        _rpcService.start();

    }

    public void destroy() {
        _rpcService.stop();
    }

    /*
     * Handle reply from the pool that mover actually started.
     *
     * If the pools is not know yet, create a mapping between pool name
     * and NFSv4.1 device id. Finally, notify waiting request that we have got
     * the reply for LAYOUTGET
     */
    public void messageArrived(PoolPassiveIoFileMessage message) {

        String poolName = message.getPoolName();

        _log.debug("NFS mover ready: {}", poolName);

        InetSocketAddress poolAddress = message.socketAddress();
        PoolDS device = _poolNameToIpMap.get(poolName);

        try {
            if (device == null || !device.getInetSocketAddress().equals(poolAddress)) {
                /* pool is unknown yet or has been restarted so create new device and device-id */
                int id = this.nextDeviceID();

                if( device != null ) {
                    /*
                     * clean stale entry
                     */
                    deviceid4 oldId = device.getDeviceId();
                    _deviceMap.remove(oldId);
                }
                /*
                 * TODO: the PoolPassiveIoFileMessage have to be adopted to send list
                 * of all interfaces
                 */
                deviceid4 deviceid = deviceidOf(id);
                device = new PoolDS(deviceid, poolAddress);

                _poolNameToIpMap.put(poolName, device);
                _deviceMap.put(deviceid, device);
                _log.debug("new mapping: {}", device);
            }

            XdrDecodingStream xdr = new XdrBuffer(ByteBuffer.wrap(message.challange()));
            stateid4 stateid = new stateid4();

            xdr.beginDecoding();
            stateid.xdrDecode(xdr);
            xdr.endDecoding();

            NfsTransfer transfer = _ioMessages.get(stateid);
            transfer.setPool(poolName);
            transfer.redirect(device);

        } catch (UnknownHostException ex) {
            _log.error("Invald address returned by {} : {}", poolName, ex.getMessage() );
        } catch (OncRpcException ex) {
           // forced by interface
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            // forced by interface
            throw new RuntimeException(ex);
        }

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
    public device_addr4 getDeviceInfo(NFS4Client client, deviceid4 deviceId) {
        /* in case of MDS access we return the same interface which client already connected to */
        if (deviceId.equals(MDS_ID)) {
            return DeviceManager.deviceAddrOf( new RoundRobinStripingPattern<InetSocketAddress>(),
                    client.getLocalAddress());
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
    public Layout layoutGet(FsInode inode, int ioMode, NFS4Client client, stateid4 stateid)
            throws IOException {

        try {
            deviceid4 deviceid;
            if (inode.type() != FsInodeType.INODE) {
                /*
                 * all non regular files ( AKA pnfs dot files ) provided by door itself.
                 */
                deviceid = MDS_ID;
            } else {
                PnfsId pnfsId = new PnfsId(inode.toString());
                NfsTransfer transfer = new NfsTransfer(_pnfsHandler, Subjects.ROOT, new FsPath("/"),
                        client.getRemoteAddress(), stateid);

                NFS4ProtocolInfo protocolInfo = transfer.getProtocolInfoForPool();
                protocolInfo.door(new CellPath(this.getCellName(), this.getCellDomainName()));

                transfer.setCellName(this.getCellName());
                transfer.setDomainName(this.getCellDomainName());
                transfer.setBillingStub(_billingStub);
                transfer.setPoolStub(_poolManagerStub);
                transfer.setPoolManagerStub(_poolManagerStub);
                transfer.setPnfsId(pnfsId);
                transfer.setClientAddress(client.getRemoteAddress());
                transfer.readNameSpaceEntry();

                _ioMessages.put(protocolInfo.stateId(), transfer);

                PoolDS ds = getPool(transfer, protocolInfo, ioMode);
                deviceid = ds.getDeviceId();
            }

            nfs_fh4 fh = new nfs_fh4(inode.toFullString().getBytes());

            //  -1 is special value, which means entire file
            layout4 layout = Layout.getLayoutSegment(deviceid, fh, ioMode,
                    0, nfs4_prot.NFS4_UINT64_MAX);

            return new Layout(true, stateid, new layout4[]{layout});

        } catch (InterruptedException e) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER,
                    e.getMessage());
        } catch (CacheException ce) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER,
                    ce.getMessage());
        }

    }

    private PoolDS getPool(NfsTransfer transfer, NFS4ProtocolInfo protocolInfo, int iomode)
            throws InterruptedException, CacheException, IOException {


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
    public List<deviceid4> getDeviceList(NFS4Client client) {
        List<deviceid4> knownDevices = new ArrayList<deviceid4>();

        knownDevices.addAll(_deviceMap.keySet());

        return knownDevices;
    }


    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#releaseDevice(stateid4 stateid)
     */
    @Override
    public void  layoutReturn(NFS4Client client, stateid4 stateid) {

        _log.debug("Releasing device by stateid: {}", stateid);

        Transfer transfer = _ioMessages.get(stateid);
        if (transfer != null) {

            _log.debug("Sending KILL to {}@{}", transfer.getMoverId(),
                transfer.getPool() );

            transfer.killMover(5000);
        }else{
            _log.warn("Can't find mover by stateid: {}", stateid);
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

        pw.println("NFSv4.1 door (MDS):");
        pw.printf("  IO queue: %s\n", _ioQueue);
        pw.println( String.format("  Concurrent Thread number : %d", _rpcService.getThreadCount() ));
        pw.println("  Known pools (DS):\n");
        for(Map.Entry<String, PoolDS> ioDevice: _poolNameToIpMap.entrySet()) {
            pw.println( String.format("    %s : %s", ioDevice.getKey(),ioDevice.getValue() ));
        }

        pw.println();
        pw.println("  Known movers (layouts):");
        for(Transfer io: _ioMessages.values()) {
            pw.println( String.format("    %s : %s@%s", io.getPnfsId(), io.getMoverId(), io.getPool() ));
        }

        pw.println();
        pw.println("  Known clients:");
        for (NFS4Client client : _nfs4.getClients()) {
            pw.println( String.format("    %s", client ));
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

    public static final String hh_set_thread_count = " <count> # set number of threads for processing NFS requests";
    public String ac_set_thread_count_$_1(Args args) throws Exception {
        _rpcService.setThreadCount(Integer.valueOf(args.argv(0)));
        return "Thread count: " + _rpcService.getThreadCount();
    }

    private static deviceid4 deviceidOf(int id) {
        byte[] deviceidBytes = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];
        Bytes.putInt(deviceidBytes, 0, id);

        return new deviceid4(deviceidBytes);
    }

    private static class PoolDS {

        private final deviceid4 _deviceId;
        private final InetSocketAddress _socketAddress;
        private final device_addr4 _deviceAddr;

        public PoolDS(deviceid4 deviceId, InetSocketAddress ip) {
            _deviceId = deviceId;
            _socketAddress = ip;
            _deviceAddr = DeviceManager
                    .deviceAddrOf(new RoundRobinStripingPattern<InetSocketAddress>(), ip);
        }

        public deviceid4 getDeviceId() {
            return _deviceId;
        }

        public InetSocketAddress getInetSocketAddress() {
            return _socketAddress;
        }

        public device_addr4 getDeviceAddr() {
            return _deviceAddr;
        }

        @Override
        public String toString() {
            return String.format("DS: %s, InetAddress: %s",
                    _deviceId, _socketAddress);
        }
    }

    private static class NfsTransfer extends RedirectedTransfer<PoolDS> {

        private final stateid4 _stateid;
        private final NFS4ProtocolInfo _protocolInfo;

        NfsTransfer(PnfsHandler pnfs, Subject subject, FsPath path, InetSocketAddress client,
                stateid4 stateid) {
            super(pnfs, subject, path);
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
        boolean binary = args.getOpt("binary") != null;
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
        List<IoDoorEntry> entries = new ArrayList<IoDoorEntry>();
        for (Transfer transfer : _ioMessages.values()) {
            entries.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(this.getCellName(), this.getCellDomainName());
        doorInfo.setProtocol("NFSV4.1", "0");
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(entries.toArray(new IoDoorEntry[0]));
        return (args.getOpt("binary") != null) ? doorInfo : doorInfo.toString();
    }
}
