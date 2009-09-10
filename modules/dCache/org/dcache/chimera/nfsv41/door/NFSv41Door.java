/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import com.sun.grizzly.BaseSelectionKeyHandler;
import com.sun.grizzly.Controller;
import com.sun.grizzly.DefaultProtocolChain;
import com.sun.grizzly.DefaultProtocolChainInstanceHandler;
import com.sun.grizzly.ProtocolChain;
import com.sun.grizzly.ProtocolChainInstanceHandler;
import com.sun.grizzly.ProtocolFilter;
import com.sun.grizzly.TCPSelectorHandler;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.xdr.OncRpcException;
import org.acplt.oncrpc.apps.jportmap.OncRpcEmbeddedPortmap;
import org.apache.log4j.Logger;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor;
import org.dcache.chimera.namespace.ChimeraStorageInfoExtractable;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v4.DeviceID;
import org.dcache.chimera.nfs.v4.DeviceManager;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.cells.CellStub;
import org.dcache.chimera.nfs.v4.NFSServerV41;
import org.dcache.chimera.nfs.v4.NFS4Client;
import org.dcache.chimera.nfs.v4.NFSv4StateHandler;
import org.dcache.chimera.nfs.v4.NFS4IoDevice;
import org.dcache.chimera.nfs.v4.NFSv41DeviceManager;
import org.dcache.chimera.nfs.v4.xdr.device_addr4;
import org.dcache.chimera.nfs.v4.xdr.layoutiomode4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import java.nio.ByteBuffer;
import org.acplt.oncrpc.OncRpcPortmapClient;
import org.dcache.cells.CellCommandListener;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v4.xdr.layouttype4;
import org.dcache.chimera.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.chimera.nfsv41.Utils;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.RpcDispatcher;
import org.dcache.xdr.RpcParserProtocolFilter;
import org.dcache.xdr.RpcProtocolFilter;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrDecodingStream;

public class NFSv41Door extends AbstractCell
        implements NFSv41DeviceManager, CellCommandListener {

    private static final Logger _log = Logger.getLogger(NFSv41Door.class);

    /** dCache-friendly NFS device id to pool name mapping */
    private Map<String, NFS4IoDevice> _poolNameToIpMap = new HashMap<String, NFS4IoDevice>();

    /** All known devices */
    private Map<DeviceID, NFS4IoDevice> _deviceMap = new HashMap<DeviceID, NFS4IoDevice>();


    /** next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);

    private final Map<stateid4, PoolIoFileMessage> _ioMessages = new ConcurrentHashMap<stateid4, PoolIoFileMessage>();

    /**
     * The usual timeout for NFS ops. is 30s.
     * We will use a bit shorter (27s) one to avoid retries.
     */
    private final static int NFS_REPLY_TIMEOUT = 27000; 

    /**
     * nfsv4 server engine
     */

    /** storage info extractor */
    private ChimeraStorageInfoExtractable _storageInfoExctractor;

    @Option (
        name = "poolManager",
        description = "well known name of the pool manager",
        defaultValue = "PoolManager"
    )
    private CellPath _poolManagerPath;

    @Option(
       name = "chimeraConfig",
       description = "path to chimera config file",
       required = true
    )
    private File _chimeraConfigFile;

    @Option(
        name = "nfs-exports",
        description = "path to nfs exports file",
        defaultValue = "/etc/exports"
    )
    private File _exports;

    /** request/reply mapping */
    private final Map<stateid4, NFS4IoDevice> _requestReplyMap = new HashMap<stateid4, NFS4IoDevice>();

    /**
     * Cell communication helper.
     */
    private final CellStub _poolManagerCellStub;

    /**
     * Grizzly thread controller
     */
    private Controller _controller;

    public NFSv41Door(String cellName, String args) throws Exception {
        super(cellName, args);
        _poolManagerCellStub = new CellStub();
        _poolManagerCellStub.setCellEndpoint(this);
        _poolManagerCellStub.setDestinationPath(_poolManagerPath);
        _poolManagerCellStub.setTimeout(NFS_REPLY_TIMEOUT);

        doInit();
    }

    @Override
    protected void init() throws Exception {

        super.init();

        AccessLatency defaultAccessLatency;
        String accessLatensyOption = getArgs().getOpt("DefaultAccessLatency");
        if( accessLatensyOption != null && accessLatensyOption.length() > 0) {
            /*
             * IllegalArgumentException thrown if option is invalid
             */
            defaultAccessLatency = AccessLatency.getAccessLatency(accessLatensyOption);
        }else{
            defaultAccessLatency = StorageInfo.DEFAULT_ACCESS_LATENCY;
        }

        RetentionPolicy defaultRetentionPolicy;
        String retentionPolicyOption = getArgs().getOpt("DefaultRetentionPolicy");
        if( retentionPolicyOption != null && retentionPolicyOption.length() > 0) {
            /*
             * IllegalArgumentException thrown if option is invalid
             */
            defaultRetentionPolicy = RetentionPolicy.getRetentionPolicy(retentionPolicyOption);
        }else{
            defaultRetentionPolicy = StorageInfo.DEFAULT_RETENTION_POLICY;
        }
        _storageInfoExctractor = new ChimeraOsmStorageInfoExtractor(defaultAccessLatency, defaultRetentionPolicy);

        XMLconfig config = new XMLconfig(_chimeraConfigFile);

        final FileSystemProvider fs = new JdbcFs(config);

        boolean isPortMapRunning = OncRpcEmbeddedPortmap.isPortmapRunning();
        if (!isPortMapRunning) {
            _log.info("Portmap is not available, starting embedded one...");
            new OncRpcEmbeddedPortmap();
        }

        final NFSv41DeviceManager _dm = this;
        final ExportFile exportFile = new ExportFile(_exports);

        final Map<Integer, RpcDispatchable> programs = new HashMap<Integer, RpcDispatchable>();

        new Thread("NFSv4.1 Door Thread") {
            @Override
            public void run() {
                try {
                    NFSServerV41 nfs4 = new NFSServerV41(_dm, fs, exportFile );

                    new OncRpcEmbeddedPortmap(2000);

                    OncRpcPortmapClient portmap = new OncRpcPortmapClient(InetAddress.getByName("127.0.0.1"));
                    portmap.getOncRpcClient().setTimeout(2000);
                    portmap.setPort(100005, 3, 6, 2049);
                    portmap.setPort(100005, 1, 6, 2049);
                    portmap.setPort(100003, 4, 6, 2049);

                    ExportFile exports = new ExportFile(new File("/etc/exports"));
                    MountServer ms = new MountServer(exports, fs);

                    programs.put(100003, nfs4);
                    programs.put(100005, ms);

                    final ProtocolFilter rpcFilter = new RpcParserProtocolFilter();
                    final ProtocolFilter rpcProcessor = new RpcProtocolFilter();
                    final ProtocolFilter rpcDispatcher = new RpcDispatcher(programs);

                    _controller = new Controller();
                    final TCPSelectorHandler tcp_handler = new TCPSelectorHandler();
                    tcp_handler.setPort(2049);
                    tcp_handler.setSelectionKeyHandler(new BaseSelectionKeyHandler());

                    _controller.addSelectorHandler(tcp_handler);
                    _controller.setReadThreadsCount(5);

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

                    _controller.setProtocolChainInstanceHandler(pciHandler);

                    try {
                        _controller.start();
                    } catch (IOException e) {
                        _log.fatal("Exception in controller...", e);
                    }

                } catch (org.acplt.oncrpc.OncRpcException e) {
                    // TODO: kill the cell
                    error(e);
                } catch (OncRpcException e) {
                    // TODO: kill the cell
                    error(e);
                } catch (IOException e) {
                    // TODO: kill the cell
                    error(e);
                } catch (ChimeraFsException e) {
                    // TODO: kill the cell
                    error(e);
                }

            }

        }.start();

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

        _log.debug("NFS mover ready: " + poolName);

        InetSocketAddress poolAddress = message.socketAddress();
        NFS4IoDevice device = _poolNameToIpMap.get(poolName);

        try {
            if (device == null || !getSocketAddress(device).equals(poolAddress)) {
                /* pool is unknown yet or has been restarted so create new device and device-id */
                int id = this.nextDeviceID();

                if( device != null ) {
                    /*
                     * clean stale entry
                     */
                    DeviceID oldId = device.getDeviceId();
                    _deviceMap.remove(oldId);
                }
                /*
                 * TODO: the PoolPassiveIoFileMessage have to be adopted to send list
                 * of all interfaces
                 */
                InetSocketAddress[] addresses = new InetSocketAddress[1];
                addresses[0] = poolAddress;
                device_addr4 deviceAddr = DeviceManager.deviceAddrOf(addresses);
                DeviceID deviceID = DeviceID.valueOf(id);
                device = new NFS4IoDevice(deviceID, deviceAddr);
                _poolNameToIpMap.put(poolName, device);
                _deviceMap.put(deviceID, device);
                _log.debug("pool " + poolName + " mapped to deviceid " + deviceID
                        + " " + message.getId() + " inet: " + poolAddress);
            }

            XdrDecodingStream xdr = new XdrBuffer(ByteBuffer.wrap(message.challange()));
            stateid4 stateid = new stateid4();

            xdr.beginDecoding();
            stateid.xdrDecode(xdr);
            xdr.endDecoding();

            synchronized (_requestReplyMap) {
                _requestReplyMap.put(stateid, device);
                _requestReplyMap.notifyAll();
            }

        } catch (UnknownHostException ex) {
            _log.error("Invald address returned by " + poolName + " : " + ex.getMessage() );
        } catch (OncRpcException ex) {
           // forced by interface
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            // forced by interface
            throw new RuntimeException(ex);
        }

    }

    protected void messageArrived(DoorTransferFinishedMessage transferFinishedMessage) {

        NFS4ProtocolInfo protocolInfo = (NFS4ProtocolInfo)transferFinishedMessage.getProtocolInfo();
        _ioMessages.remove(protocolInfo.stateId());
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("    $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $");
    }

    // message handling

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
    public NFS4IoDevice getIoDevice(DeviceID deviceId) {

        return _deviceMap.get(deviceId);

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
    public NFS4IoDevice getIoDevice(FsInode inode, int ioMode, InetAddress clientIp, stateid4 stateid)
            throws IOException {

        try {

            StorageInfo storageInfo = _storageInfoExctractor.getStorageInfo(inode);

            PnfsId pnfsId = new PnfsId(inode.toString());

            NFS4ProtocolInfo protocolInfo = new NFS4ProtocolInfo(clientIp, stateid);
            protocolInfo.door(new CellPath(this.getCellName(), this
                    .getCellDomainName()));

            return getNFSMover(pnfsId, storageInfo, protocolInfo, ioMode);

        } catch (InterruptedException e) {
            throw new IOException(e.getMessage());
        } catch (CacheException ce) {
            // java6 way, throw new IOException(ce.getMessage(), ce);
            throw new IOException(ce.getMessage());
        }

    }

    private NFS4IoDevice getNFSMover(PnfsId pnfsId, StorageInfo storageInfo,
            NFS4ProtocolInfo protocolInfo, int iomode) throws InterruptedException, IOException {

        PoolMgrSelectPoolMsg getPoolMessage;

        if ( (iomode == layoutiomode4.LAYOUTIOMODE4_READ) || !storageInfo.isCreatedOnly() ) {
            _log.debug("looking for read pool for " + pnfsId.toString());
            getPoolMessage = new PoolMgrSelectReadPoolMsg(pnfsId,
                    storageInfo, protocolInfo, storageInfo.getFileSize());
        } else {
            _log.debug("looking for write pool for " + pnfsId.toString());
            getPoolMessage = new PoolMgrSelectWritePoolMsg(pnfsId,
                    storageInfo, protocolInfo, 0);
        }

        _log.debug("requesting pool for IO: " + pnfsId );
        try {
            getPoolMessage = _poolManagerCellStub.sendAndWait(getPoolMessage);
        }catch (CacheException e) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER, e.getMessage() );
        }
        _log.debug("pool received. Requesting the file: " + getPoolMessage.getPoolName());

        PoolIoFileMessage poolIOMessage ;
        if ( (iomode == layoutiomode4.LAYOUTIOMODE4_READ) || !storageInfo.isCreatedOnly() ) {
            poolIOMessage = new PoolDeliverFileMessage(
                    getPoolMessage.getPoolName(), pnfsId,
                    protocolInfo, storageInfo);
        }else{
            poolIOMessage = new PoolAcceptFileMessage(
                    getPoolMessage.getPoolName(), pnfsId,
                    protocolInfo, storageInfo);
        }

        try {
            poolIOMessage = _poolManagerCellStub.sendAndWait( new CellPath(getPoolMessage.getPoolName()),  poolIOMessage);
        }catch(CacheException e) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER, e.getMessage() );
        }
        _log.debug("mover ready: pool=" + getPoolMessage.getPoolName() + " moverid=" +
                poolIOMessage.getMoverId());
        _ioMessages.put( protocolInfo.stateId(), poolIOMessage);

            /*
             * FIXME;
             * 
             * usually RPC request will timeout in 30s.
             * We have to handle this cases and return LAYOUTTRYLATER
             * or GRACE.
             * 
             */
        NFS4IoDevice device;
        stateid4 stateid = protocolInfo.stateId();
        int timeToWait = NFS_REPLY_TIMEOUT;
        synchronized (_requestReplyMap) {
            while (!_requestReplyMap.containsKey(stateid) && timeToWait > 0) {
                long s = System.currentTimeMillis();
                _requestReplyMap.wait(NFS_REPLY_TIMEOUT);
                timeToWait -= System.currentTimeMillis() - s;
            }
            if( timeToWait <= 0 ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER,
                        "Mover did not started in time");
            }

            device = _requestReplyMap.remove(stateid);
        }

        _log.debug("request: " + stateid + " : received device: " + device.getDeviceId());

        return device;
    }

    @Override
    public List<NFS4IoDevice> getIoDeviceList() {
        List<NFS4IoDevice> knownDevices = new ArrayList<NFS4IoDevice>();

        knownDevices.addAll(_deviceMap.values());

        return knownDevices;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#addIoDevice(NFS4IoDevice device, int ioMode)
     */
    @Override
    public void addIoDevice(NFS4IoDevice device, int ioMode) {
        _deviceMap.put(device.getDeviceId(), device);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#releaseDevice(stateid4 stateid)
     */
    @Override
    public void releaseDevice(stateid4 stateid) {

        PoolIoFileMessage poolIoFileMessage = _ioMessages.remove(stateid);
        if (poolIoFileMessage != null) {

            PoolMoverKillMessage message =
                    new PoolMoverKillMessage(poolIoFileMessage.getPoolName(),
                    poolIoFileMessage.getMoverId());

            _poolManagerCellStub.send(message);
        }
    }

    /**
     * Get {@link InetSocketAddress} connected to particular device.
     * TODO: this code have to go back into NFSv4.1 server code
     * @param device
     * @return address of the device
     * @throws UnknownHostException
     * @throws OncRpcException
     * @throws IOException
     * @throws IllegalArgumentException if device type is not NfsFileLyaout type
     */
    InetSocketAddress getSocketAddress(NFS4IoDevice device) throws UnknownHostException, OncRpcException, IOException {

        device_addr4 addr = device.getDeviceAddr();
        if( addr.da_layout_type != layouttype4.LAYOUT4_NFSV4_1_FILES ) {
            throw new IllegalArgumentException("Unsupported layout type: " +addr.da_layout_type );
        }

        nfsv4_1_file_layout_ds_addr4 file_layout = Utils.decodeFileDevice(device.getDeviceAddr().da_addr_body);
        return Utils.device2Address(file_layout.nflda_multipath_ds_list[0].value[0].na_r_addr);

    }

    /*
     * Cell specific
     */

    @Override
    public String getInfo() {

        StringBuilder sb = new StringBuilder();
        sb.append("NFSv4.1 door (MDS): \n");
        sb.append("  Concurrent Thread number : ").append(_controller.getReadThreadsCount()).append("\n");
        sb.append("  Thread pool              : ").append(_controller.getThreadPool()).append("\n");
        sb.append("  Known pools (DS):\n");
        for(Map.Entry<String, NFS4IoDevice> ioDevice: _poolNameToIpMap.entrySet()) {
            sb.append("    ").append(ioDevice.getKey()).append(" : ").
                    append(ioDevice.getValue()).append("\n");
        }

        sb.append("\n  Known movers (layouts):\n");
        for(PoolIoFileMessage io: _ioMessages.values()) {
            sb.append("    ").append(io.getPnfsId()).append(" : ").append(io.getMoverId()).
                    append("@").append(io.getPoolName()).append("\n");
        }

        sb.append("\n  Known clients:\n");
        for (NFS4Client client : NFSv4StateHandler.getInstace().getClients()) {
            sb.append("    ").append(client).append("\n");
        }

        return sb.toString();

    }

    public static final String hh_ac_kill_mover = " <pool> <moverid> # kill mover on the pool";
    public String ac_kill_mover_$_2(Args args) throws Exception {
        int mover = Integer.parseInt(args.argv(1));
        String pool = args.argv(0);

        PoolMoverKillMessage message = new PoolMoverKillMessage(pool, mover);

        message.setReplyRequired(false);
        sendMessage(new CellMessage(new CellPath(pool), message));
        return "";
    }

    public static final String hh_ac_set_thread_count = " <count> # set number of concurrent threads";
    public String ac_set_thread_count_$_1(Args args) throws Exception {
        _controller.setReadThreadsCount(Integer.valueOf(args.argv(0)));
        return "Thread count: " + _controller.getReadThreadsCount();
    }
}
