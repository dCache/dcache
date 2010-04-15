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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
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

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import java.nio.ByteBuffer;
import org.acplt.oncrpc.OncRpcPortmapClient;
import org.dcache.cells.CellInfoProvider;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.xdr.mount_prot;
import org.dcache.chimera.nfs.v4.MDSOperationFactory;
import org.dcache.chimera.nfs.v4.xdr.layouttype4;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.chimera.nfsv41.Utils;
import org.dcache.poolmanager.PoolManagerAdapter;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.RpcDispatcher;
import org.dcache.xdr.RpcParserProtocolFilter;
import org.dcache.xdr.RpcProtocolFilter;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrDecodingStream;

public class NFSv41Door extends AbstractCellComponent implements
        NFSv41DeviceManager, CellCommandListener,
        CellMessageReceiver, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(NFSv41Door.class);

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

    /** request/reply mapping */
    private final Map<stateid4, NFS4IoDevice> _requestReplyMap = new HashMap<stateid4, NFS4IoDevice>();

    /**
     * Cell communication helper.
     */
    private CellStub _poolManagerStub;

    /**
     * Communication with the PoolManager.
     */
    private PoolManagerAdapter _poolManagerAdapter;


    private PnfsHandler _pnfsHandler;

    /**
     * Grizzly thread controller
     */
    private Controller _controller;

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

    public void init() throws Exception {

        boolean isPortMapRunning = OncRpcEmbeddedPortmap.isPortmapRunning();
        if (!isPortMapRunning) {
            _log.info("Portmap is not available, starting embedded one...");
            new OncRpcEmbeddedPortmap();
        }

        final NFSv41DeviceManager _dm = this;

        final Map<OncRpcProgram, RpcDispatchable> programs = new HashMap<OncRpcProgram, RpcDispatchable>();

        new Thread("NFSv4.1 Door Thread") {
            @Override
            public void run() {
                try {
                    NFSServerV41 nfs4 = new NFSServerV41( new MDSOperationFactory(),
                            _dm, _fileFileSystemProvider, _exportFile );

                    new OncRpcEmbeddedPortmap(2000);

                    OncRpcPortmapClient portmap = new OncRpcPortmapClient(InetAddress.getByName("127.0.0.1"));
                    portmap.getOncRpcClient().setTimeout(2000);
                    portmap.setPort(100005, 3, 6, 2049);
                    portmap.setPort(100005, 1, 6, 2049);
                    portmap.setPort(100003, 4, 6, 2049);

                    MountServer ms = new MountServer(_exportFile, _fileFileSystemProvider);

                    programs.put(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
                    programs.put(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);

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
                        _log.error("Exception in controller...", e);
                    }

                } catch (org.acplt.oncrpc.OncRpcException e) {
                    // TODO: kill the cell
                    _log.error(e.toString());
                } catch (OncRpcException e) {
                    // TODO: kill the cell
                    _log.error(e.toString());
                } catch (IOException e) {
                    // TODO: kill the cell
                    _log.error(e.toString());
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

        _log.debug("NFS mover ready: {}", poolName);

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
                _log.debug("pool {} mapped to deviceid {} {} inet: {}",
                        new Object[]{
                            poolName,
                            deviceID,
                            message.getId(),
                            poolAddress
                        });
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
        _ioMessages.remove(protocolInfo.stateId());
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
            PnfsId pnfsId = new PnfsId(inode.toString());
            StorageInfo storageInfo = _pnfsHandler.getStorageInfoByPnfsId(pnfsId).getStorageInfo();

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

        PoolIoFileMessage poolIOMessage = null;
        try {

            if ((iomode == layoutiomode4.LAYOUTIOMODE4_READ) || !storageInfo.isCreatedOnly()) {
                _log.debug("looking for read pool for {}", pnfsId);
                poolIOMessage = _poolManagerAdapter.readFile(pnfsId, storageInfo, protocolInfo, _poolManagerStub.getTimeout());
            } else {
                _log.debug("looking for write pool for {}", pnfsId);
                poolIOMessage = _poolManagerAdapter.writeFile(pnfsId, storageInfo, protocolInfo, _poolManagerStub.getTimeout());
            }

        } catch (NoRouteToCellException ex) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_RESOURCE, ex.getMessage());

        } catch (CacheException e) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER, e.getMessage());
        }
        _log.debug("mover ready: pool={} moverid={}", poolIOMessage.getPoolName(),
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

        _log.debug("request: {} : received device: {}", stateid, device.getDeviceId());

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

        _log.debug("Releasing device by stateid: {}", stateid);

        PoolIoFileMessage poolIoFileMessage = _ioMessages.remove(stateid);
        if (poolIoFileMessage != null) {

            PoolMoverKillMessage message =
                    new PoolMoverKillMessage(poolIoFileMessage.getPoolName(),
                    poolIoFileMessage.getMoverId());

            _log.debug("Sending KILL to {}@{}", poolIoFileMessage.getMoverId(),
                poolIoFileMessage.getPoolName() );

            try {
                _poolManagerStub.send(new CellPath(poolIoFileMessage.getPoolName()),
                                      message);
            } catch (NoRouteToCellException e) {
                _log.error("Failed to kill mover: {}", e.getMessage());
            }

        }else{
            _log.warn("Can't find mover by stateid: {}", stateid);
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

    /**
     * Gets the generic PoolManagerAdapter to handle read and write request
     * @return the _poolManagerAdapter
     */
    public PoolManagerAdapter getPoolManagerAdapter() {
        return _poolManagerAdapter;
    }

    /**
     * Sets the generic PoolManagerAdapter to handle read and write request
     * @param poolManagerAdapter the _poolManagerAdapter to set
     */
    public void setPoolManagerAdapter(PoolManagerAdapter poolManagerAdapter) {
        this._poolManagerAdapter = poolManagerAdapter;
    }

    /*
     * Cell specific
     */
    @Override
    public void getInfo(PrintWriter pw) {

        pw.println("NFSv4.1 door (MDS):");
        pw.println( String.format("  Concurrent Thread number : %d", _controller.getReadThreadsCount() ));
        pw.println( String.format("  Thread pool              : %s", _controller.getThreadPool() ));
        pw.println("  Known pools (DS):\n");
        for(Map.Entry<String, NFS4IoDevice> ioDevice: _poolNameToIpMap.entrySet()) {
            pw.println( String.format("    %s : %s", ioDevice.getKey(),ioDevice.getValue() ));
        }

        pw.println();
        pw.println("  Known movers (layouts):");
        for(PoolIoFileMessage io: _ioMessages.values()) {
            pw.println( String.format("    %s : %s@%s", io.getPnfsId(), io.getMoverId(), io.getPoolName() ));
        }

        pw.println();
        pw.println("  Known clients:");
        for (NFS4Client client : NFSv4StateHandler.getInstace().getClients()) {
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
        _controller.setReadThreadsCount(Integer.valueOf(args.argv(0)));
        return "Thread count: " + _controller.getReadThreadsCount();
    }
}
