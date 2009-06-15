/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import java.io.File;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.acplt.oncrpc.OncRpcException;
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
import org.dcache.chimera.nfs.v4.HimeraNFS4Server;
import org.dcache.chimera.nfs.v4.NFS4IoDevice;
import org.dcache.chimera.nfs.v4.NFSv41DeviceManager;
import org.dcache.chimera.nfs.v4.device_addr4;
import org.dcache.chimera.nfs.v4.layoutiomode4;
import org.dcache.chimera.nfs.v4.nfs4_prot;
import org.dcache.chimera.nfs.v4.nfsstat4;
import org.dcache.chimera.nfs.v4.stateid4;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.Message;
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
import org.acplt.oncrpc.XdrBufferDecodingStream;
import org.dcache.chimera.nfs.v4.client.GetDeviceListStub;
import org.dcache.chimera.nfs.v4.client.NFSv41Client;
import org.dcache.chimera.nfs.v4.layouttype4;
import org.dcache.chimera.nfs.v4.nfsv4_1_file_layout_ds_addr4;

public class NFSv41Door extends AbstractCell implements NFSv41DeviceManager {

    private static final Logger _log = Logger
            .getLogger("logger.org.dcache.doors.nfsv4");

    /** dCache-friendly NFS device id to pool name mapping */
    private Map<String, NFS4IoDevice> _poolNameToIpMap = new HashMap<String, NFS4IoDevice>();

    /** All known devices */
    private Map<DeviceID, NFS4IoDevice> _deviceMap = new HashMap<DeviceID, NFS4IoDevice>();


    /** next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);

    private final Map<StateidAsKey, PoolIoFileMessage> _ioMessages = new ConcurrentHashMap<StateidAsKey, PoolIoFileMessage>();

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
    private final Map<StateidAsKey, NFS4IoDevice> _requestReplyMap = new HashMap<StateidAsKey, NFS4IoDevice>();

    public NFSv41Door(String cellName, String args) throws Exception {
        super(cellName, args);

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

        new Thread("NFSv4.1 Door Thread") {
            @Override
            public void run() {
                try {
                    HimeraNFS4Server nfsServer = new HimeraNFS4Server(2049, _dm, fs, exportFile );
                    nfsServer.run();
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
                    DeviceID oldId = new DeviceID(device.getDeviceId());
                    _deviceMap.remove(oldId);
                }
                /*
                 * TODO: the PoolPassiveIoFileMessage have to be adopted to send list
                 * of all interfaces
                 */
                InetSocketAddress[] addresses = new InetSocketAddress[1];
                addresses[0] = poolAddress;
                device_addr4 deviceAddr = DeviceManager.deviceAddrOf(addresses);
                DeviceID deviceID = new DeviceID(id2deviceid(id));
                device = new NFS4IoDevice(id2deviceid(id), deviceAddr);
                _poolNameToIpMap.put(poolName, device);
                _deviceMap.put(deviceID, device);
                _log.debug("pool " + poolName + " mapped to deviceid " + Arrays.toString(deviceID.getId()) + " " + message.getId() + " inet: " + poolAddress);
            }

            XdrBufferDecodingStream xdr = new XdrBufferDecodingStream(message.challange());
            stateid4 stateid = new stateid4();

            xdr.beginDecoding();
            stateid.xdrDecode(xdr);
            xdr.endDecoding();

            synchronized (_requestReplyMap) {
                _requestReplyMap.put(new StateidAsKey(stateid), device);
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
    public NFS4IoDevice getIoDevice(byte[] deviceId) {

        NFS4IoDevice device = null;

        device = _deviceMap.get(new DeviceID(deviceId));

        return device;
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
                    storageInfo, protocolInfo, storageInfo.getFileSize() );
        } else {
            _log.debug("looking for write pool for " + pnfsId.toString());
            getPoolMessage = new PoolMgrSelectWritePoolMsg(pnfsId,
                    storageInfo, protocolInfo, 0);
        }

        _log.debug("requesting pool for IO: " + pnfsId );
        getPoolMessage = sendMessageXXX(_poolManagerPath, getPoolMessage, nfsstat4.NFS4ERR_LAYOUTTRYLATER);
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

        poolIOMessage = sendMessageXXX( new CellPath(getPoolMessage.getPoolName()),  poolIOMessage, nfsstat4.NFS4ERR_LAYOUTTRYLATER);
        _log.debug("mover ready: pool=" + getPoolMessage.getPoolName() + " moverid=" +
                poolIOMessage.getMoverId());
        _ioMessages.put( new StateidAsKey(protocolInfo.stateId()), poolIOMessage);

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
            StateidAsKey stateidAsKey = new StateidAsKey(stateid);
            while (!_requestReplyMap.containsKey(stateidAsKey) && timeToWait > 0) {

                long s = System.currentTimeMillis();
                _requestReplyMap.wait(NFS_REPLY_TIMEOUT);
                timeToWait -= System.currentTimeMillis() - s;
            }
            if( timeToWait <= 0 ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTTRYLATER,
                        "Mover did not started in time");
            }

            device = _requestReplyMap.remove(stateidAsKey);
        }

        _log.debug("request: " + Arrays.toString(stateid.other) + " : received device: " + Arrays.toString(device.getDeviceId()));

        return device;
    }

    
    private <T extends Message> T sendMessageXXX(CellPath destination, T message, int error_state)
            throws InterruptedException, IOException {

        CellMessage reply;
        try {
            reply = sendAndWait(new CellMessage(destination, message),
                    NFS_REPLY_TIMEOUT);
        }catch (NoRouteToCellException e) {
            // FIXME: in some cases we simply can retry
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_SERVERFAULT, e.getMessage());
        }

        if( reply == null ) {
            throw new ChimeraNFSException(error_state, message.getClass() + " not available in time");
        }

        if( reply.getMessageObject().getClass().isInstance(Exception.class) ) {
                Exception e = (Exception)reply.getMessageObject();
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_SERVERFAULT, e.getMessage());
        }

        T poolReply = (T)reply.getMessageObject();
        if( poolReply.getReturnCode() != 0 ) {
            throw new ChimeraNFSException(error_state,
                    "pool not available: " + poolReply.getReturnCode() );
        }

        return poolReply;        
    }

    @Override
    public List<NFS4IoDevice> getIoDeviceList() {
        List<NFS4IoDevice> knownDevices = new ArrayList<NFS4IoDevice>();

        knownDevices.addAll(_deviceMap.values());

        return knownDevices;
    }


    private static byte[] id2deviceid(int id) {


        byte[] buffer = Integer.toString(id).getBytes();
        byte[] devData = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];

        int len = Math.min(buffer.length, nfs4_prot.NFS4_DEVICEID4_SIZE);
        System.arraycopy(buffer, 0, devData, 0, len);

        return devData;

    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#addIoDevice(NFS4IoDevice device, int ioMode)
     */
    @Override
    public void addIoDevice(NFS4IoDevice device, int ioMode) {
        _deviceMap.put(new DeviceID(device.getDeviceId()), device);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#releaseDevice(IoDevice ioDevice)
     */
    @Override
    public void releaseDevice(stateid4 stateid) {

        try {

            PoolIoFileMessage poolIoFileMessage = _ioMessages.remove(new StateidAsKey(stateid));
            if( poolIoFileMessage != null ) {

                PoolMoverKillMessage message =
                    new PoolMoverKillMessage(poolIoFileMessage.getPoolName(),
                            poolIoFileMessage.getMoverId());

                message.setReplyRequired(false);
                try {
                    sendMessage(new CellMessage(new CellPath(poolIoFileMessage.getPoolName()),
                                                message));
                } catch (NoRouteToCellException e) {
                    _log.info("can't send mover kill message to " + poolIoFileMessage.getPoolName());
                }
            }

        }catch(NumberFormatException nfe) {
            _log.warn("invalid state id: " + new String(stateid.other));
        }
    }


    /*
     * wrapper class to make equals works on stateid4 objects.
     * The autogeberated code do not provide them,
     *
     * TODO: while spec is stabilized, i cann commit all autogenerated files into
     * source tree and fix classes to natively support equals.
     */
    public static class StateidAsKey {

        private final stateid4 _stateid;

        public StateidAsKey(stateid4 stateid) {
            _stateid = stateid;
        }

        @Override
        public boolean equals(Object obj) {

            if (obj == this) return true;

            if ( !(obj instanceof StateidAsKey))  return false;

            final stateid4 other = ((StateidAsKey) obj)._stateid;

            return other.seqid.value == _stateid.seqid.value && Arrays.equals(_stateid.other, other.other);
        }

        @Override
        public int hashCode() {
            return _stateid.seqid.value;
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

        nfsv4_1_file_layout_ds_addr4 file_layout = GetDeviceListStub.decodeFileDevice(device.getDeviceAddr().da_addr_body);
        return NFSv41Client.device2Address(file_layout.nflda_multipath_ds_list[0].value[0].na_r_addr);

    }
}
