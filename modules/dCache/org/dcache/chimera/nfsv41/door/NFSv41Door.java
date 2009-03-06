/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import org.dcache.chimera.nfs.v4.HimeraNFS4Exception;
import org.dcache.chimera.nfs.v4.HimeraNFS4Server;
import org.dcache.chimera.nfs.v4.NFS4IoDevice;
import org.dcache.chimera.nfs.v4.NFSv41DeviceManager;
import org.dcache.chimera.nfs.v4.device_addr4;
import org.dcache.chimera.nfs.v4.layoutiomode4;
import org.dcache.chimera.nfs.v4.nfs4_prot;
import org.dcache.chimera.nfs.v4.nfsstat4;
import org.dcache.chimera.nfs.v4.stateid4;
import org.dcache.chimera.nfs.v4.uint32_t;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
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

public class NFSv41Door extends AbstractCell implements NFSv41DeviceManager {

    private static final Logger _log = Logger
            .getLogger("logger.org.dcache.doors.nfsv4");

    /** dCache-friendly NFS device id to pool name mapping */
    private Map<String, NFS4IoDevice> _poolNameToIpMap = new HashMap<String, NFS4IoDevice>();

    /** All known devices */
    private Map<DeviceID, NFS4IoDevice> _deviceMap = new HashMap<DeviceID, NFS4IoDevice>();


    /** next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);

    private final Map<Long, PoolIoFileMessage> _ioMessages = new ConcurrentHashMap<Long, PoolIoFileMessage>();

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
    private final Map<Long, RequestReply> _requestReplyMap = new HashMap<Long, RequestReply>();
    private final AtomicLong _requestCounter = new AtomicLong(0);

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

        NFS4IoDevice device = _poolNameToIpMap.get(poolName);
        if (device == null) {

            /* pool is unknown yet, so create new device and device-id */

            InetSocketAddress poolAddress = message.socketAddress();

            int id = this.nextDeviceID();

            /*
             * TODO: the PoolPassiveIoFileMessage have to be adopted to send list
             * of all interfaces
             */
            InetSocketAddress[] addresses = new InetSocketAddress[1];
            addresses[0] = poolAddress;
            device_addr4 deviceAddr = DeviceManager.deviceAddrOf( addresses );

            DeviceID deviceID = new DeviceID(id2deviceid(id));

            device = new NFS4IoDevice(id2deviceid(id) , deviceAddr);

            _poolNameToIpMap.put(poolName, device);
            _deviceMap.put(deviceID, device);

            _log.debug("pool " + poolName + " mapped to deviceid "
                    + Arrays.toString(deviceID.getId()) + " " + message.getId() +
                    " inet: " + poolAddress);
        }

        synchronized (_requestReplyMap) {
            _requestReplyMap.put(message.getId(), new RequestReply(device) );
            _requestReplyMap.notifyAll();
        }

    }

    /*
     * Handle reply from PoolManager.
     * 
     * After getting a pool name send IO request to the pool.
     * Notify client in case of error ( GRACE or LAYOUTTRYLATER )
     * 
     */
    public void messageArrived(PoolMgrSelectPoolMsg message) {

        if (message.getReturnCode() == 0) {

            _log.debug("pool received. Requesting the file: " + message.getPoolName());

            PoolIoFileMessage poolMessage ;

            /*
             * TODO:
             * 
             * This part can be split into two messageArrived(XX,YY).
             * 
             */
            if( message instanceof PoolMgrSelectReadPoolMsg ) {

                poolMessage = new PoolDeliverFileMessage(
                        message.getPoolName(), message.getPnfsId(),
                        message.getProtocolInfo(), message
                            .getStorageInfo());
            }else{
                poolMessage = new PoolAcceptFileMessage(
                        message.getPoolName(), message.getPnfsId(),
                        message.getProtocolInfo(), message
                                .getStorageInfo());
            }

            /*
             * connect pass message id to the new request.
             * id used to identify device id
             */
            poolMessage.setId(message.getId());

            try {
                sendMessage(new CellMessage(new CellPath(message
                        .getPoolName()), poolMessage));
            } catch (NoRouteToCellException e) {
                /* 
                 * FIXME:
                 * 
                 * It's absolutely valid situation when pool went down before
                 * we have send IO request to it ( we should be happy about it ).
                 * 
                 * Simple add retry mechanism here. To make life easier for now
                 * I just skip it and will wait till Tatjana implements new
                 * door framework. 
                 */
            }
        } else {
            _log.error("pool error: " + message.getReturnCode() + " "
                    + message.getErrorObject());
            synchronized (_requestReplyMap) {
                _requestReplyMap.put(message.getId(), 
                        new RequestReply(
                                message.getReturnCode(),
                                message.getErrorObject())
                );
                _requestReplyMap.notifyAll();
            }
        }
    }

    public void messageArrived(PoolIoFileMessage message) {
        
        if( message.getReturnCode() != 0 ) {
            // failed to start a mover
            _log.error("pool error: " + message.getReturnCode() + " "
                    + message.getErrorObject());
            synchronized (_requestReplyMap) {
                _requestReplyMap.put(message.getId(),
                        new RequestReply(message.getReturnCode(),
                                message.getErrorObject())
                );
                _requestReplyMap.notifyAll();
            }
        }else{

            // keep mover id to stop(kill) it later
            try {
                _log.debug("mover ready: pool=" + message.getPoolName() + " moverid=" + message.getMoverId());
                NFS4ProtocolInfo protocolInfo =  (NFS4ProtocolInfo)message.getProtocolInfo();
                _ioMessages.put(protocolInfo.stateId(), message);
            }catch (ClassCastException e) {
                _log.error("unexpected protocol type received: " + message.getProtocolInfo().getClass());
            }

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
     * @throws HimeraNFS4Exception in case of NFS friendly errors ( like ACCESS ) 
     * @throws IOException in case of any other errors
     */
    @Override
    public IoDevice getIoDevice(FsInode inode, int ioMode, InetAddress clientIp)
            throws IOException {

        long myRequstId;
        NFS4IoDevice device;

        try {

            myRequstId = _requestCounter.incrementAndGet();

            PoolMgrSelectPoolMsg getPoolMessage = null;

            StorageInfo storageInfo = _storageInfoExctractor.getStorageInfo(inode);

            PnfsId pnfsId = new PnfsId(inode.toString());

            NFS4ProtocolInfo protocolInfo = new NFS4ProtocolInfo(clientIp, myRequstId);
            protocolInfo.door(new CellPath(this.getCellName(), this
                    .getCellDomainName()));


            /*
             * some how I always get a  LAYOUTIOMODE4_RW from solaris client.
             * let guess what client really wants.
             */
            if ( (ioMode == layoutiomode4.LAYOUTIOMODE4_READ) || !storageInfo.isCreatedOnly() ) {
                _log.debug("looking for read pool for " + inode.toString());
                getPoolMessage = new PoolMgrSelectReadPoolMsg(pnfsId,
                        storageInfo, protocolInfo, inode.statCache().getSize());
            } else {
                _log.debug("looking for write pool for " + inode.toString());
                getPoolMessage = new PoolMgrSelectWritePoolMsg(pnfsId,
                        storageInfo, protocolInfo, 0);
            }

            try {

                getPoolMessage.setId(myRequstId);

                sendMessage(new CellMessage(_poolManagerPath, getPoolMessage));
            } catch (NoRouteToCellException e) {
                throw new IOException(e.getMessage());
            }

            synchronized (_requestReplyMap) {

                /*
                 * FIXME;
                 * 
                 * usually RPC request will timeout in 30s.
                 * We have to handle this cases and return LAYOUTTRYLATER
                 * or GRACE.
                 * 
                 */
                while (!_requestReplyMap.containsKey(myRequstId)) {

                    try {
                        _requestReplyMap.wait();
                    } catch (InterruptedException e) {
                        throw new IOException(e.getMessage());
                    }
                }

                RequestReply reply = _requestReplyMap.remove(myRequstId);
                if( reply.getStatus() != 0 ) {
                    _log.debug("failed to get layout: " + reply.getError());
                    throw new HimeraNFS4Exception(nfsstat4.NFS4ERR_LAYOUTTRYLATER, "failed to get layout: " + reply.getError());
                }
                device = reply.getDevice();
                _log.debug("request: " + myRequstId + " : received device: " + Arrays.toString(device.getDeviceId()));

            }

            /*
             * put you request id as state id.
             * as soon as LAYOUTRETURN comes with this state id we can kill the mover
             */
            stateid4 stateid = new stateid4();
            stateid.seqid = new uint32_t(0);
            stateid.other = requestId2State(myRequstId);

            return new IoDevice(device, stateid);

        }catch(ChimeraFsException ce) {
            // java6 way,  throw new IOException(ce.getMessage(), ce);
            throw new IOException(ce.getMessage());
        } catch (CacheException ce) {
            // java6 way, throw new IOException(ce.getMessage(), ce);
            throw new IOException(ce.getMessage());
        }


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


    private static byte[] requestId2State(long id) {


        byte[] buffer = Long.toHexString(id).getBytes();
        byte[] devData = new byte[12];

        int len = Math.min(buffer.length, devData.length );
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
    public void releaseDevice(IoDevice ioDevice) {
        /*
         * here we have to connect find pool+mover by stateid and kill it
         */

        stateid4 stateid = ioDevice.getStateid();

        try {

            Long requestId = Long.valueOf( new String(stateid.other).trim(), 16 );
            PoolIoFileMessage poolIoFileMessage = _ioMessages.remove(requestId);
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
     * just a dummy class to pass to requester reply or error code
     */
    private static class RequestReply {

        private final int _status;
        private final NFS4IoDevice _device;
        private final Object _error;

        RequestReply(NFS4IoDevice device) {
            _status = 0;
            _device = device;
            _error = null;
        }

        RequestReply(int status, Object error) {
            _status = status;
            _device = null;
            _error = error;
        }

        NFS4IoDevice getDevice() {
            return _device;
        }

        Object getError() {
            return _error;
        }

        int getStatus() {
            return _status;
        }
    }
}
