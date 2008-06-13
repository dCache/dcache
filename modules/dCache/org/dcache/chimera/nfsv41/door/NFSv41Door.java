/*
 * $Id:NFSv41Door.java 140 2007-06-07 13:44:55Z tigran $
 */
package org.dcache.chimera.nfsv41.door;

import java.io.File;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.apps.jportmap.OncRpcEmbeddedPortmap;
import org.apache.log4j.Logger;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor;
import org.dcache.chimera.namespace.ChimeraStorageInfoExtractable;
import org.dcache.chimera.nfs.v4.DeviceID;
import org.dcache.chimera.nfs.v4.DeviceManager;
import org.dcache.chimera.nfs.v4.HimeraNFS4Server;
import org.dcache.chimera.nfs.v4.NFS4IoDevice;
import org.dcache.chimera.nfs.v4.NFSv41DeviceManager;
import org.dcache.chimera.nfs.v4.device_addr4;
import org.dcache.chimera.nfs.v4.layoutiomode4;
import org.dcache.chimera.nfs.v4.nfs4_prot;
import org.dcache.chimera.nfs.v4.stateid4;
import org.dcache.chimera.nfs.v4.uint32_t;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

public class NFSv41Door extends CellAdapter implements NFSv41DeviceManager {

    private static final Logger _log = Logger
            .getLogger("logger.org.dcache.doors.nfsv4");

    // standard Cell staff
    private final CellNucleus _nucleus;
    private final Args _args;

    /* dCache-friendly NFS device id to pool name mapping */
    private Map<String, NFS4IoDevice> _poolNameToIpMap = new HashMap<String, NFS4IoDevice>();

    /* All known devices */
    private Map<DeviceID, NFS4IoDevice> _deviceMap = new HashMap<DeviceID, NFS4IoDevice>();


    /* next device id, 0 reserved for MDS */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);

    /**
     * nfsv4 server engine
     */
    private HimeraNFS4Server _nfsServer = null;
    private JdbcFs _fs = null;

    /** storage info extractor */
    private final ChimeraStorageInfoExtractable _storageInfoExctractor = new ChimeraOsmStorageInfoExtractor();

    /** cell name of PoolManager */
    private final String _poolManagerName = "PoolManager";

    /** request/reply mapping */
    private final Map<Long, NFS4IoDevice> _requestReplayMap = new HashMap<Long, NFS4IoDevice>();
    private final AtomicLong _requestCounter = new AtomicLong(0);

    public NFSv41Door(String cellName, String args) throws Exception {
        super(cellName, args, false);

        try {

            _nucleus = getNucleus();
            _args = getArgs();

            XMLconfig config = new XMLconfig(new File(_args
                    .getOpt("chimeraConfig")));

            _fs = new JdbcFs(config);

            boolean isPortMapRunning = OncRpcEmbeddedPortmap.isPortmapRunning();
            if (!isPortMapRunning) {
                _log.info("Portmap is not available, starting embedded one...");
                new OncRpcEmbeddedPortmap();
            }

            final NFSv41DeviceManager _dm = this;
            new Thread("NFSv4.1 Door Thread") {
                @Override
                public void run() {
                    try {
                        _nfsServer = new HimeraNFS4Server(2049, _dm, _fs);
                        _nfsServer.run();

                    } catch (OncRpcException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (ChimeraFsException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                }

            }.start();

        } catch (Exception e) {
            esay(e);
            start();
            kill();
            throw e;
        }

        start();
    }

    // CellAdapter

    @Override
    public void messageArrived(CellMessage msg) {

        Object object = msg.getMessageObject();
        if (!(object instanceof diskCacheV111.vehicles.Message)) {
            say("Unexpected message class " + object.getClass());
            say("source = " + msg.getSourceAddress());
            return;
        }

        if (object instanceof PoolPassiveIoFileMessage) {
            poolPassiveIoFileMessageArrived((PoolPassiveIoFileMessage) object);
        } else if (object instanceof PoolMgrSelectPoolMsg) {

            PoolMgrSelectPoolMsg selectPoolMsg = (PoolMgrSelectPoolMsg) object;
            if (selectPoolMsg.getReturnCode() == 0) {
                _log.debug("pool received. Requesting the file: "
                        + selectPoolMsg.getPoolName());

                PoolIoFileMessage poolMessage ;

                if( selectPoolMsg instanceof PoolMgrSelectReadPoolMsg ) {

                    poolMessage = new PoolDeliverFileMessage(
                        selectPoolMsg.getPoolName(), selectPoolMsg.getPnfsId(),
                        selectPoolMsg.getProtocolInfo(), selectPoolMsg
                                .getStorageInfo());
                }else{
                    poolMessage = new PoolAcceptFileMessage(
                            selectPoolMsg.getPoolName(), selectPoolMsg.getPnfsId(),
                            selectPoolMsg.getProtocolInfo(), selectPoolMsg
                                    .getStorageInfo());
                }

                /*
                 * connect pass message id to the new request.
                 * id used to identify device id
                 */
                poolMessage.setId(selectPoolMsg.getId());

                try {
                    sendMessage(new CellMessage(new CellPath(selectPoolMsg
                            .getPoolName()), poolMessage));
                } catch (NotSerializableException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (NoRouteToCellException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                _log.error("pool error: " + selectPoolMsg.getReturnCode() + " "
                        + selectPoolMsg.getErrorObject());
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

    private void poolPassiveIoFileMessageArrived(
            PoolPassiveIoFileMessage message) {

        String poolName = message.getPoolName();

        _log.debug("NFS mover ready: " + poolName);

        NFS4IoDevice device = _poolNameToIpMap.get(poolName);
        if (device == null) {

            /* pool is unknown yet, so create new device and device-id */

            InetSocketAddress poolAddress = message.socketAddress();

            int id = this.nextDeviceID();


            //hard coded for now
            device_addr4 deviceAddr = DeviceManager.deviceAddrOf( poolAddress );

            DeviceID deviceID = new DeviceID(id2deviceid(id));

            device = new NFS4IoDevice(id2deviceid(id) , deviceAddr);

            _poolNameToIpMap.put(poolName, device);
            _deviceMap.put(deviceID, device);

            _log.debug("pool " + poolName + " mapped to deviceid "
                    + Arrays.toString(deviceID.getId()) + " " + message.getId() +
                    " inet: " + poolAddress);
        }

        synchronized (_requestReplayMap) {
            _requestReplayMap.put(message.getId(), device );
            _requestReplayMap.notifyAll();
        }

    }

    // NFSv41DeviceManager interface

    /*
    	The most important calls is LAYOUTGET, OPEN, CLOSE, LAYOUTRETURN
    	The READ, WRITE and  COMMIT goes to storage device.

    	We assume the follwing mapping between nfs and dcache:

    	     NFS     |  dCache
    	_____________|________________________________________
    	LAYOUTGET    : get pool, bind the answer to the client
    	OPEN         : send IO request to the pool
    	CLOSE        : sent end-of-IO to the pool, LAYOUTRECALL
    	LAYOUTRETURN : unbind pool from client

     */

    public NFS4IoDevice getIoDevice(byte[] deviceId) {

        NFS4IoDevice device = null;

        device = _deviceMap.get(new DeviceID(deviceId));

        return device;
    }

    /**
     * ask pool manager for a file
     */
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
            //if (ioMode == layoutiomode4.LAYOUTIOMODE4_RW) {
            if ( storageInfo.isCreatedOnly() ) {
                _log.debug("looking for write pool for " + inode.toString());
                getPoolMessage = new PoolMgrSelectWritePoolMsg(pnfsId,
                        storageInfo, protocolInfo, 0);
            } else {
                _log.debug("looking for read pool for " + inode.toString());
                getPoolMessage = new PoolMgrSelectReadPoolMsg(pnfsId,
                        storageInfo, protocolInfo, inode.statCache().getSize());
            }

            try {

                getPoolMessage.setId(myRequstId);

                sendMessage(new CellMessage(new CellPath(_poolManagerName),
                        getPoolMessage));
            } catch (NoRouteToCellException e) {
                throw new IOException(e.getMessage());
            }
        }catch(ChimeraFsException ce) {
            // java6 way,  throw new IOException(ce.getMessage(), ce);
            throw new IOException(ce.getMessage());
        } catch (CacheException ce) {
            // java6 way, throw new IOException(ce.getMessage(), ce);
            throw new IOException(ce.getMessage());
        }

        synchronized (_requestReplayMap) {

            while (_requestReplayMap.isEmpty()
                    || !_requestReplayMap.containsKey(myRequstId)) {

                try {
                    _requestReplayMap.wait();
                } catch (InterruptedException e) {
                    throw new IOException(e.getMessage());
                }
            }

            device = _requestReplayMap.remove(myRequstId);
            _log.debug("request: " + myRequstId + " : recieved device: " + Arrays.toString(device.getDeviceId()));

        }

        /*
         * FIXME: here we have to connect request with state.
         * probably the best to use is pool + mover
         */
        stateid4 stateid = new stateid4();
        stateid.seqid = new uint32_t(0);
        stateid.other = new byte[12];

        return new IoDevice(device, stateid);
    }

    public List<NFS4IoDevice> getIoDeviceList() {
        List<NFS4IoDevice> knownDevices = new ArrayList<NFS4IoDevice>();

        knownDevices.addAll(_deviceMap.values());

        return knownDevices;
    }


    private static byte[] id2deviceid(int id) {


        byte[] puffer = Integer.toString(id).getBytes();
        byte[] devData = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];

        int len = puffer.length >  nfs4_prot.NFS4_DEVICEID4_SIZE? nfs4_prot.NFS4_DEVICEID4_SIZE : puffer.length;
        System.arraycopy(puffer, 0, devData, 0, len);

        return devData;

    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#addIoDevice(NFS4IoDevice device, int ioMode)
     */
    public void addIoDevice(NFS4IoDevice device, int ioMode) {
        // TODO Auto-generated method stub
        _deviceMap.put(new DeviceID(device.getDeviceId()), device);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#releaseDevice(IoDevice ioDevice)
     */
    public void releaseDevice(IoDevice ioDevice) {
        // TODO Auto-generated method stub
        /*
         * here we have to connect find pool+mover by stateid and kill it
         */
    }
}
