package org.dcache.chimera.nfsv41.mover;

import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.movers.ManualMover;
import org.dcache.pool.repository.Allocator;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.pool.movers.IoMode;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrEncodingStream;

public class NFSv41ProtocolMover implements ManualMover {

    private final CellEndpoint _cell;
    private long _bytesTransferred = 0;
    private long _lastAccessTime = 0;
    private long _started = 0;
    private long _ended = 0;

    private IoMode _ioMode = IoMode.READ;
    private static final Logger _log = LoggerFactory.getLogger(NFSv41ProtocolMover.class);

    private static NFSv4MoverHandler _nfsIO;
    static {
        try {
            String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");
            PortRange portRange;
            if( dcachePorts != null) {
                portRange = PortRange.valueOf(dcachePorts);
            }else{
                portRange = new PortRange(0);
            }
            _nfsIO = new NFSv4MoverHandler(portRange);
        }catch(Exception e) {
            _nfsIO = null;
            _log.error("Failed to initialize NFS mover", e);
        }
    }


    public NFSv41ProtocolMover(CellEndpoint cell) {
        _cell = cell;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getAttribute()
     */
	@Override
    public Object getAttribute(String name) {
        // forced by MoverProtocol interface
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getBytesTransferred()
     */
	@Override
    public long getBytesTransferred() {
        return _bytesTransferred;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getLastTransferred()
     */
	@Override
    public long getLastTransferred() {
        return _lastAccessTime;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#getTransferTime()
     */
	@Override
    public long getTransferTime() {

        if (_ended < _started) {
            return System.currentTimeMillis() - _started;
        } else {
            return _ended - _started;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.pool.movers.MoverProtocol#runIO(RandomAccessFile raf, ProtocolInfo protocol,
     *      StorageInfo storage, PnfsId pnfsId,
     *      SpaceMonitor spaceMonitor, int access)
     */
	@Override
    public void runIO(RandomAccessFile raf, ProtocolInfo protocol,
            StorageInfo storage, PnfsId pnfsId,
            Allocator allocator, IoMode access)
            throws Exception {

        _log.debug("new IO request for {} access: {}", pnfsId, access);

        if( _nfsIO == null ) {
            throw new IllegalStateException("NFS mover not ready");
        }
        NFS4ProtocolInfo nfs4ProtocolInfo = (NFS4ProtocolInfo) protocol;

        InetAddress localAddress = NetworkUtils.getLocalAddressForClient(nfs4ProtocolInfo.getHosts());

        _log.debug("using local interface: {}", localAddress);

        _ioMode = access;
        stateid4 stateid = ((NFS4ProtocolInfo) protocol).stateId();

        MoverBridge moverBridge = new MoverBridge(this, pnfsId, stateid, raf.getChannel(), access, allocator );

        _nfsIO.addHandler(moverBridge);
        try {

            _started = System.currentTimeMillis();

            /*
             * FIXME:
             * we need to change someting in PoolPassiveIoFileMessage. Probably
             * include some protocol specific information, like ProtocolInfo.
             *
             * While such change will brake intercomponent and inter version compatibility
             * let prosponde it till final sollution.
             *
             */

            XdrEncodingStream xdr = new XdrBuffer(128);
            xdr.beginEncoding();
            stateid.xdrEncode(xdr);
            xdr.endEncoding();
            byte[] d = xdr.body().array();

            PoolPassiveIoFileMessage msg = new PoolPassiveIoFileMessage(_cell.getCellInfo().getCellName(),
                    new InetSocketAddress(localAddress, _nfsIO.getLocalPort()), d);

            CellPath cellpath = ((NFS4ProtocolInfo) protocol).door();
            _cell.sendMessage(new CellMessage(cellpath, msg));


            /*
             * hang forever, until thread is not stopped( interrupted )
             */
            boolean done = false;
            while( !done ) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                }catch(InterruptedException ie) {
                    done = true;
                }
            }

        }finally{
            /*
             * tell nfs engine that IO for this file not allowed any more.
             */
            _nfsIO.removeHandler(moverBridge);
        }

        _ended = System.currentTimeMillis();
        _log.debug("IO request for {} done", pnfsId);
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.movers.MoverProtocol#setAttribute(String name, Object attribute)
     */
	@Override
    public void setAttribute(String name, Object attribute) {
        // forced by MoverProtocol interface
    }


    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.movers.MoverProtocol#wasChanged()
     */
	@Override
    public boolean wasChanged() {
        return  _ioMode == IoMode.WRITE && (getBytesTransferred() > 0);
    }


    /**
     * Set number of transfered bytes. The total transfered bytes count will
     * be increased. All negative values  ignored. The last access time is updated.
     *
     * @param bytesTransferred
     */
	@Override
    public void setBytesTransferred(long bytesTransferred) {

        if( bytesTransferred  < 0 ) return;

        _bytesTransferred += bytesTransferred;
        _lastAccessTime = System.currentTimeMillis();
    }

}
