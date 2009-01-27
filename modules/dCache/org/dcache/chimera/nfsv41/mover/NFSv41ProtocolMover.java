/*
 * $Id:NFSv41ProtocolMover.java 140 2007-06-07 13:44:55Z tigran $
 */

package org.dcache.chimera.nfsv41.mover;

import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.dcache.pool.movers.MoverProtocol;
import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

public class NFSv41ProtocolMover implements MoverProtocol {

    private static final int INC_SPACE = (50 * 1024 * 1024);
    private static final int DEFAULT_PORT = 2052;
    //
    // <init>( CellAdapter cell ) ;
    //
    private Args _args = null;
    private CellAdapter _cell = null;

    private final static NFSv4MoverHandler _nfsIO = new NFSv4MoverHandler(DEFAULT_PORT);

    public NFSv41ProtocolMover(CellAdapter cell) {
        // forced by mover contract

        _cell = cell;
        _args = _cell.getArgs();

    }

    public Object getAttribute(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    public long getBytesTransferred() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getLastTransferred() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getTransferTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    public void runIO(RandomAccessFile fileChannel, ProtocolInfo protocol,
            StorageInfo storage, PnfsId pnfsId,
            SpaceMonitor spaceMonitor, int access)
            throws Exception {



        /*
         * FIXME:
         * here we have to put all interfaces and send it to client.
         * in pNFS language this is't a multipath device.
         * To make this work, PoolPassiveIoFileMessage have to be changed.
         */
        InetAddress localIp = null;
        Enumeration<NetworkInterface> ne = NetworkInterface.getNetworkInterfaces();
        while(ne.hasMoreElements() ) {
            NetworkInterface iface = ne.nextElement();
            // java 6 way to do it
//            if( !iface.isLoopback() && iface.isUp() ) {
//                localIp = iface.getInetAddresses().nextElement();
//                break;
//            }

            localIp = iface.getInetAddresses().nextElement();
            if( !localIp.isLoopbackAddress() ) break;

        }

        _nfsIO.addHandler(pnfsId, fileChannel.getChannel());

        PoolPassiveIoFileMessage msg = new PoolPassiveIoFileMessage(_cell.getCellName(),
                new InetSocketAddress(localIp, DEFAULT_PORT), "".getBytes());


        CellPath cellpath = ((NFS4ProtocolInfo) protocol).door();
        msg.setId( ((NFS4ProtocolInfo) protocol).stateId() );
        _cell.sendMessage(new CellMessage(cellpath, msg), true, true);


        /*
         * hang forever, until thread is not stopped( interrupted )
         */
        while( !Thread.interrupted() ) {
            Thread.sleep(5000);
        }

    }

    public void setAttribute(String name, Object attribute) {
        // TODO Auto-generated method stub

    }

    public boolean wasChanged() {
        // TODO Auto-generated method stub
        return false;
    }

}
