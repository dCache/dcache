package org.dcache.pool.movers;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.util.PortRange;

import org.dcache.pool.repository.Allocator;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.movers.NetIFContainer;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.util.NetworkUtils;

public class XrootdProtocol_2 implements MoverProtocol
{
    private static final Logger _log = LoggerFactory.getLogger(XrootdProtocol_2.class);

    private static final int[] DEFAULT_PORTRANGE = {20000, 25000};
    private static final int LISTEN_TIMEOUT = 10 * 60 * 1000;

    private final CellEndpoint cell;
    private RandomAccessFile diskFile;
    private XrootdProtocolInfo protocolInfo;
    private StorageInfo storageInfo;
    private PnfsId pnfsId;
    private Allocator allocator;
    private volatile long lastTransferred;
    private volatile long transferTime;
    private volatile long bytesTransferred;
    private final CountDownLatch transferFinishedSync = new CountDownLatch(1);
    private volatile boolean transferSuccessful = false;
    private int xrootdFileHandle;

    // this checksum comes from the door and is compared to the
    // calculated one when the reopen is received to make sure that
    // the OpenRequest has not changed meanwhile
    private long openChecksum;

    public XrootdProtocol_2(CellEndpoint cell)
    {
        this.cell = cell;
        _log.debug("Xrootd mover created.");
    }

    public void runIO(RandomAccessFile diskFile, ProtocolInfo protocol, StorageInfo storage, PnfsId pnfsId, Allocator allocator, int access) throws Exception
    {
        this.diskFile = diskFile;
        this.protocolInfo = (XrootdProtocolInfo) protocol;
        this.storageInfo = storage;
        this.pnfsId = pnfsId;
        this.allocator = allocator;

        Socket socket = accept(protocolInfo);
        try {
            _log.info("got connection attempt");

            // create new XrootdConnection based on existing socket
            PhysicalXrootdConnection physicalXrootdConnection =
                new PhysicalXrootdConnection(socket, XrootdProtocol.DATA_SERVER);
            try {
                // set controller for this connection to handle login, auth
                // and connection-specific settings
                XrootdMoverController controller =
                    new XrootdMoverController(this, physicalXrootdConnection);
                physicalXrootdConnection.setConnectionListener(controller);

                // initialise counter
                setLastTransferred();

                // gets notfied on transfer finished by xrootd subsystem
                transferFinishedSync.await();
            } finally {
                physicalXrootdConnection.closeConnection();
            }
        } finally {
            socket.close();
        }

        if (!isTransferSuccessful()) {
            _log.error("xrootd transfer failed");
            throw new CacheException("xrootd transfer failed");
        }

        _log.info("normal end of xrootd mover process, transfer successful");
    }

    private Socket accept(XrootdProtocolInfo xrootdProtocol)
        throws IllegalArgumentException, IOException, CacheException,
               NoRouteToCellException
    {
        String portRange = System.getProperty("org.dcache.net.tcp.portrange");
        PortRange range;
        if (portRange != null) {
            range = PortRange.valueOf(portRange);
        } else {
            range = new PortRange(DEFAULT_PORTRANGE[0], DEFAULT_PORTRANGE[1]);
        }

        ServerSocket xrootdServer = new ServerSocket();
        try {
            range.bind(xrootdServer);

            int serverPort = xrootdServer.getLocalPort();

            //
            // look for all network interfaces
            //
            this.xrootdFileHandle = xrootdProtocol.getXrootdFileHandle();
            this.openChecksum = xrootdProtocol.getChecksum();

            Collection netifsCol = new ArrayList();

            InetAddress localIP = NetworkUtils.getLocalAddressForClient(xrootdProtocol.getHosts());

            if (localIP != null && !localIP.isLoopbackAddress() && localIP instanceof Inet4Address) {
                // the ip we got from the hostname is at least not
                // 127.0.01 and from the IP4-family
                Collection col = new ArrayList(1);
                col.add(localIP);
                netifsCol.add(new NetIFContainer("", col));
                _log.debug("sending ip-address derived from hostname to Xrootd-door: "+localIP+" port: "+serverPort);
            } else {
                // the ip we got from the hostname seems to be bad,
                // let's loop through the network interfaces
                Enumeration ifList = NetworkInterface.getNetworkInterfaces();

                while (ifList.hasMoreElements()) {
                    NetworkInterface netif =
                        (NetworkInterface) ifList.nextElement();

                    Enumeration ips = netif.getInetAddresses();
                    Collection ipsCol = new ArrayList(2);

                    while (ips.hasMoreElements()) {
                        InetAddress addr = (InetAddress) ips.nextElement();

                        // check again each ip from each interface.
                        // WARNING: multiple ip addresses in case of
                        // multiple ifs could be selected, we can't
                        // determine the "correct" one
                        if (addr instanceof Inet4Address
                            && !addr.isLoopbackAddress()) {
                            ipsCol.add(addr);
                            _log.debug("sending ip-address derived from network-if to Xrootd-door: "+addr+" port: "+serverPort);
                        }
                    }

                    if (ipsCol.size() > 0) {
                        netifsCol.add(new NetIFContainer(netif.getName(), ipsCol));
                    } else {
                        throw new CacheException("Error: Cannot determine my ip address. Aborting transfer");
                    }
                }
            }

            //
            // send message back to the door, containing the new
            // serverport and ip
            //
            CellPath cellpath = xrootdProtocol.getXrootdDoorCellPath();
            XrootdDoorAdressInfoMessage doorMsg = new XrootdDoorAdressInfoMessage(getXrootdFileHandle(), serverPort, netifsCol);
            cell.sendMessage (new CellMessage(cellpath, doorMsg));

            _log.debug("sending redirect message to Xrootd-door "+ cellpath);
            _log.info("Xrootd mover listening on port: " + serverPort);

            //
            // awaiting connection from client
            //
            xrootdServer.setSoTimeout(LISTEN_TIMEOUT);

            return xrootdServer.accept();
        } catch (SocketTimeoutException e) {
            throw new CacheException("xrootd transfer failed, mover serversocket timed out after listening for "+LISTEN_TIMEOUT/1000+" secconds. ");
        } finally {
            xrootdServer.close();
        }
    }

    public void setAttribute(String name, Object attribute) {}

    public Object getAttribute(String name) {
        throw new
            IllegalArgumentException( "Couldn't find "+name ) ;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    public long getTransferTime() {
        return transferTime;
    }

    public void setTransferTime(long transferTime) {
        this.transferTime = transferTime;
    }

    public long getLastTransferred()
    {
        return lastTransferred;
    }

    public void setLastTransferred()
    {
        lastTransferred = System.currentTimeMillis();
    }

    public boolean wasChanged()
    {
        return false;
    }

    RandomAccessFile getDiskFile()
    {
        return diskFile;
    }

    PnfsId getPnfsId()
    {
        return pnfsId;
    }

    XrootdProtocolInfo getProtocolInfo()
    {
        return protocolInfo;
    }

    Allocator getAllocator()
    {
        return allocator;
    }

    StorageInfo getStorageInfo()
    {
        return storageInfo;
    }


    void setTransferFinished()
    {
        transferFinishedSync.countDown();
    }

    int getXrootdFileHandle()
    {
        return xrootdFileHandle;
    }

    void setTransferSuccessful()
    {
        transferSuccessful = true;
    }

    boolean isTransferSuccessful()
    {
        return transferSuccessful;
    }

    long getOpenChecksum()
    {
        return openChecksum;
    }
}
