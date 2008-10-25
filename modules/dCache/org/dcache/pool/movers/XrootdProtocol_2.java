package org.dcache.pool.movers;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;

import org.apache.log4j.Logger;

import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.XrootdProtocol;

import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.movers.NetIFContainer;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

public class XrootdProtocol_2 implements MoverProtocol {

    private static final Logger _log = Logger.getLogger(XrootdProtocol_2.class);

    private static final int[] DEFAULT_PORTRANGE = {20000, 25000};
    private static final int LISTEN_TIMEOUT = 10 * 60 * 1000;

    private final CellEndpoint cell;
    private RandomAccessFile diskFile;
    private ProtocolInfo protocolInfo;
    private StorageInfo storageInfo;
    private PnfsId pnfsId;
    private SpaceMonitor spaceMonitor;
    private long lastTransferred;
    private long transferTime;
    private long bytesTransferred;
    private Object transferFinishedSync = new Object();
    private boolean transferSuccessful = false;
    private PhysicalXrootdConnection physicalXrootdConnection;
    private XrootdMoverController controller;
    private int xrootdFileHandle;

    //	this checksum comes from the door and is compared to the calculated one when the reopen is received
    //	to make sure that the OpenRequest has not changed meanwhile
    private long openChecksum;

    private static Object portSyncer = new Object();
    private static int lastMoverPort = -1;


    public XrootdProtocol_2(CellEndpoint cell) {
        this.cell = cell;
        _log.info("Xrootd mover is started.");
    }


    public void runIO(RandomAccessFile diskFile, ProtocolInfo protocol, StorageInfo storage, PnfsId pnfsId, SpaceMonitor spaceMonitor, int access) throws Exception {

        this.diskFile = diskFile;
        this.protocolInfo = protocol;
        this.storageInfo = storage;
        this.pnfsId = pnfsId;
        this.spaceMonitor = spaceMonitor;


        //
        //		scan for and try to bind an unused server port
        //
        String portRange = System.getProperty("org.dcache.net.tcp.portrange");
        int firstPort, lastPort;

        if (portRange != null) {
            String[] range = portRange.split(":");
            try {
                firstPort = Integer.parseInt(range[0]);
                lastPort = Integer.parseInt(range[1]);
            }catch(NumberFormatException nfe) {
                firstPort = lastPort = 0;
            }

        } else {
            firstPort = DEFAULT_PORTRANGE[0];
            lastPort = DEFAULT_PORTRANGE[1];
        }

        if( firstPort >= lastPort ) {
            throw new SocketException("xrootd portrange not large enough");
        }

        //      find free port or raise execption if no unused port can be found (end of mover)
        ServerSocket xrootdServer = createServerSocket(firstPort, lastPort);
        int serverPort = xrootdServer.getLocalPort();


        //
        //		look for all network interfaces
        //
        XrootdProtocolInfo xrootdProtocol = (XrootdProtocolInfo) protocol;
        this.xrootdFileHandle = xrootdProtocol.getXrootdFileHandle();
        this.openChecksum = xrootdProtocol.getChecksum();


        Collection netifsCol = new ArrayList();

        //		try to pick the ip address with corresponds to the hostname (which is hopefully visible to the world)
        InetAddress localIP = InetAddress.getLocalHost();

        if (localIP != null && !localIP.isLoopbackAddress() && localIP instanceof Inet4Address) {
            //			the ip we got from the hostname is at least not 127.0.01 and from the IP4-family
            Collection col = new ArrayList(1);
            col.add(localIP);
            netifsCol.add(new NetIFContainer("", col));
            _log.error("sending ip-address derived from hostname to Xrootd-door: "+localIP+" port: "+serverPort);
        } else {
            //			the ip we got from the hostname seems to be bad, let's loop through the network interfaces
            Enumeration ifList = NetworkInterface.getNetworkInterfaces();

            while (ifList.hasMoreElements()) {
                NetworkInterface netif = (NetworkInterface) ifList
                    .nextElement();

                Enumeration ips = netif.getInetAddresses();
                Collection ipsCol = new ArrayList(2);

                while (ips.hasMoreElements()) {
                    InetAddress addr = (InetAddress) ips.nextElement();

                    //					check again each ip from each interface.
                    //					WARNING: multiple ip addresses in case of multiple ifs could be selected,
                    //					we can't determine the "correct" one
                    if (addr instanceof Inet4Address
                        && !addr.isLoopbackAddress()) {
                        ipsCol.add(addr);
                        _log.error("sending ip-address derived from network-if to Xrootd-door: "+addr+" port: "+serverPort);
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
        // 		send message back to the door, containing the new serverport and ip
        //
        CellPath cellpath = xrootdProtocol.getXrootdDoorCellPath();
        XrootdDoorAdressInfoMessage doorMsg = new XrootdDoorAdressInfoMessage(getXrootdFileHandle(), serverPort, netifsCol);
        cell.sendMessage (new CellMessage(cellpath, doorMsg));

        _log.info("sending redirect message to Xrootd-door "+ cellpath);
        _log.info("Xrootd mover listening on port: " + serverPort);

        //
        //	    awaiting connection from client
        //
        xrootdServer.setSoTimeout(LISTEN_TIMEOUT);

        Socket socket = null;
        try {
            socket = xrootdServer.accept();
        } catch (SocketTimeoutException e) {
            throw new CacheException("xrootd transfer failed, mover serversocket timed out after listening for "+LISTEN_TIMEOUT/1000+" secconds. ");
        } finally {
            xrootdServer.close();
        }

        _log.info("got connection attempt");

        //	    pass connection handling over to the xrootd subsystem
        initXrootd(socket);


        //	    initialise counter
        setLastTransferred();

        //	    gets notfied on transfer finished by xrootd subsystem
        synchronized (transferFinishedSync) {
            transferFinishedSync.wait();
        }

        //	    end of transfer
        physicalXrootdConnection.closeConnection();

        if (!isTransferSuccessful()) {
            _log.error("xrootd transfer failed");
            throw new CacheException("xrootd transfer failed");
        }

        _log.error("normal end of xrootd mover process, transfer successful");

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

    public long getLastTransferred() {
        return lastTransferred;
    }

    public void setLastTransferred() {
        this.lastTransferred = System.currentTimeMillis();
    }

    public boolean wasChanged() {
        return false;
    }

    public RandomAccessFile getDiskFile() {
        return diskFile;
    }


    public PnfsId getPnfsId() {
        return pnfsId;
    }


    public ProtocolInfo getProtocolInfo() {
        return protocolInfo;
    }


    public SpaceMonitor getSpaceMonitor() {
        return spaceMonitor;
    }


    public StorageInfo getStorageInfo() {
        return storageInfo;
    }


    public void setTransferFinished() {

        synchronized (transferFinishedSync) {
            transferFinishedSync.notify();
        }
    }

    private void initXrootd(Socket socket) throws IOException{

        //		create new XrootdConnection based on existing socket
        physicalXrootdConnection = new PhysicalXrootdConnection(socket, XrootdProtocol.DATA_SERVER);

        //		set controller for this connection to handle login, auth and connection-specific settings
        controller = new XrootdMoverController(this, physicalXrootdConnection);
        physicalXrootdConnection.setConnectionListener(controller);

    }


    public int getXrootdFileHandle() {
        return xrootdFileHandle;
    }


    public void setTransferSuccessful() {
        this.transferSuccessful = true;
    }

    public boolean isTransferSuccessful() {
        return this.transferSuccessful;
    }

    private ServerSocket createServerSocket(int minPort, int maxPort) throws IOException {
        InetSocketAddress socketAddress;
        ServerSocket socket = new ServerSocket();

        //        synchronize access to lastMoverPort among all xrootd mover
        synchronized (portSyncer) {

            int currentPort;
            if (lastMoverPort == -1) {
                //            	initial situation, this is the first xrootd mover instance
                currentPort = lastMoverPort = minPort;
            } else {
                //            	another xrootd mover was run before, increment port
                //            	because we don't want to reuse a server port as far as possible
                //            	due to the xrootd logical channel attempt
            	currentPort = lastMoverPort;
                if (++currentPort > maxPort) {
                    currentPort = minPort;
                }
            }

            //        	loop through all ports in portrange and try binding
            while (true) {
                socketAddress =  new InetSocketAddress( currentPort ) ;
                try {
                    socket.bind(socketAddress);
                    break;
                }catch(IOException e) {
                    // port is busy, bind failed
                }

                if (++currentPort > maxPort) {
                    //        			start again from the beginning
                    currentPort = minPort;
                }

                if (currentPort == lastMoverPort) {
                    //        			at this point we've looped through all ports in the portrange without
                    //        			finding a bindable port => abort mover
                    throw new IOException("no unused port found in specified portrange, exiting");
                }
            }

            lastMoverPort = currentPort;
        }

        return socket;
    }


    public long getOpenChecksum() {
        return openChecksum;
    }
}
