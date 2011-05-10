/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.nfs.v4.xdr.*;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrEncodingStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.utils.Bytes;
import org.dcache.utils.net.InetSocketAddresses;

/**
 *
 * the instance of this class have to ask Pool Manager for a pool and return it
 * to the client.
 *
 */

public class DeviceManager implements NFSv41DeviceManager {

    /*
     * reserved device for IO through MDS (for pnfs dot files)
     */
    private static final deviceid4 MDS_ID = deviceidOf(0);

    private static final Logger _log = LoggerFactory.getLogger(DeviceManager.class);

    /* hack for multiple pools */
    private final Random _deviceIdGenerator = new Random();

    private final Map<deviceid4, device_addr4> _deviceMap =
            new ConcurrentHashMap<deviceid4, device_addr4>();

    private InetSocketAddress[] _knownDataServers;
    private final StripingPattern<InetSocketAddress> _stripingPattern = new
            RoundRobinStripingPattern<InetSocketAddress>();

    /**
     * Set configures data servers. Each string represents a dataserver
     * as <code>IP:port</code>
     * @param servers
     */
    public void setDataservers(String[] servers) {
        _knownDataServers = new InetSocketAddress[servers.length];
        for(int i = 0; i < servers.length; i++) {
            _knownDataServers[i] = InetSocketAddresses.inetAddressOf(servers[i]);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#getIoDeviceId(org.dcache.chimera.FsInode,
     *      int, java.net.InetAddress)
     */
    @Override
    public Layout layoutGet(FsInode inode, int ioMode, NFS4Client client, stateid4 stateid)
            throws IOException {

        device_addr4 deviceAddr;
        deviceid4 deviceId;

        if (inode.type() != FsInodeType.INODE) {
            deviceId = MDS_ID;
        } else {

            if(_knownDataServers.length == 0) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_LAYOUTUNAVAILABLE,
                        "No dataservers available");
            }
            int id = _deviceIdGenerator.nextInt(256);
            ++id; /* 0 is reserved */
            deviceId = deviceidOf(id);

            _log.debug("generating new device: {} ({}) for stateid {}",
                    new Object[]{deviceId, id, stateid});

            deviceAddr = deviceAddrOf(_stripingPattern, _knownDataServers);

            _deviceMap.put(deviceId, deviceAddr);
        }

        nfs_fh4 fh = new nfs_fh4(inode.toFullString().getBytes());

        //  -1 is special value, which means entire file
        layout4 layout = Layout.getLayoutSegment(deviceId, fh, ioMode, 0, nfs4_prot.NFS4_UINT64_MAX);

        return  new Layout(true, stateid, new layout4[]{layout});
    }


    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#layoutGet(int)
     */
    @Override
    public device_addr4 getDeviceInfo(NFS4Client client, deviceid4 deviceId) {

        _log.debug("lookup for device: {}", deviceId );
        /* in case of MDS access we return the same interface which client already connected to */
        if(deviceId.equals(MDS_ID)) {
            return deviceAddrOf(_stripingPattern, client.getLocalAddress());
        }

        return  _deviceMap.get(deviceId);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#getDeviceList()
     */
    @Override
    public List<deviceid4> getDeviceList(NFS4Client client) {
        return new ArrayList<deviceid4>(_deviceMap.keySet());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.chimera.nfsv4.NFSv41DeviceManager#layoutReturn()
     */
    @Override
    public void layoutReturn(NFS4Client client, stateid4 stateid) {
        // I'am fine
        _log.debug( "release device for stateid {}", stateid );
    }

    /**
     * Create a multipath based NFSv4.1 file layout address.
     *
     * @param stripingPattern of the device
     * @param deviceAddress
     * @return device address
     */
    public static device_addr4 deviceAddrOf(StripingPattern<InetSocketAddress> stripingPattern,
            InetSocketAddress ... deviceAddress) {

        nfsv4_1_file_layout_ds_addr4 file_type = new nfsv4_1_file_layout_ds_addr4();

        file_type.nflda_multipath_ds_list = new multipath_list4[deviceAddress.length];

        for( int i = 0; i < deviceAddress.length; i++ ) {

            file_type.nflda_multipath_ds_list[i] = new multipath_list4();
            file_type.nflda_multipath_ds_list[i].value = new netaddr4[1];

            file_type.nflda_multipath_ds_list[i].value[0] = new netaddr4();
            file_type.nflda_multipath_ds_list[i].value[0].na_r_addr =
                                InetSocketAddresses.uaddrOf(deviceAddress[i]);
            file_type.nflda_multipath_ds_list[i].value[0].na_r_netid = "tcp";
        }

        file_type.nflda_stripe_indices = stripingPattern.getPattern(deviceAddress);

        XdrEncodingStream xdr = new XdrBuffer(128);
        try {
            xdr.beginEncoding();
            file_type.xdrEncode(xdr);
            xdr.endEncoding();
        }catch(OncRpcException e) {
            /* forced by interface, should never happen. */
            throw new RuntimeException("Unexpected OncRpcException:", e);
        }catch(IOException e) {
            /* forced by interface, should never happen. */
            throw new RuntimeException("Unexpected IOException:", e);
        }

        ByteBuffer body = xdr.body();
        byte[] retBytes = new byte[body.limit()] ;
        body.get(retBytes);

        device_addr4 addr = new device_addr4();
        addr.da_layout_type = layouttype4.LAYOUT4_NFSV4_1_FILES;
        addr.da_addr_body = retBytes;

        return addr;

    }

    private static deviceid4 deviceidOf(int id) {
        byte[] deviceidBytes = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];
        Bytes.putInt(deviceidBytes, 0, id);

        return new deviceid4(deviceidBytes);
    }
}
