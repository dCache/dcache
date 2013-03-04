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

import java.io.IOException;
import java.util.List;

import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.v4.xdr.device_addr4;
import org.dcache.chimera.nfs.v4.xdr.deviceid4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;

public interface NFSv41DeviceManager {

    /**
     * Get a file {@link Layout}. An state is associated with the returned layout.
     * The layout is considered to be in use until <code>layoutReturn</code>
     * method is not called.
     *
     * @param inode
     * @param ioMode the value of the returned layout iomode. A ioMode of either
     *      LAYOUTIOMODE4_READ or LAYOUTIOMODE4_RW MUST be returned.
     * @param client associated with LAYOUTGET call
     * @param stateid reflecting the corresponding valid open, byte-range lock,
     *      or delegation stateid.
     * @return
     * @throws IOException
     */
    public Layout layoutGet(FsInode inode, int ioMode, NFS4Client client, stateid4 stateid)
            throws IOException;

    /**
     * Get storage device address information for the specified device {@link deviceid4}.
     *
     * @param client
     * @param deviceId
     * @return device address
     */
    public device_addr4 getDeviceInfo(NFS4Client client, deviceid4 deviceId) throws IOException;

    /**
     * Get list of all device IDs used bu server.
     *
     * @param client
     * @return list of devices
     */
    public List<deviceid4> getDeviceList(NFS4Client client) throws IOException;

    /**
     * Release layout associated with state id.
     *
     * @param client
     * @param stateid
     */
    public void layoutReturn(NFS4Client client, stateid4 stateid) throws IOException;
}
