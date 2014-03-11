package org.dcache.chimera.nfsv41.door;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.dcache.nfs.v4.RoundRobinStripingPattern;
import org.dcache.nfs.v4.StripingPattern;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.utils.Bytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author tigran
 */
public class PoolDeviceMapTest {

    private final StripingPattern<InetSocketAddress[]> _stripingPattern
            = new RoundRobinStripingPattern<>();

    private PoolDeviceMap _poolDeviceMap;

    @Before
    public void setUp() {
        _poolDeviceMap = new PoolDeviceMap();
    }

    @Test
    public void testGetMissingById() {
        deviceid4 id = deviceidOf(1);
        NFSv41Door.PoolDS ds = _poolDeviceMap.getByDeviceId(id);
        assertNull("Got not existing", ds);
    }

    @Test
    public void testGetMissingByName() {
        String name = "somePool";
        NFSv41Door.PoolDS ds = _poolDeviceMap.getByPoolName(name);
        assertNull("Got not existing", ds);
    }

    @Test
    public void testGetExisting() throws UnknownHostException {
        String name = "somePool";
        deviceid4 id1 = deviceidOf(1);
        InetSocketAddress[] ip = new InetSocketAddress[]{new InetSocketAddress(0)};

        NFSv41Door.PoolDS ds1 = new NFSv41Door.PoolDS(id1, _stripingPattern, ip, 0);

        _poolDeviceMap.add(name, ds1);
        NFSv41Door.PoolDS ds;
        ds = _poolDeviceMap.getByDeviceId(id1);
        assertNotNull("Can't get existing by id", ds);

        ds = _poolDeviceMap.getByPoolName(name);
        assertNotNull("Can't get existing by name", ds);
    }

    @Test
    public void testUpdateExisting() throws UnknownHostException {
        String name = "somePool";
        deviceid4 id1 = deviceidOf(1);
        deviceid4 id2 = deviceidOf(2);
        InetSocketAddress[] ip = new InetSocketAddress[]{new InetSocketAddress(0)};

        NFSv41Door.PoolDS ds1 = new NFSv41Door.PoolDS(id1, _stripingPattern, ip, 0);
        NFSv41Door.PoolDS ds2 = new NFSv41Door.PoolDS(id2, _stripingPattern, ip, 0);

        _poolDeviceMap.add(name, ds1);
        _poolDeviceMap.add(name, ds2);

        NFSv41Door.PoolDS ds;
        ds = _poolDeviceMap.getByDeviceId(id1);
        assertNull("Update did not invalidate old id", ds);

        ds = _poolDeviceMap.getByPoolName(name);
        assertNotNull("Can't get updated by name", ds);
    }

    private static deviceid4 deviceidOf(int id) {
        byte[] deviceidBytes = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];
        Bytes.putInt(deviceidBytes, 0, id);

        return new deviceid4(deviceidBytes);
    }
}
