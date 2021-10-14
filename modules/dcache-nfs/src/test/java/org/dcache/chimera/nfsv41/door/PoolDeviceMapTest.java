package org.dcache.chimera.nfsv41.door;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.junit.Before;
import org.junit.Test;

public class PoolDeviceMapTest {

    private PoolDeviceMap _poolDeviceMap;

    @Before
    public void setUp() {
        _poolDeviceMap = new PoolDeviceMap();
    }

    @Test
    public void testGetMissingById() {
        deviceid4 id = PoolDeviceMap.deviceidOf(1);
        NFSv41Door.PoolDS ds = _poolDeviceMap.getByDeviceId(id);
        assertNull("Got not existing", ds);
    }

    @Test
    public void testGetExisting() throws UnknownHostException {
        String name = "somePool";
        InetSocketAddress[] ip = new InetSocketAddress[]{new InetSocketAddress(0)};
        long verivier = 0;

        NFSv41Door.PoolDS ds = _poolDeviceMap.getOrCreateDS(name, verivier, ip);
        ds = _poolDeviceMap.getByDeviceId(ds.getDeviceId());
        assertNotNull("Can't get existing by id", ds);
    }

    @Test
    public void testUpdateExisting() throws UnknownHostException {
        String name = "somePool";
        InetSocketAddress[] ip = new InetSocketAddress[]{new InetSocketAddress(0)};

        NFSv41Door.PoolDS ds = _poolDeviceMap.getOrCreateDS(name, 0, ip);
        assertSame(ds, _poolDeviceMap.getOrCreateDS(name, 0, ip));
    }

    @Test
    public void testRemoveExisting() throws UnknownHostException {
        String name = "somePool";
        InetSocketAddress[] ip = new InetSocketAddress[]{new InetSocketAddress(0)};

        NFSv41Door.PoolDS ds = _poolDeviceMap.getOrCreateDS(name, 0, ip);
        _poolDeviceMap.remove(name);
        assertNull("Removed pool stil available", _poolDeviceMap.getByDeviceId(ds.getDeviceId()));
    }
}
