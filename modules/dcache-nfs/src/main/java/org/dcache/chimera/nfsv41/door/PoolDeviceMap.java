package org.dcache.chimera.nfsv41.door;

import com.google.common.collect.ImmutableSet;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.chimera.nfsv41.door.NFSv41Door.PoolDS;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.LayoutDriver;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.util.Bytes;

/**
 * A mapping between pool name, nfs device id and pool's ip addresses.
 */
public class PoolDeviceMap {

    /**
     * next device id, 0 reserved for MDS
     */
    private final AtomicInteger _nextDeviceID = new AtomicInteger(1);

    /**
     * layout specific driver.
     */
    private final LayoutDriver _layoutDriver;

    /**
     * dCache-friendly NFS device id to pool name mapping
     */
    private final Map<String, PoolDS> _poolNameToIpMap = new HashMap<>();

    /**
     * All known devices
     */
    private final Map<deviceid4, PoolDS> _deviceMap = new HashMap<>();

    /*
     * as there are mostly reads, use read-write locks to increase concurrency
     */
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock _rlock = _lock.readLock();
    private final ReentrantReadWriteLock.WriteLock _wlock = _lock.writeLock();

    public PoolDeviceMap(LayoutDriver layoutDriver) {
        _layoutDriver = layoutDriver;
    }

    static deviceid4 deviceidOf(int id) {
        byte[] deviceidBytes = new byte[nfs4_prot.NFS4_DEVICEID4_SIZE];
        Bytes.putInt(deviceidBytes, 0, id);

        return new deviceid4(deviceidBytes);
    }

    Collection<PoolDS> getDevices() {
        _rlock.lock();
        try {
            return ImmutableSet.copyOf(_poolNameToIpMap.values());
        } finally {
            _rlock.unlock();
        }
    }

    Collection<deviceid4> getDeviceIds() {
        _rlock.lock();
        try {
            return ImmutableSet.copyOf(_deviceMap.keySet());
        } finally {
            _rlock.unlock();
        }
    }

    PoolDS getOrCreateDS(String name, long verifier, InetSocketAddress[] poolAddress) {
        _wlock.lock();
        try {

            PoolDS ds = _poolNameToIpMap.get(name);
            if (ds != null && ds.getVerifier() == verifier) {
                return ds;
            }

            if (ds != null) {
                // remove old mapping
                _deviceMap.remove(ds.getDeviceId());
            }
            deviceid4 deviceid = deviceidOf(_nextDeviceID.incrementAndGet());
            ds = new PoolDS(deviceid, _layoutDriver.getDeviceAddress(poolAddress), poolAddress, verifier);
            _poolNameToIpMap.put(name, ds);
            _deviceMap.put(ds.getDeviceId(), ds);
            return ds;

        } catch(ChimeraNFSException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            _wlock.unlock();
        }
    }

    PoolDS getByDeviceId(deviceid4 deviceId) {
        _rlock.lock();
        try {
            return _deviceMap.get(deviceId);
        } finally {
            _rlock.unlock();
        }
    }

    Collection<Map.Entry<String,PoolDS>> getEntries() {
        _rlock.lock();
        try {
            return ImmutableSet.copyOf(_poolNameToIpMap.entrySet());
        } finally {
            _rlock.unlock();
        }
    }

    PoolDS remove(String pool) {
        _wlock.lock();
        try {
            return _poolNameToIpMap.remove(pool);
        } finally {
            _wlock.unlock();
        }
    }
}
