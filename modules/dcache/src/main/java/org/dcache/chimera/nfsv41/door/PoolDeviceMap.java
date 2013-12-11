package org.dcache.chimera.nfsv41.door;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.chimera.nfsv41.door.NFSv41Door.PoolDS;

/**
 * A mapping between pool name, nfs device id and pool's ip addresses.
 */
public class PoolDeviceMap {

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

    void add(String poolName, PoolDS device) {
        _wlock.lock();
        try {
            PoolDS oldDevice = _poolNameToIpMap.put(poolName, device);
            // remove dead entry
            if (oldDevice != null) {
                _deviceMap.remove(oldDevice.getDeviceId());
            }
            _deviceMap.put(device.getDeviceId(), device);
        } finally {
            _wlock.unlock();
        }
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

    synchronized PoolDS getByPoolName(String name) {
        _rlock.lock();
        try {
            return _poolNameToIpMap.get(name);
        } finally {
            _rlock.unlock();
        }
    }

    synchronized PoolDS getByDeviceId(deviceid4 deviceId) {
        _rlock.lock();
        try {
            return _deviceMap.get(deviceId);
        } finally {
            _rlock.unlock();
        }
    }

    synchronized Collection<Map.Entry<String,PoolDS>> getEntries() {
        _rlock.lock();
        try {
            return ImmutableSet.copyOf(_poolNameToIpMap.entrySet());
        } finally {
            _rlock.unlock();
        }
    }
}
