package org.dcache.services.pinmanager;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;

/**
 * Background task used to perform the actual pinning operation. To do
 * this, it will talk to the PNFS manager, pool manager and pools.
 *
 * It is spawned by a Pin instance, and will do a callback to that pin
 * when done.
 */
class Pinner extends SMCTask
{
    private final static long POOLMANAGER_TIMEOUT = 60 * 60 * 1000; // 1 hour
    private final static long POOL_TIMEOUT = 60 * 1000; // 1 minute
    private final static long PNFS_TIMEOUT = 5 * 60 * 1000; // 5 minutes

    private final Pin _pin;
    private final PnfsId _pnfsId;
    private final PinnerContext _fsm;
    private final CellPath _pnfsManager;
    private final CellPath _poolManager;
    private StorageInfo _storageInfo;
    private String _readPoolName;
    private String _clientHost;

    public Pinner(PinManager manager,
        PnfsId pnfsId,
        String clientHost,
        StorageInfo storageInfo,
        Pin pin)
    {
        super(manager);

        _pnfsManager = manager.getPnfsManager();
        _poolManager = manager.getPoolManager();
        _pnfsId = pnfsId;
        _storageInfo = storageInfo;
        _clientHost = clientHost;
        _pin = pin;

        _fsm = new PinnerContext(this);
        setContext(_fsm);
        _fsm.go();
    }

    /** Returns the current state of the pinner. */
    public String toString()
    {
        return _fsm.getState().toString();
    }

    public StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    public void setStorageInfo(StorageInfo info)
    {
        _pin.setStorageInfo(info);
        _storageInfo = info;
    }

    public void setReadPool(String name)
    {
        _readPoolName = name;
    }

    void retrieveStorageInfo()
    {
        sendMessage(_pnfsManager,
                    new PnfsGetStorageInfoMessage(_pnfsId),
                    PNFS_TIMEOUT);
    }

    void findReadPool()
    {
        String host = _clientHost== null ?
            "localhost":
                _clientHost;
        DCapProtocolInfo pinfo =
            new DCapProtocolInfo("DCap", 3, 0, host, 0);

        PoolMgrSelectReadPoolMsg request =
            new PoolMgrSelectReadPoolMsg(_pnfsId,
                                         _storageInfo,
                                         pinfo,
                                         0);

        sendMessage(_poolManager, request, POOLMANAGER_TIMEOUT);
    }

    void markSticky()
    {
        PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(_readPoolName, _pnfsId, true,
                                     getCellName(), -1);
        sendMessage(new CellPath(_readPoolName), setStickyRequest,
                    POOL_TIMEOUT);
    }

    void succeed()
    {
        _pin.pinSucceeded();
    }

    void fail(Object reason)
    {
        _pin.pinFailed(reason);
    }
}
