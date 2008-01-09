package org.dcache.services.pinmanager1;

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
    private final Pin _pin;
    private final PnfsId _pnfsId;
    private final PinnerContext _fsm;
    private final CellPath _pnfsManager;
    private final CellPath _poolManager;
    private StorageInfo _storageInfo;
    private String _readPoolName;
    private long _expiration;

    public Pinner(PinManager manager,
                  PnfsId pnfsId, StorageInfo storageInfo, Pin pin,
        long expiration)
    {
        super(manager);

        _pnfsManager = manager.getPnfsManager();
        _poolManager = manager.getPoolManager();
        _pnfsId = pnfsId;
        _storageInfo = storageInfo;
        _pin = pin;
        _expiration = expiration;
        _fsm = new PinnerContext(this);
        setContext(_fsm);
        _fsm.go();
        info("Pinner constructor done");
    }

    private PinManager getManager() {
        return (PinManager)_cell;
    }
    
    private void info(String s) {
        getManager().info("Pinner: "+s);
    }
    
    private void error(String s) {
        getManager().error("Pinner: "+s);
    }
    
    /** Returns the current state of the pinner. */
    public String toString()
    {
        return _fsm.getState().toString();
    }

    public StorageInfo getStorageInfo()
    {
        info("getStorageInfo");
        return _storageInfo;
    }

    public void setStorageInfo(StorageInfo info)
    {
        info("setStorageInfo");
        _pin.setStorageInfo(info);
        _storageInfo = info;
    }

    public void setReadPool(String name)
    {
        info("setReadPool");
        _readPoolName = name;
    }

    void retrieveStorageInfo()
    {
        info("retrieveStorageInfo");
        sendMessage(_pnfsManager,
                    new PnfsGetStorageInfoMessage(_pnfsId),
                    60 * 60 * 1000);
    }

    void findReadPool()
    {
        info("findReadPool");
        DCapProtocolInfo pinfo =
            new DCapProtocolInfo("DCap", 3, 0, "localhost", 0);

        PoolMgrSelectReadPoolMsg request =
            new PoolMgrSelectReadPoolMsg(_pnfsId,
                                         _storageInfo,
                                         pinfo,
                                         0);

        sendMessage(_poolManager, request, 1*24*60*60*1000);
    }

    void markSticky()
    {
        info("markSticky");
        long stikyBitExpiration = _expiration;
        if(stikyBitExpiration > 0) {
            stikyBitExpiration += PinManager.POOL_LIFETIME_MARGIN;
        }
        PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(_readPoolName,
            _pnfsId, 
            true,
            //Use a pin specific name, so multiple pins of the same file 
            // by the pin manager would be possible
            // needed if the unpinning is started and new pin request 
            // has arrived
            getCellName()+_pin.getId(), 
            stikyBitExpiration);
        sendMessage(new CellPath(_readPoolName), setStickyRequest,
                    1 * 24 * 60 * 60 * 1000);
    }

    void succeed()
    {
        info("succeed");
        try {
            getManager().pinSucceeded(_pin,_readPoolName);
        } catch (PinException pe) {
            error(pe.toString());
        }
        //_pin.pinSucceeded();
    }

    void fail(Object reason)
    {
        error("failed: "+reason);
        try {
            getManager().pinFailed(_pin);
        } catch (PinException pe) {
           error(pe.toString());
        }
       // _pin.pinFailed(reason);
    }
}
