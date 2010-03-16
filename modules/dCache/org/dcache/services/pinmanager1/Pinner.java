package org.dcache.services.pinmanager1;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;
import org.dcache.auth.Subjects;
import org.dcache.auth.AuthorizationRecord;

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
    private final PinManagerJob _job;
    private final PinnerContext _fsm;
    private final CellPath _pnfsManager;
    private final CellPath _poolManager;
    private String _readPoolName;
    private long _expiration;

    /**
     * Pool manager allowed stated for this request.
     */
    private final int _allowedStates;
    // We record this now in Pinner, since upon success
    // we need to change the original pin expiration time
    // which will be calcuated only after the file was
    // succesfully staged or otherwise made available and
    // the read pool is given to us
    private long _orginalPinRequestId;

    public Pinner(PinManager manager,
        PinManagerJob job,
        Pin pin,
        long orginalPinRequestId,
        int allowedStates)
    {
        super(manager);
        _job = job;
        _pnfsManager = manager.getPnfsManager();
        _poolManager = manager.getPoolManager();
        _pin = pin;
        _orginalPinRequestId = orginalPinRequestId;
        _allowedStates = allowedStates;
        _fsm = new PinnerContext(this);
        setContext(_fsm);
        synchronized (this) {
            _fsm.go();
        }
        job.setSMCTask(this);

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
        return _job.getStorageInfo();
    }

    public void setStorageInfo(StorageInfo info)
    {
        info("setStorageInfo");
        _job.setStorageInfo(info);
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
                    new PnfsGetStorageInfoMessage(_job.getPnfsId()),
                    5 * 60 * 1000);
    }

    void findReadPool()
    {
        String host = _job.getClientHost() == null ?
            "localhost":
            _job.getClientHost();
        info("findReadPool for "+_job.getPnfsId()+" host="+host);
        DCapProtocolInfo pinfo =
            new DCapProtocolInfo("DCap", 3, 0, host, 0);

        PoolMgrSelectReadPoolMsg request =
            new PoolMgrSelectReadPoolMsg(_job.getPnfsId(),
                                         _job.getStorageInfo(),
                                         pinfo,
                                         0,
                                         _allowedStates);
        request.setSubject(_job.getSubject());

        sendMessage(_poolManager, request, 60*60*1000);
    }

    void markSticky()
    {
        if(_job.getLifetime() == -1) {
            _expiration = -1;
        } else {
            _expiration = System.currentTimeMillis() +_job.getLifetime();
        }
        info("markSticky "+_job.getPnfsId()+" in pool "+_readPoolName+
               ( _expiration==-1?" forever":" until epoc "+_expiration));
        long stickyBitExpiration = _expiration;
        if(stickyBitExpiration > 0) {
            stickyBitExpiration += PinManager.POOL_LIFETIME_MARGIN;
        }
        PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(_readPoolName,
            _job.getPnfsId(),
            true,
            //Use a pin specific name, so multiple pins of the same file
            // by the pin manager would be possible
            // needed if the unpinning is started and new pin request
            // has arrived
            getCellName()+_pin.getId(),
            stickyBitExpiration);
        sendMessage(new CellPath(_readPoolName), setStickyRequest,
                    90 * 1000);
    }

    void succeed()
    {
        info("succeed, file pinned in "+_readPoolName);
        try {
            getManager().pinSucceeded(_pin,
                _readPoolName,
                _expiration,
                _orginalPinRequestId);
        } catch (PinException pe) {
            error(pe.toString());
        }
        //_pin.pinSucceeded();
    }

    void fail(Object reason)
    {
        error("failed: "+reason);
        try {
            getManager().pinFailed(_pin,reason);
        } catch (PinException pe) {
           error(pe.toString());
        }
       // _pin.pinFailed(reason);
    }
}
