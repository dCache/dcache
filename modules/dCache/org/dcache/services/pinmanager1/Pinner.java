package org.dcache.services.pinmanager1;

import java.util.EnumSet;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.*;
import diskCacheV111.poolManager.RequestContainerV5;

import dmg.cells.nucleus.CellPath;
import org.dcache.auth.Subjects;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task used to perform the actual pinning operation. To do
 * this, it will talk to the PNFS manager, pool manager and pools.
 *
 * It is spawned by a Pin instance, and will do a callback to that pin
 * when done.
 */
class Pinner extends SMCTask
{
    private static final Logger _logger =
        LoggerFactory.getLogger(SMCTask.class);

    private final Pin _pin;
    private final PinManagerJob<PinManagerPinMessage> _job;
    private final PinnerContext _fsm;
    private final CellStub _pnfsManager;
    private final CellStub _poolManager;
    private final CellStub _pool;
    private String _readPoolName;
    private long _expiration;
    private final PinManager _manager;
    private FileAttributes _fileAttributes;

    /**
     * Pool manager allowed stated for this request.
     */
    private final EnumSet<RequestContainerV5.RequestState> _allowedStates;
    // We record this now in Pinner, since upon success
    // we need to change the original pin expiration time
    // which will be calcuated only after the file was
    // succesfully staged or otherwise made available and
    // the read pool is given to us
    private long _orginalPinRequestId;

    public Pinner(PinManager manager,
                  PinManagerJob<PinManagerPinMessage> job,
                  Pin pin,
                  long orginalPinRequestId,
                  EnumSet<RequestContainerV5.RequestState> allowedStates,
                  CellStub pnfsManager,
                  CellStub poolManager,
                  CellStub pool)
    {
        super(manager.getCellEndpoint());
        _manager = manager;
        _job = job;
        _pnfsManager = pnfsManager;
        _poolManager = poolManager;
        _pool = pool;
        _pin = pin;
        _orginalPinRequestId = orginalPinRequestId;
        _allowedStates = allowedStates;
        _fsm = new PinnerContext(this);

        if (job.getMessage() != null) {
            _fileAttributes = job.getMessage().getFileAttributes();
        }

        setContext(_fsm);
        synchronized (this) {
            _fsm.go();
        }
        job.setSMCTask(this);

        _logger.info("Pinner constructor done");
    }

    /** Returns the current state of the pinner. */
    public String toString()
    {
        return _fsm.getState().toString();
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public void setFileAttributes(FileAttributes fileAttributes)
    {
        _fileAttributes = fileAttributes;
    }

    public void setReadPool(String name)
    {
        _logger.info("setReadPool");
        _readPoolName = name;
    }

    void retrieveFileAttributes()
    {
        _logger.info("retrieveStorageInfo");
        PnfsGetFileAttributes msg =
            new PnfsGetFileAttributes(_job.getPnfsId(),
                                      PoolMgrSelectReadPoolMsg.getRequiredAttributes());
        send(_pnfsManager, msg);
    }

    private ProtocolInfo getProtocolInfo()
    {
        if (_job.getMessage() != null) {
            return _job.getMessage().getProtocolInfo();
        }

        DCapProtocolInfo pinfo =
            new DCapProtocolInfo("DCap", 3, 0, "localhost", 0);
        return pinfo;
    }

    void findReadPool()
    {
        PoolMgrSelectReadPoolMsg request =
            new PoolMgrSelectReadPoolMsg(getFileAttributes(),
                                         getProtocolInfo(),
                                         0,
                                         _allowedStates);
        request.setSubject(_job.getSubject());

        send(_poolManager, request);
    }

    void markSticky()
    {
        if(_job.getLifetime() == -1) {
            _expiration = -1;
        } else {
            _expiration = System.currentTimeMillis() +_job.getLifetime();
        }
        _logger.info("markSticky "+_job.getPnfsId()+" in pool "+_readPoolName+
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
        send(_pool, setStickyRequest, _readPoolName);
    }

    void succeed()
    {
        _logger.info("succeed, file pinned in "+_readPoolName);
        try {
            _manager.pinSucceeded(_pin,
                _readPoolName,
                _expiration,
                _orginalPinRequestId);
        } catch (PinException pe) {
            _logger.error(pe.toString());
        }
        //_pin.pinSucceeded();
    }

    void fail(Object reason)
    {
        _logger.error("failed: "+reason);
        try {
            _manager.pinFailed(_pin,reason);
        } catch (PinException pe) {
           _logger.error(pe.toString());
        }
       // _pin.pinFailed(reason);
    }
}
