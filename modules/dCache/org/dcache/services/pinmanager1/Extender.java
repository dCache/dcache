package org.dcache.services.pinmanager1;

import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;
import org.dcache.cells.CellStub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task used to perform the actual unpinning operation.
 *
 * It is spawned by a Pin instance, and will do a callback to that pin
 * when done.
 */
class Extender extends SMCTask
{
    private static final Logger _logger =
        LoggerFactory.getLogger(Extender.class);

    protected final PinRequest _pinRequest;
    protected final Pin _pin;
    private final PinManagerJob _job;
    protected final ExtenderContext _fsm;
    protected final long _expiration;
    protected final PinManager _manager;
    protected final CellStub _pool;

    public Extender(PinManager manager,
                    Pin pin,
                    PinRequest pinRequest,
                    PinManagerJob job,
                    long expiration,
                    CellStub pool)
    {
        super(manager.getCellEndpoint());

        _manager = manager;
        _pin = pin;
        _pinRequest = pinRequest;
        _pool = pool;
        _expiration = expiration;
        _job = job;
        _fsm = new ExtenderContext(this);
        setContext(_fsm);
        synchronized (this) {
            _fsm.go();
        }
        job.setSMCTask(this);
        _logger.info("Extender constructor done ");
    }

    @Override
    public String toString()
    {
        return _fsm.getState().toString();
    }

    void fail(Object reason)
    {
        _logger.error(" failed: "+reason);
        //_pin.unpinFailed(reason);
        try {
            _manager.extendFailed(_pin,
                    _pinRequest,
                    _job,
                    reason);
        } catch (PinException pe) {
            _logger.error(pe.toString());
        }
    }

    void succeed()
    {
        _logger.info("succeeded");
        try {
            _manager.extendSucceeded(_pin,
                _pinRequest,
                _job,
                _expiration);
        } catch (PinException pe) {
            _logger.error(pe.toString());
        }

        //_pin.unpinSucceeded();
    }

    String getPool() {
        return _pin.getPool();
    }
    void extendStickyFlagLifetime()
    {
        String poolName = _pin.getPool();
            String stickyBitName = getCellName()+
                               Long.toString(_pin.getId());
           _logger.info("extend sticky flag  in "+poolName+" for "+
                _pin.getPnfsId()+" stickyBitNameName:"+stickyBitName);

            PoolSetStickyMessage setStickyRequest =
                new PoolSetStickyMessage(poolName,
                _pin.getPnfsId(), true,stickyBitName,_expiration);
            send(_pool, setStickyRequest, poolName);
    }
}
