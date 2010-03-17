package org.dcache.services.pinmanager1;

import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;
/**
 * Background task used to perform the actual unpinning operation.
 *
 * It is spawned by a Pin instance, and will do a callback to that pin
 * when done.
 */
class Extender extends SMCTask
{
    protected final PinRequest _pinRequest;
    protected final Pin _pin;
    private final PinManagerJob _job;
    protected final ExtenderContext _fsm;
    protected final long _expiration;
    public Extender(PinManager manager,
        Pin pin,
        PinRequest pinRequest,
        PinManagerJob job,
        long expiration)
    {
        super(manager);

        _pin = pin;
        _pinRequest = pinRequest;

        _expiration = expiration;
        _job = job;
        _fsm = new ExtenderContext(this);
        setContext(_fsm);
        synchronized (this) {
            _fsm.go();
        }
        job.setSMCTask(this);
        info("Extender constructor done ");
    }

    private void info(String s) {
        getManager().info("Extender: "+s);
    }

    private void error(String s) {
        getManager().error("Extender: "+s);
    }

    private PinManager getManager() {
        return (PinManager)_cell;
    }

    @Override
    public String toString()
    {
        return _fsm.getState().toString();
    }

    void fail(Object reason)
    {
        error(" failed: "+reason);
        //_pin.unpinFailed(reason);
        try {
            getManager().extendFailed(_pin,
                    _pinRequest,
                    _job,
                    reason);
        } catch (PinException pe) {
            error(pe.toString());
        }
    }

    void succeed()
    {
        info("succeeded");
        try {
            getManager().extendSucceeded(_pin,
                _pinRequest,
                _job,
                _expiration);
        } catch (PinException pe) {
            error(pe.toString());
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
           info("extend sticky flag  in "+poolName+" for "+
                _pin.getPnfsId()+" stickyBitNameName:"+stickyBitName);

            PoolSetStickyMessage setStickyRequest =
                new PoolSetStickyMessage(poolName,
                _pin.getPnfsId(), true,stickyBitName,_expiration);
            setStickyRequest.setReplyRequired(true);
            sendMessage(new CellPath(poolName), setStickyRequest,90*1000);
    }
}
