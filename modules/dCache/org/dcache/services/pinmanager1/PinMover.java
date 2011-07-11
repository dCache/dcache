package org.dcache.services.pinmanager1;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;

/**
 * Background task used to perform the actual pinning operation. To do
 * this, it will talk to the PNFS manager, pool manager and pools.
 *
 * It is spawned by a Pin instance, and will do a callback to that pin
 * when done.
 */
class PinMover extends SMCTask
{
    private final Pin _srcPoolPin;
    private final Pin _dstPoolPin;
    private final PnfsId _pnfsId;
    private final PinMoverContext _fsm;
    private final CellPath _pnfsManager;
    private final CellPath _poolManager;
    private String _dstPoolName;
    private long _expiration;
    private PinManagerMovePinMessage _movePin;
    protected final CellMessage _envelope ;
    // We record this now in Pinner, since upon success
    // we need to change the original pin expiration time
    // which will be calcuated only after the file was
    // succesfully staged or otherwise made available and
    // the read pool is given to us
    private long _orginalPinRequestId;

    public PinMover(PinManager manager,
        PnfsId pnfsId,
        Pin srcPoolPin,
        Pin dstPoolPin,
        String dstPoolName,
        long expiration,
        PinManagerMovePinMessage movePin,
        CellMessage envelope)
    {
        super(manager);

        _pnfsManager = manager.getPnfsManager();
        _poolManager = manager.getPoolManager();
        _pnfsId = pnfsId;
        _dstPoolName = dstPoolName;
        _srcPoolPin = srcPoolPin;
        _dstPoolPin = dstPoolPin;
        _expiration = expiration;
        _movePin = movePin;
         _envelope = envelope;
        _fsm = new PinMoverContext(this);
        setContext(_fsm);
        info("PinMover constructor done");
    }

    public void start()
    {
        _fsm.go();
    }

    private PinManager getManager() {
        return (PinManager)_cell;
    }

    private void debug(String s) {
        getManager().debug("PinMover: "+s);
    }

    private void info(String s) {
        getManager().info("PinMover: "+s);
    }

    private void error(String s) {
        getManager().error("PinMover: "+s);
    }

    /** Returns the current state of the pinner. */
    public String toString()
    {
        return _fsm.getState().toString();
    }


    void markSticky()
    {
        info("markSticky");
        long stickyBitExpiration = _expiration;
        if(stickyBitExpiration > 0) {
            stickyBitExpiration += PinManager.POOL_LIFETIME_MARGIN;
        }
        PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(_dstPoolName,
            _pnfsId,
            true,
            //Use a pin specific name, so multiple pins of the same file
            // by the pin manager would be possible
            // needed if the unpinning is started and new pin request
            // has arrived
            getCellName()+_dstPoolPin.getId(),
            stickyBitExpiration);
        sendMessage(new CellPath(_dstPoolName), setStickyRequest,
                    90 * 1000);
    }

    void unsetStickyFlags()
    {
        String stickyBitName = getCellName()+
                Long.toString(_srcPoolPin.getId());
        String oldStickyBitName = getCellName();
        String srcPoolName = _srcPoolPin.getPool();
        info("unsetStickyFlags in "+srcPoolName+" for "+
            _pnfsId+" stickyBitNameName:"+stickyBitName);

        PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(srcPoolName,
            _pnfsId, false,stickyBitName,-1);
            setStickyRequest.setReplyRequired(true);
            sendMessage(new CellPath(srcPoolName), setStickyRequest,90*1000);
    }
    boolean pinMoveSucceed()
    {
        debug("pinMoveSucceed");
        try {
            return getManager().pinMoveToNewPoolPinSucceeded(
                _srcPoolPin,
                _dstPoolPin,
                _dstPoolName,
                _expiration,
                _movePin,
                _envelope);
        } catch (PinException pe) {
            error(pe.toString());
            return false;
        }
        //_pin.pinSucceeded();
    }


    void succeed()
    {
        info("succeed");
        try {
            getManager().pinMoveSucceeded(
                _srcPoolPin,
                _dstPoolPin,
                _dstPoolName,
                _expiration,
                _movePin,
                _envelope);
        } catch (PinException pe) {
            error(pe.toString());
        }
        //_pin.pinSucceeded();
    }

    void fail(Object reason)
    {
        error("failed: "+reason);
        try {
            getManager().pinMoveFailed(                _srcPoolPin,
                _dstPoolPin,
                _dstPoolName,
                _expiration,
                _movePin,
                _envelope,
                reason);

        } catch (PinException pe) {
           error(pe.toString());
        }
       // _pin.pinFailed(reason);
    }
}
