package org.dcache.services.pinmanager1;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import java.util.List;
import java.util.ArrayList;

/**
 * Background task used to perform the actual unpinning operation.
 *
 * It is spawned by a Pin instance, and will do a callback to that pin
 * when done.
 */
class Unpinner extends SMCTask
{
    protected final PnfsId _pnfsId;
    protected final Pin _pin;
    protected final UnpinnerContext _fsm;
    protected final CellPath _pnfsManager;
    protected  final List<String> locations = new ArrayList<String>();
    protected final boolean isOldStylePin;
    public Unpinner(PinManager manager, PnfsId pnfsId, Pin pin)
    {
        super(manager);

        _pnfsId = pnfsId;
        _pin = pin;
        _pnfsManager = manager.getPnfsManager();
        String pool = _pin.getPool();
        isOldStylePin = pool == null || pool.equals("unknown");
        if(!isOldStylePin) {
            locations.add(pool);
        }
        
        _fsm = new UnpinnerContext(this);
        setContext(_fsm);
        _fsm.go();
        info("Unpinner constructor done, isOldStylePin="+isOldStylePin);
    }

    private void info(String s) {
        getManager().info("Unpinner: "+s);
    }

    private void error(String s) {
        getManager().error("Unpinner: "+s);
    }

    private PinManager getManager() {
        return (PinManager)_cell;
    }
    boolean isOldStylePin() {
        return isOldStylePin;
    }
    
    public String toString()
    {
        return _fsm.getState().toString();
    }

    void fail(Object reason)
    {
        error(" failed: "+reason);
        //_pin.unpinFailed(reason);
        try {
            getManager().unpinFailed(_pin);
        } catch (PinException pe) {
            error(pe.toString());
        }
    }

    void succeed()
    {
        info("succeeded");
        try {
            getManager().unpinSucceeded(_pin);
        } catch (PinException pe) {
            error(pe.toString());
        }
        
        //_pin.unpinSucceeded();
    }

    void deletePnfsFlags()
    {
        info("deletePnfsFlags");
        PnfsFlagMessage pfm = new PnfsFlagMessage(_pnfsId, "s", "delete");
        pfm.setValue("*");
        pfm.setReplyRequired(true);
        sendMessage(_pnfsManager, pfm, 60*60*1000);
    }

    void findCacheLocations()
    {
        info("findCacheLocations");
        PnfsGetCacheLocationsMessage request =
            new PnfsGetCacheLocationsMessage(_pnfsId);
        sendMessage(_pnfsManager, request, 60*60*1000);
    }

    void setLocations(List<String> locations) {
        info("setLocations");
        this.locations.addAll(locations);
    }
    
    void unsetStickyFlags()
    {
        for (String poolName: locations) {
            String stickyBitName = getCellName()+
                    Long.toString(_pin.getId());
            String oldStickyBitName = getCellName();
            info("unsetStickyFlags in "+poolName+" for "+
                _pnfsId+" stikyBitNameName:"+stickyBitName);

            PoolSetStickyMessage setStickyRequest = 
                new PoolSetStickyMessage(poolName,
                _pnfsId, false,stickyBitName,-1);
            if(!isOldStylePin) {
                setStickyRequest.setReplyRequired(true);
                sendMessage(new CellPath(poolName), setStickyRequest,60*60*1000);
            } else {
                try {
                    // unpin using new format 
                    sendMessage(new CellPath(poolName), setStickyRequest);
                    setStickyRequest = 
                        new PoolSetStickyMessage(poolName,
                            _pnfsId, false,oldStickyBitName,-1);
                    // unpin using old format 
                    sendMessage(new CellPath(poolName), setStickyRequest);
                } catch (NoRouteToCellException e) {
                    error("PoolSetStickyMessage (false) failed : " + e.getMessage());
                }
            }
        }
        if(isOldStylePin) {
            _fsm.unsetStickyFlagMessagesSent();
        }
    }
}
