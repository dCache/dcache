package org.dcache.services.pinmanager;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.*;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import java.util.List;

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

    public Unpinner(PinManager manager, PnfsId pnfsId, Pin pin)
    {
        super(manager);

        _pnfsId = pnfsId;
        _pin = pin;
        _pnfsManager = manager.getPnfsManager();

        _fsm = new UnpinnerContext(this);
        setContext(_fsm);
    }

    public String toString()
    {
        return _fsm.getState().toString();
    }

    void fail(Object reason)
    {
        _pin.unpinFailed(reason);
    }

    void succeed()
    {
        _pin.unpinSucceeded();
    }

    void deletePnfsFlags()
    {
        PnfsFlagMessage pfm = new PnfsFlagMessage(_pnfsId, "s", "delete");
        pfm.setValue("*");
        pfm.setReplyRequired(true);
        sendMessage(_pnfsManager, pfm, 60*60*1000);
    }

    void findCacheLocations()
    {
        PnfsGetCacheLocationsMessage request =
            new PnfsGetCacheLocationsMessage(_pnfsId);
        sendMessage(_pnfsManager, request, 60*60*1000);
    }

    void unsetStickyFlags(List<String> locations)
    {
        for (String poolName: locations) {
            PoolSetStickyMessage setStickyRequest =
                new PoolSetStickyMessage(poolName, _pnfsId, false,getCellName(),-1);
            try {
                sendMessage(new CellPath(poolName), setStickyRequest);
            } catch (NoRouteToCellException e) {
                _cell.esay("PoolSetStickyMessage (false) failed : " + e.getMessage());
            }
        }
    }
}
