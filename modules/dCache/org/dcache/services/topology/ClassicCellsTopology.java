package org.dcache.services.topology;

import dmg.cells.network.CellDomainNode;
import dmg.util.Args;

import org.dcache.cells.CellCommandListener;

/**
 * CellsTopology for dCache installation with classic (non-JMS) cells
 * communication.
 */
public class ClassicCellsTopology
    extends AbstractCellsTopology
    implements CellsTopology,
               CellCommandListener
{
    private volatile CellDomainNode[] _infoMap;

    public void update()
        throws InterruptedException
    {
        _infoMap = buildTopologyMap(getCellDomainName()).values().toArray(new CellDomainNode[0]);
    }

    @Override
    public CellDomainNode[] getInfoMap()
    {
        return _infoMap;
    }

    public final String hh_update = "# initiates background update";
    public String ac_update(Args args)
    {
        Thread thread = new Thread() {
                public void run()
                {
                    try {
                        update();
                    } catch (InterruptedException e) {
                    }
                }
            };
        thread.start();
        return "Background update started";
    }
}
