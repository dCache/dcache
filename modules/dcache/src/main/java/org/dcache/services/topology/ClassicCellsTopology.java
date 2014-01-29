package org.dcache.services.topology;

import java.util.Collection;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellCommandListener;

import org.dcache.util.Args;

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
        Collection<CellDomainNode> nodes =
                buildTopologyMap(getCellDomainName()).values();
        _infoMap = nodes.toArray(new CellDomainNode[nodes.size()]);
    }

    @Override
    public CellDomainNode[] getInfoMap()
    {
        return _infoMap;
    }

    public static final String hh_update = "# initiates background update";
    public String ac_update(Args args)
    {
        Thread thread = new Thread() {
                @Override
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
