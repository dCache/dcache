package org.dcache.services.topology;

import java.util.Collection;
import java.util.concurrent.Callable;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;

import dmg.cells.nucleus.CellIdentityAware;
import dmg.util.command.Command;

/**
 * CellsTopology for dCache installation with classic cells
 * communication.
 */
public class ClassicCellsTopology
    extends AbstractCellsTopology
    implements CellsTopology, CellCommandListener, CellIdentityAware
{
    private volatile CellDomainNode[] _infoMap;

    private CellAddressCore _cellAddress;

    @Override
    public void setCellAddress(CellAddressCore address)
    {

        _cellAddress = address;
    }

    public void update()
        throws InterruptedException
    {
        Collection<CellDomainNode> nodes =
                buildTopologyMap(_cellAddress.getCellDomainName()).values();
        _infoMap = nodes.toArray(new CellDomainNode[nodes.size()]);
    }

    @Override
    public CellDomainNode[] getInfoMap()
    {
        return _infoMap;
    }

    @Command(name = "update", hint = "initiates background update",
            description = "Starts background thread to retrieve " +
                    "and update the current domain map topology.")
    public class UpdateCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            new Thread(() -> {
                try {
                    update();
                } catch (InterruptedException e) {
                }
            }).start();
            return "Background update started";
        }
    }
}
