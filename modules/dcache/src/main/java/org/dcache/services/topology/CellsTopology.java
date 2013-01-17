package org.dcache.services.topology;

import dmg.cells.network.CellDomainNode;

public interface CellsTopology
{
    CellDomainNode[] getInfoMap();
}
