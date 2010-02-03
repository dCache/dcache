package org.dcache.services.topology;

import dmg.cells.network.CellDomainNode;
import dmg.util.Args;

import org.dcache.cells.CellCommandListener;

public class CellsTopologyInterpreter
    implements CellCommandListener
{
    private CellsTopology _topology;

    public void setCellsTopology(CellsTopology topology)
    {
        _topology = topology;
    }

    public final String hh_ls = "[-l] # list available domains";
    public String ac_ls_$_0(Args args)
    {
        boolean detail = args.getOpt("l") != null;

        CellDomainNode [] info = _topology.getInfoMap();
        if (info == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (CellDomainNode node: info) {
            sb.append(node.getName());
            if (detail) {
                sb.append(" ").append(node.getAddress());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public final String hh_gettopomap =
        "# provides topology map in binary form";
    public Object ac_gettopomap(Args args)
    {
        return _topology.getInfoMap();
    }
}