package org.dcache.services.topology;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellCommandListener;

import org.dcache.util.Args;

public class CellsTopologyInterpreter
    implements CellCommandListener
{
    private CellsTopology _topology;
    private HostnameService _hostnameService;

    public void setCellsTopology(CellsTopology topology)
    {
        _topology = topology;
    }

    public void setHostnameService(HostnameService hostnameService) {
        _hostnameService = hostnameService;
    }

    public static final String hh_ls = "[-l] # list available domains";
    public String ac_ls_$_0(Args args)
    {
        boolean detail = args.hasOption("l");

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

    public static final String hh_gettopomap =
        "# provides topology map in binary form";
    public Object ac_gettopomap(Args args)
    {
        return _topology.getInfoMap();
    }
    public static final String hh_getallhostnames = "# returns a complete " +
            "list of all hosts running a domain of this dCache instance. " +
            "Run updatehostnames first to get uptodate values";

    public String ac_getallhostnames(Args args) {
        return _hostnameService.getHostnames();
    }
    public static final String hh_updatehostnames = "# starts background thread to retrieve" +
            "all hostnames of hosts hosting a dCache domain";

    public String ac_updatehostnames(Args args) {
        Thread thread = new Thread() {

            @Override
            public void run() {
                _hostnameService.updateHostnames();
            }
        };
        thread.start();
        return "Hostname update started";
    }
}
