package org.dcache.services.topology;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellCommandListener;

import dmg.util.command.Command;
import dmg.util.command.Option;

import java.io.Serializable;
import java.util.concurrent.Callable;

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

    @Command(name = "ls", hint = "list available domains",
            description = "List the names of all available domains " +
                    "in dCache system.")
    public class LsCommand implements Callable<String>
    {
        @Option(name = "l", usage = "show domain address")
        boolean detail;

        @Override
        public String call()
        {
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
    }

    @Command(name = "gettopomap", hint = "provides topology map",
            description = "Show detailed topology map of all domains " +
                    "in the dCache system. This map contains information " +
                    "of how the domains are connected with each other.")
    public class GettopomapCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call()
        {
            return _topology.getInfoMap();
        }
    }

    @Command(name = "getallhostnames", hint = "list all hostnames",
            description = "Returns a complete list of all hosts running " +
                    "a domain of this dCache instance. To get an up-to-date " +
                    "list, first run updatehostnames command.")
    public class GetallhostnamesCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return _hostnameService.getHostnames();
        }
    }

    @Command(name = "updatehostnames", hint = "update hostname record",
            description = "Starts background thread to retrieve the hostnames " +
                    "of all hosts, which are hosting a dCache domain. The dCache " +
                    "hostname service get updated to reflect the retrieved " +
                    "hostnames.")
    public class UpdatehostnamesCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            new Thread(_hostnameService::updateHostnames).start();
            return "Hostname update started";
        }
    }
}
