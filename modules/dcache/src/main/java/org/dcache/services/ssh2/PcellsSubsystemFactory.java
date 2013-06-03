package org.dcache.services.ssh2;

import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.cells.CellMessageSender;

public class PcellsSubsystemFactory implements NamedFactory<Command>, CellMessageSender
{
    private CellEndpoint endpoint;

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String getName()
    {
        return "pcells";
    }

    @Override
    public Command create()
    {
        return new PcellsCommand(endpoint);
    }


}
