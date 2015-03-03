package org.dcache.services.ssh2;

import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.springframework.beans.factory.annotation.Required;

import dmg.cells.nucleus.CellEndpoint;

import dmg.cells.nucleus.CellMessageSender;

public class PcellsSubsystemFactory implements NamedFactory<Command>, CellMessageSender
{
    private CellEndpoint endpoint;
    private String prompt;

    @Required
    public void setPrompt(String prompt)
    {
        this.prompt = prompt;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
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
        return new PcellsCommand(endpoint, prompt);
    }


}
