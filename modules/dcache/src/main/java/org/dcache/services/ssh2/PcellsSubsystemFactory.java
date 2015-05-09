package org.dcache.services.ssh2;

import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.springframework.beans.factory.annotation.Required;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.services.login.LoginBrokerSource;

import org.dcache.cells.CellStub;

public class PcellsSubsystemFactory implements NamedFactory<Command>, CellMessageSender
{
    private CellEndpoint endpoint;
    private CellStub spaceManager;
    private CellStub poolManager;
    private CellStub pnfsManager;
    private LoginBrokerSource loginBrokerSource;

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        this.endpoint = endpoint;
    }

    @Required
    public void setSpaceManager(CellStub spaceManager)
    {
        this.spaceManager = spaceManager;
    }

    @Required
    public void setPoolManager(CellStub poolManager)
    {
        this.poolManager = poolManager;
    }

    @Required
    public void setPnfsManager(CellStub pnfsManager)
    {
        this.pnfsManager = pnfsManager;
    }

    @Required
    public void setLoginBrokerSource(LoginBrokerSource source)
    {
        this.loginBrokerSource = source;
    }

    @Override
    public String getName()
    {
        return "pcells";
    }

    @Override
    public Command create()
    {
        return new PcellsCommand(endpoint, spaceManager, poolManager, pnfsManager, loginBrokerSource);
    }
}
