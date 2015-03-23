package org.dcache.services.ssh2;

import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.springframework.beans.factory.annotation.Required;

import java.util.Arrays;
import java.util.List;

import dmg.cells.nucleus.CellEndpoint;

import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;
import org.dcache.util.TransferCollector;

import static java.util.stream.Collectors.toList;

public class PcellsSubsystemFactory implements NamedFactory<Command>, CellMessageSender
{
    private CellEndpoint endpoint;
    private CellStub spaceManager;
    private CellStub poolManager;
    private CellStub pnfsManager;
    private List<CellPath> loginBrokers;

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
    public void setLoginBrokers(String loginBrokers)
    {
        this.loginBrokers = Arrays.stream(loginBrokers.split(",")).map(CellPath::new).collect(toList());
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

    @Override
    public String getName()
    {
        return "pcells";
    }

    @Override
    public Command create()
    {
        TransferCollector transferCollector = new TransferCollector(new CellStub(endpoint), loginBrokers);
        return new PcellsCommand(endpoint, spaceManager, poolManager, pnfsManager, transferCollector);
    }
}
