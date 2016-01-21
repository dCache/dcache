package org.dcache.services.ssh2;

import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;

public class LegacySubsystemFactory implements NamedFactory<Command>, CellMessageSender
{
    private CellEndpoint _endpoint;

    private File _historyFile;
    private int _historySize;
    private boolean _useColor;
    private String _prompt;

    @Required
    public void setPrompt(String prompt)
    {
        _prompt = prompt;
    }

    @Required
    public void setHistoryFile(File historyFile)
    {
        _historyFile = historyFile;
    }

    @Required
    public void setHistorySize(int historySize) {
        _historySize = historySize;
    }

    @Required
    public void setUseColor(boolean useColor)
    {
        _useColor = useColor;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _endpoint = endpoint;
    }

    @Override
    public String getName()
    {
        return "legacy";
    }

    @Override
    public Command create()
    {
        return new LegacyAdminShellCommand(_endpoint, _historyFile, _historySize, _prompt, _useColor);
    }
}
