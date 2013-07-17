package org.dcache.services.ssh2;

import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.cells.CellMessageSender;

public class CommandFactory implements Factory<Command>, CellMessageSender
{
    private CellEndpoint _endpoint;
    private File _historyFile;
    private boolean _useColor;

    @Required
    public void setHistoryFile(File historyFile)
    {
        _historyFile = historyFile;
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
    public Command create() {
        return new ConsoleReaderCommand(_endpoint, _historyFile, _useColor);
    }
}
