package org.dcache.services.ssh2;

import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;

import diskCacheV111.admin.UserAdminShell;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;

import org.dcache.cells.CellStub;
import org.dcache.util.list.ListDirectoryHandler;

public class ShellFactory implements Factory<Command>, CellMessageSender
{
    private CellEndpoint _endpoint;
    private File _historyFile;
    private int _historySize;
    private boolean _useColor;
    private CellStub _pnfsManager;
    private CellStub _poolManager;
    private CellStub _acm;
    private String _prompt;
    private ListDirectoryHandler _list;

    @Required
    public void setHistoryFile(File historyFile)
    {
        _historyFile = historyFile;
    }

    @Required
    public void setHistorySize(int size) {
        _historySize = size;
    }

    @Required
    public void setUseColor(boolean useColor)
    {
        _useColor = useColor;
    }

    @Required
    public void setPnfsManager(CellStub stub)
    {
        _pnfsManager = stub;
    }

    @Required
    public void setPoolManager(CellStub stub)
    {
        _poolManager = stub;
    }

    @Required
    public void setAcm(CellStub stub)
    {
        _acm = stub;
    }

    @Required
    public void setPrompt(String prompt)
    {
        _prompt = prompt;
    }

    @Required
    public void setListHandler(ListDirectoryHandler list)
    {
        _list = list;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
    }

    @Override
    public Command create()
    {
        return new ShellCommand(_historyFile, _historySize, _useColor, createShell());
    }

    private UserAdminShell createShell()
    {
        UserAdminShell shell = new UserAdminShell(_prompt);
        shell.setCellEndpoint(_endpoint);
        shell.setPnfsManager(_pnfsManager);
        shell.setPoolManager(_poolManager);
        shell.setAcm(_acm);
        shell.setListHandler(_list);
        return shell;
    }
}
