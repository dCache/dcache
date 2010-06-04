package org.dcache.services.login;

import javax.security.auth.Subject;

import org.dcache.auth.LoginStrategy;
import org.dcache.auth.LoginReply;
import org.dcache.cells.CellStub;

import diskCacheV111.util.CacheException;

public class RemoteLoginStrategy implements LoginStrategy
{
    private CellStub _stub;

    public RemoteLoginStrategy(CellStub stub)
    {
        _stub = stub;
    }

    public void setCellStub(CellStub stub)
    {
        _stub = stub;
    }

    public CellStub getCellStub()
    {
        return _stub;
    }

    public LoginReply login(Subject subject) throws CacheException
    {
        try {
            LoginMessage message =
                _stub.sendAndWait(new LoginMessage(subject));
            return new LoginReply(message.getSubject(),
                                  message.getLoginAttributes());
        } catch (InterruptedException e) {
            throw new CacheException("Login failed because the operation was interrupted");
        }
    }
}