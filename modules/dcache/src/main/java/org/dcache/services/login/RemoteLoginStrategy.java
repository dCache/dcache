package org.dcache.services.login;

import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.cells.CellStub;

public class RemoteLoginStrategy implements LoginStrategy
{
    private CellStub _stub;

    public RemoteLoginStrategy()
    {
    }

    public RemoteLoginStrategy(CellStub stub)
    {
        setCellStub(stub);
    }

    @Required
    public void setCellStub(CellStub stub)
    {
        if (stub == null) {
            throw new NullPointerException();
        }
        _stub = stub;
    }

    public CellStub getCellStub()
    {
        return _stub;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        if (_stub == null) {
            throw new IllegalStateException("CellStub is not set");
        }

        try {
            LoginMessage message = _stub.sendAndWait(new LoginMessage(subject));
            return new LoginReply(message.getSubject(),
                                  message.getLoginAttributes());
        } catch (CacheException e) {
            /* Note that dCache vehicles can transport errors.  These
             * are re-thrown as a CacheException (or subclass thereof)
             * with a corresponding return-code value.  The
             * return-code exists to support legacy code; subclassing
             * CacheException is more correct.
             *
             * Some exceptions, if thrown by the remote cell, will be
             * translated to the generic CacheException class.  In
             * particular, if IllegalArgumentException is thrown then
             * the generic CacheException will be thrown with
             * return-code CacheException.INVALID_ARGS.
             *
             * LoginStrategy classes are expected to throw
             * IllegalArgumentException when presented with a Subject
             * they structurally do not support; however, remotely
             * throwing IllegalArgumentException will be mapped to
             * CacheException, so breaking the LoginStrategy contract.
             *
             * Here we map a generic CacheException with return-code
             * CacheException.INVALID_ARGS to
             * IllegalArgumentException.
             */
            if (e.getRc() == CacheException.INVALID_ARGS) {
                throw new IllegalArgumentException(e.getMessage(), e);
            } else {
                throw e;
            }
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new CacheException("Login failed because the operation was interrupted");
        }
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        if (_stub == null) {
            throw new IllegalStateException("CellStub is not set");
        }

        try {
            return _stub.sendAndWait(new MapMessage(principal)).getMappedPrincipal();
        } catch (InterruptedException e) {
            throw new CacheException("Login failed because the operation was interrupted");
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        }
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        if (_stub == null) {
            throw new IllegalStateException("CellStub is not set");
        }

        try {
            return _stub.sendAndWait(new ReverseMapMessage(principal)).getMappedPrincipals();
        } catch (InterruptedException e) {
            throw new CacheException("Login failed because the operation was interrupted");
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage());
        }
    }
}
