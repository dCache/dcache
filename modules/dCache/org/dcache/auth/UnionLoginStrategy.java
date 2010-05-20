package org.dcache.auth;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import javax.security.auth.Subject;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.ReadOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoginStrategy which forms the union of allowed logins of several
 * access strategies. Login will be granted by the first of a list of
 * LoginStrategies which grants login. If no LoginStrategy grants
 * login, an anonymous login can optionally be generated.
 */
public class UnionLoginStrategy implements LoginStrategy
{
    private final static Logger _log =
        LoggerFactory.getLogger(UnionLoginStrategy.class);

    /**
     * Describes various levels of access.
     */
    public enum AccessLevel
    {
        NONE, READONLY, FULL;
    }

    private List<LoginStrategy> _loginStrategies = Collections.emptyList();
    private AccessLevel _anonymousAccess = AccessLevel.NONE;

    public void setLoginStrategies(List<LoginStrategy> list)
    {
        _loginStrategies = new ArrayList<LoginStrategy>(list);
    }

    public List<LoginStrategy> getLoginStrategies()
    {
        return Collections.unmodifiableList(_loginStrategies);
    }

    public void setAnonymousAccess(AccessLevel level)
    {
        _anonymousAccess = level;
    }

    public AccessLevel getAnonymousAccess()
    {
        return _anonymousAccess;
    }

    public LoginReply login(Subject subject) throws CacheException
    {
        for (LoginStrategy strategy: _loginStrategies) {
            try {
                LoginReply login = strategy.login(subject);
                if (!Subjects.isNobody(login.getSubject())) {
                    return login;
                }
            } catch (IllegalArgumentException e) {
                /* Our current LoginStrategies throw
                 * IllegalArgumentException when provided with a
                 * Subject they cannot handle.
                 */
            } catch (PermissionDeniedCacheException e) {
                /* As we form the union of all allowed logins of all
                 * strategies, we ignore the failure and try the next
                 * strategy.
                 */
                _log.debug("Login failed for {}: {}", subject, e);
            }
        }

        switch (_anonymousAccess) {
        case READONLY:
            LoginReply reply = new LoginReply();
            reply.getLoginAttributes().add(new ReadOnly(true));
            return reply;

        case FULL:
            return new LoginReply();

        default:
            throw new PermissionDeniedCacheException("Access denied");
        }
    }
}