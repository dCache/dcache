package org.dcache.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.ReadOnly;

/**
 * LoginStrategy which forms the union of allowed logins of several
 * access strategies. Login will be granted by the first of a list of
 * LoginStrategies which grants login. If no LoginStrategy grants
 * login, an anonymous login can optionally be generated.
 */
public class UnionLoginStrategy implements LoginStrategy
{
    private static final Logger _log =
        LoggerFactory.getLogger(UnionLoginStrategy.class);

    /**
     * Describes various levels of access.
     */
    public enum AccessLevel
    {
        NONE, READONLY, FULL
    }

    private List<LoginStrategy> _loginStrategies = Collections.emptyList();
    private AccessLevel _anonymousAccess = AccessLevel.NONE;

    public void setLoginStrategies(List<LoginStrategy> list)
    {
        _loginStrategies = new ArrayList<>(list);
    }

    public List<LoginStrategy> getLoginStrategies()
    {
        return Collections.unmodifiableList(_loginStrategies);
    }

    public void setAnonymousAccess(AccessLevel level)
    {
        _log.debug( "Setting anonymous access to {}", level);
        _anonymousAccess = level;
    }

    public AccessLevel getAnonymousAccess()
    {
        return _anonymousAccess;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        for (LoginStrategy strategy: _loginStrategies) {
            _log.debug( "Attempting login strategy: {}", strategy.getClass().getName());

            try {
                LoginReply login = strategy.login(subject);
                _log.debug( "Login strategy returned {}", login.getSubject());

                if (!Subjects.isNobody(login.getSubject())) {
                    return login;
                }
            } catch (IllegalArgumentException e) {
                /* LoginStrategies throw IllegalArgumentException when
                 * provided with a Subject they cannot handle.
                 */
                _log.debug("Login failed with IllegalArgumentException for {}: {}", subject,
                        e.getMessage());
            } catch (PermissionDeniedCacheException e) {
                /* As we form the union of all allowed logins of all
                 * strategies, we ignore the failure and try the next
                 * strategy.
                 */
                _log.debug("Permission denied for {}: {}", subject,
                        e.getMessage());
            }
        }
        _log.debug( "Strategies failed, trying for anonymous access");

        LoginReply reply = new LoginReply();
        switch (_anonymousAccess) {
        case READONLY:
            _log.debug( "Allowing read-only access as an anonymous user");
            reply.getLoginAttributes().add(new ReadOnly(true));
            break;

        case FULL:
            _log.debug( "Allowing full access as an anonymous user");
            reply.getLoginAttributes().add(new ReadOnly(false));
            break;

        default:
            _log.debug( "Login failed");
            throw new PermissionDeniedCacheException("Access denied");
        }
        return reply;
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        for (LoginStrategy strategy: _loginStrategies) {
            Principal result = strategy.map(principal);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        Set<Principal> result = new HashSet<>();
        for (LoginStrategy strategy: _loginStrategies) {
            result.addAll(strategy.reverseMap(principal));
        }
        return result;
    }
}
