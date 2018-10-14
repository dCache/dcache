package org.dcache.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.Restrictions;

/**
 * LoginStrategy which consults a list of several access strategies when
 * authenticating a user.  The first strategy in the list is consulted.  If that
 * strategy successfully authenticates the user then there is no further action
 * and the LoginReply is used.  If that strategy understands the supplied
 * credentials and rejects the login then no further strategies are consulted.
 * If the strategy does not understand the supplied credential then the the next
 * strategy is tried.  Once no further strategies are to be tried and the user
 * has not been logged in successfully then.
 * <p>
 * If none of the consulted strategies has granted login then the behaviour
 * depends on whether the user supplied any credentials.  If no credentials were
 * presented then the login attempt is treated as anonymous and the correct
 * level of access is granted.  If credentials were presented then it is
 * configurable whether the user is treated as the anonymous user, or the login
 * failure is propagated.
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
    private boolean _shouldFallback = true;

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

    public void setFallbackToAnonymous(boolean fallback)
    {
        _shouldFallback = fallback;
    }

    public boolean hasFallbackToAnonymous()
    {
        return _shouldFallback;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        Optional<Principal> origin = subject.getPrincipals().stream()
                .filter(Origin.class::isInstance)
                .findFirst();

        boolean areCredentialsSupplied = !subject.getPrivateCredentials().isEmpty()
                || !subject.getPublicCredentials().isEmpty()
                || !subject.getPrincipals().stream().allMatch(Origin.class::isInstance);

        PermissionDeniedCacheException loginFailure = null;
        try {
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
                }
            }
        } catch (PermissionDeniedCacheException e) {
            loginFailure = e;
        }

        if (areCredentialsSupplied && !_shouldFallback) {
            throw loginFailure != null ? loginFailure : new PermissionDeniedCacheException("Access denied");
        }

        _log.debug( "Strategies failed, trying for anonymous access");

        LoginReply reply = new LoginReply();
        switch (_anonymousAccess) {
        case READONLY:
            _log.debug( "Allowing read-only access as an anonymous user");
            reply.getLoginAttributes().add(Restrictions.readOnly());
            if (origin.isPresent()) {
                reply.getSubject().getPrincipals().add(origin.get());
            }
            break;

        case FULL:
            _log.debug( "Allowing full access as an anonymous user");
            if (origin.isPresent()) {
                reply.getSubject().getPrincipals().add(origin.get());
            }
            break;

        default:
            _log.debug( "Login failed");
            throw loginFailure != null ? loginFailure : new PermissionDeniedCacheException("Access denied");
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
