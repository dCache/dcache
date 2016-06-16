package org.dcache.webdav;

import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;

import diskCacheV111.util.CacheException;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.UnionLoginStrategy.AccessLevel;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restrictions;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.dcache.auth.UnionLoginStrategy.AccessLevel.READONLY;
import static org.dcache.auth.UnionLoginStrategy.AccessLevel.NONE;

/**
 * Add support for logging in a particular user as Users.NOBODY, all other
 * requests are passed on to some wrapped LoginStrategy.  If AccessLevel is NONE
 * then all requests are passed onto the wrapped LoginStrategy.
 */
public class AnonymousUserLoginStrategy implements LoginStrategy
{
    private LoginStrategy _inner;
    private String _username;
    private AccessLevel _anonymousAccess = NONE;

    public void setAnonymousAccess(AccessLevel level)
    {
        _anonymousAccess = requireNonNull(level);
    }

    public AccessLevel getAnonymousAccess()
    {
        return _anonymousAccess;
    }

    @Required
    public void setNonAnonymousStrategy(LoginStrategy strategy)
    {
        _inner = requireNonNull(strategy);
    }

    @Required
    public void setUsername(String username)
    {
        _username = requireNonNull(username);
    }

    private boolean isAnonymousUser(Subject subject)
    {
        return subject.getPrivateCredentials().stream()
                    .filter(PasswordCredential.class::isInstance)
                    .map(PasswordCredential.class::cast)
                    .anyMatch(p -> _username.equals(p.getUsername())) ||
                subject.getPrincipals().stream()
                    .filter(LoginNamePrincipal.class::isInstance)
                    .anyMatch(p -> _username.equals(p.getName()));
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        if (_anonymousAccess != NONE && isAnonymousUser(subject)) {
            Set<LoginAttribute> attributes = _anonymousAccess == READONLY ?
                    singleton(Restrictions.readOnly()) : emptySet();
            return new LoginReply(Subjects.NOBODY, attributes);
        }

        return _inner.login(subject);
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        return _inner.map(principal);
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        return _inner.reverseMap(principal);
    }
}
