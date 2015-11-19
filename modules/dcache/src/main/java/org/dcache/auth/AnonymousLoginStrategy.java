package org.dcache.auth;

import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.util.CacheException;

import org.dcache.auth.attributes.LoginAttribute;

/**
 * Anonymous login strategy, used on the xrootd door when
 * no authentication is specified. The behaviour follows the old door
 * behaviour - depending on configuration, a NOBODY user, a ROOT user or a
 * user based on the configured uid/gid is returned as a result of the
 * login operation.
 *
 * @author tzangerl
 */
public class AnonymousLoginStrategy implements LoginStrategy
{
    public static final String USER_ROOT = "root";
    public static final String USER_NOBODY = "nobody";
    public static final Pattern USER_PATTERN =
        Pattern.compile("(\\d+):((\\d+)(,(\\d+))*)");

    private static final Set<LoginAttribute> NO_ATTRIBUTES =
            Collections.emptySet();

    private Subject _subject;

    public AnonymousLoginStrategy()
    {
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        return new LoginReply(_subject, NO_ATTRIBUTES);
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        return null;
    }

    /**
     * Parses a string on the form UID:GID(,GID)* and returns a
     * corresponding Subject.
     */
    private Subject parseUidGidList(String user)
    {
        Matcher matcher = USER_PATTERN.matcher(user);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid user string");
        }

        Subject subject = new Subject();
        int uid = Integer.parseInt(matcher.group(1));
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(uid));
        boolean primary = true;
        for (String group: matcher.group(2).split(",")) {
            int gid = Integer.parseInt(group);
            principals.add(new GidPrincipal(gid, primary));
            primary = false;
        }
        subject.setReadOnly();

        return subject;
    }

    @Required
    public void setUser(String user) {
        switch (user) {
        case USER_ROOT:
            _subject = Subjects.ROOT;
            break;
        case USER_NOBODY:
            _subject = Subjects.NOBODY;
            break;
        default:
            _subject = parseUidGidList(user);
            break;
        }
    }
}
