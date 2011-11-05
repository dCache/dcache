package org.dcache.auth;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.springframework.beans.factory.annotation.Required;

import diskCacheV111.util.CacheException;

/**
 * Anonymous login strategy, used on the xrootd door when
 * no authentication is specified. The behaviour follows the old door
 * behaviour - depending on configuration, a NOBODY user, a ROOT user or a
 * user based on the configured uid/gid is returned as a result of the
 * login operation.
 *
 * The root path is configurable in the dcache configuration, as is whether the
 * strategy the door is read-only for anonymous access.
 *
 * @author tzangerl
 *
 */
public class AnonymousLoginStrategy implements LoginStrategy
{
    public final static String USER_ROOT = "root";
    public final static String USER_NOBODY = "nobody";
    public final static Pattern USER_PATTERN =
        Pattern.compile("(\\d+):((\\d+)(,(\\d+))*)");

    private boolean _readOnly;
    private Subject _subject;
    private String _rootPath;

    public AnonymousLoginStrategy() {
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException {

        LoginAttribute readOnly = new ReadOnly(_readOnly);
        LoginAttribute rootPath = new RootDirectory(_rootPath);

        Set<LoginAttribute> attributes = new HashSet<LoginAttribute>();
        attributes.add(readOnly);
        attributes.add(rootPath);

        LoginReply reply = new LoginReply(_subject, attributes);

        return reply;
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
    public void setReadOnly(boolean readOnly) {
        _readOnly = readOnly;
    }

    @Required
    public void setUser(String user) {
        if (user.equals(USER_ROOT)) {
            _subject = Subjects.ROOT;
        } else if (user.equals(USER_NOBODY)) {
            _subject = Subjects.NOBODY;
        } else {
            _subject = parseUidGidList(user);
        }
    }

    /**
     * Sets the root path.
     *
     * The root path forms the root of the name space of the xrootd
     * server. Xrootd paths are translated to full PNFS paths by
     * predending the root path.
     */
    @Required
    public void setRootPath(String rootPath) {
        _rootPath = rootPath;
    }
}
