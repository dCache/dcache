package org.dcache.auth;

import javax.security.auth.Subject;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

import org.dcache.auth.attributes.LoginAttribute;

/**
 * Immutable encapsulation of a login result as provided by a
 * LoginStrategy. The LoginReply embeds a logged in Subject with all
 * mapped principals and login attributes associatated with the
 * current login.
 */
public class LoginReply
{
    private Subject _subject;
    private Set<LoginAttribute> _attributes;

    public LoginReply()
    {
        _subject = new Subject();
        _attributes = new HashSet<>();
    }

    public LoginReply(Subject subject, Set<LoginAttribute> attributes)
    {
        _subject = subject;
        _attributes = attributes;
    }

    /**
     * Returns the Subject of this LoginReply.
     */
    public Subject getSubject()
    {
        return _subject;
    }

    /**
     * Return the Set of login attributes associated with this
     * LoginReply. Each element represents some attributes associated
     * with the LoginReply.
     *
     * The returned Set is backed by this LoginReply's internal
     * login attributes Set. Any modification to the returned Set
     * affects the internal login attributes Set as well.
     */
    public Set<LoginAttribute> getLoginAttributes()
    {
        return _attributes;
    }

    /**
     * Return a Set of login attributes associated with this LoginReply
     * that are instances or subclasses of the specified Class.
     *
     * The returned Set is not backed by this LoginReply's internal
     * login attributes Set. A new Set is created and returned for
     * each method invocation. Modifications to the returned Set will
     * not affect the internal login attributes Set.
     */
    public <T extends LoginAttribute> Set<T> getLoginAttributes(Class<T> type)
    {
        Set<T> result = new HashSet<>();
        for (Object element: _attributes) {
            if (type.isInstance(element)) {
                result.add((T) element);
            }
        }
        return result;
    }

    public String toString()
    {
        String name = Subjects.getDisplayName(_subject);
        if (Subjects.isNobody(_subject)) {
            return "Login[" + name + "," + _attributes + "]";
        } else {
            return "Login[" + name + ","
                + Subjects.getUid(_subject) + ":"
                + Arrays.toString(Subjects.getGids(_subject)) + ","
                + _attributes + "]";
        }
    }
}
