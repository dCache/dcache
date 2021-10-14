package org.dcache.auth;

import static com.google.common.collect.Iterables.filter;
import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;

/**
 * The result of a login request made through some LoginStrategy.  This contains a Subject that
 * identifies the logged-in user along with a set of LoginAttribute values that qualify how this
 * user may use dCache.
 */
public class LoginReply {

    private final Subject _subject;
    private final Set<LoginAttribute> _attributes;

    public LoginReply() {
        _subject = new Subject();
        _attributes = new HashSet<>();
    }

    public LoginReply(Subject subject, Set<LoginAttribute> attributes) {
        _subject = requireNonNull(subject);
        _attributes = requireNonNull(attributes);
    }

    /**
     * Returns the identity of a successfully logged in user.  This Subject contains no public or
     * private credentials and all principals have been tagged @AuthenticationOutput.
     */
    @Nonnull
    public Subject getSubject() {
        return _subject;
    }

    /**
     * Return the Set of login attributes associated with this LoginReply. Each element represents
     * some attributes associated with the LoginReply.
     * <p>
     * The returned Set is backed by this LoginReply's internal login attributes Set. Any
     * modification to the returned Set affects the internal login attributes Set as well.
     */
    @Nonnull
    public Set<LoginAttribute> getLoginAttributes() {
        return _attributes;
    }

    /**
     * Return a Set of login attributes associated with this LoginReply that are instances or
     * subclasses of the specified Class.
     * <p>
     * The returned Set is not backed by this LoginReply's internal login attributes Set. A new Set
     * is created and returned for each method invocation. Modifications to the returned Set will
     * not affect the internal login attributes Set.
     */
    @Nonnull
    public <T extends LoginAttribute> Set<T> getLoginAttributes(Class<T> type) {
        Set<T> result = new HashSet<>();
        for (Object element : _attributes) {
            if (type.isInstance(element)) {
                result.add((T) element);
            }
        }
        return result;
    }

    public Restriction getRestriction() {
        Iterable<Restriction> restrictions = filter(_attributes, Restriction.class);

        Restriction restriction = null;

        for (Restriction r : restrictions) {
            if (restriction == null) {
                restriction = r;
            } else if (restriction.isSubsumedBy(r)) {
                restriction = r;
            } else if (r.isSubsumedBy(restriction)) {
                // skip r, restriction is already more restrictive.
            } else {
                return Restrictions.concat(restrictions);
            }
        }

        return restriction == null ? Restrictions.none() : restriction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LoginReply that = (LoginReply) o;
        return _attributes.equals(that._attributes) && _subject.equals(that._subject);

    }

    @Override
    public int hashCode() {
        int result = _subject.hashCode();
        result = 31 * result + _attributes.hashCode();
        return result;
    }

    @Override
    public String toString() {
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
