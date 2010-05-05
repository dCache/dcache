package org.dcache.auth;

import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import javax.security.auth.Subject;
import java.security.Principal;
import org.dcache.acl.Origin;

import org.globus.gsi.jaas.GlobusPrincipal;

public class Subjects {

    /**
     * The subject representing the root user, that is, a user that is
     * empowered to do everything.
     */
    public static final Subject ROOT;
    public static final Subject NOBODY;

    static {
        ROOT = new Subject();
        ROOT.getPrincipals().add(new UidPrincipal(0));
        ROOT.getPrincipals().add(new GidPrincipal(0, true));
        ROOT.setReadOnly();

        NOBODY = new Subject();
        NOBODY.setReadOnly();
    }

    /**
     * Returns true if and only if the subject is root, that is, has
     * the user ID 0.
     */
    public static boolean isRoot(Subject subject) {
        return hasUid(subject, 0);
    }

    /**
     * Returns true if and only if the subject has the given user ID.
     */
    public static boolean hasUid(Subject subject, long uid) {
        Set<UidPrincipal> principals =
                subject.getPrincipals(UidPrincipal.class);
        for (UidPrincipal principal : principals) {
            if (principal.getUid() == uid) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if and only if the subject has the given group ID.
     */
    public static boolean hasGid(Subject subject, long gid) {
        Set<GidPrincipal> principals =
                subject.getPrincipals(GidPrincipal.class);
        for (GidPrincipal principal : principals) {
            if (principal.getGid() == gid) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the users IDs of a subject.
     */
    public static long[] getUids(Subject subject) {
        Set<UidPrincipal> principals =
                subject.getPrincipals(UidPrincipal.class);
        long[] uids = new long[principals.size()];
        int i = 0;
        for (UidPrincipal principal : principals) {
            uids[i++] = principal.getUid();
        }
        return uids;
    }

    /**
     * Returns the group IDs of a subject. If the user has a primary
     * group, then first element will be a primary group ID.
     */
    public static long[] getGids(Subject subject) {
        Set<GidPrincipal> principals =
                subject.getPrincipals(GidPrincipal.class);
        long[] gids = new long[principals.size()];
        int i = 0;
        for (GidPrincipal principal : principals) {
            if (principal.isPrimaryGroup()) {
                gids[i++] = gids[0];
                gids[0] = principal.getGid();
            } else {
                gids[i++] = principal.getGid();
            }
        }
        return gids;
    }

    /**
     * Returns one of the primary group IDs of a subject.
     *
     * @throws NoSuchElementException if subject has no primary group
     */
    public static long getPrimaryGid(Subject subject)
            throws NoSuchElementException {
        Set<GidPrincipal> principals =
                subject.getPrincipals(GidPrincipal.class);
        for (GidPrincipal principal : principals) {
            if (principal.isPrimaryGroup()) {
                return principal.getGid();
            }
        }
        throw new NoSuchElementException("Subject has no primary group");
    }

    /**
     * Returns the origin of a subject. If no origin is defined, null
     * is returned. If more than one origin is defined, then
     * NoSuchElementException is thrown (the intuition being that
     * there is no unique origin).
     *
     * @param NoSuchElementException if there is more than one origin
     */
    public static Origin getOrigin(Subject subject)
            throws NoSuchElementException {
        Set<Origin> principals = subject.getPrincipals(Origin.class);
        if (principals.size() == 0) {
            return null;
        }
        if (principals.size() > 1) {
            throw new NoSuchElementException("Subject has no unique origin");
        }
        return principals.iterator().next();
    }

    /**
     * Returns the DN of a subject. If no DN is defined, null is
     * returned. If more than one DN is defined, then
     * NoSuchElementException is thrown (the intuition being that
     * there is no unique DN).
     *
     * @param NoSuchElementException if there is more than one DN
     */
    public static String getDn(Subject subject) {
        Set<GlobusPrincipal> principals =
                subject.getPrincipals(GlobusPrincipal.class);
        if (principals.size() == 0) {
            return null;
        }
        if (principals.size() > 1) {
            throw new NoSuchElementException("Subject has no unique DN");
        }
        return principals.iterator().next().getName();
    }

    /**
     * Returns the collection of FQANs of a subject.
     */
    public static Collection<String> getFqans(Subject subject) {
        Set<FQANPrincipal> principals =
                subject.getPrincipals(FQANPrincipal.class);
        if (principals.size() == 0) {
            return Collections.emptySet();
        }

        Collection<String> fqans = new ArrayList<String>();
        for (FQANPrincipal principal : principals) {
            fqans.add(principal.getName());
        }
        return fqans;
    }

    /**
     * Maps an AuthorizationRecord to a Subject. The Subject will
     * contain the UID (UidPrincipal), GID
     * (GidPrincipal), the mapped user name
     * (UserNamePrincipal), the DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal) of the AuthorizationRecord object.
     */
    public static Subject getSubject(AuthorizationRecord record) {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(record.getUid()));
        principals.add(new UserNamePrincipal(record.getIdentity()));

        boolean primary = true;
        for (GroupList list : record.getGroupLists()) {
            for (Group group : list.getGroups()) {
                principals.add(new GidPrincipal(group.getGid(), primary));
            }
            String fqan = list.getAttribute();
            if (fqan != null && !fqan.isEmpty()) {
                principals.add(new FQANPrincipal(fqan, primary));
            }
            primary = false;
        }

        String dn = record.getName();
        if (dn != null && !dn.isEmpty()) {
            principals.add(new GlobusPrincipal(dn));
        }

        return subject;
    }

    /**
     * Maps a UserAuthBase to a Subject.  The Subject will contain the
     * UID (UidPrincipal), GID
     * (GidPrincipal), DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal) principals.
     *
     * @param user UserAuthBase to convert
     * @param primary Whether the groups of user are the primary groups
     */
    public final static Subject getSubject(UserAuthBase user, boolean primary) {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(user.UID));
        principals.add(new GidPrincipal(user.GID, primary));

        String dn = user.DN;
        if (dn != null && !dn.isEmpty()) {
            principals.add(new GlobusPrincipal(dn));
        }

        String fqan = user.getFqan().toString();
        if (fqan != null && !fqan.isEmpty()) {
            principals.add(new FQANPrincipal(fqan, primary));
        }

        return subject;
    }
}
