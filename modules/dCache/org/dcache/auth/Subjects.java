package org.dcache.auth;

import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
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
     * Returns the principal of the given type of the subject. Returns
     * null if there is no such principal.
     *
     * @throw IllegalArguemntException is subject has more than one such principal
     */
    private static <T> T getUniquePrincipal(Subject subject, Class<T> type)
        throws IllegalArgumentException
    {
        T result = null;
        for (Principal principal: subject.getPrincipals()) {
            if (type.isInstance(principal)) {
                if (result != null) {
                    throw new IllegalArgumentException("Subject has multiple principals of type " + type.getSimpleName());
                }
                result = type.cast(principal);
            }
        }
        return result;
    }

    /**
     * Returns the UID of a subject.
     *
     * @throws NoSuchElementException if subject has no UID
     * @throws IllegalArgumentException is subject has more than one UID
     */
    public static long getUid(Subject subject)
        throws NoSuchElementException, IllegalArgumentException
    {
        UidPrincipal uid = getUniquePrincipal(subject, UidPrincipal.class);
        if (uid == null) {
            throw new NoSuchElementException("Subject has no UID");
        }
        return uid.getUid();
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
     * Returns the primary group ID of a subject.
     *
     * @throws NoSuchElementException if subject has no primary GID
     * @throws IllegalArgumentException if subject has several primary GID
     */
    public static long getPrimaryGid(Subject subject)
        throws NoSuchElementException, IllegalArgumentException
    {
        Set<GidPrincipal> principals =
                subject.getPrincipals(GidPrincipal.class);
        int counter = 0;
        long gid = 0;
        for (GidPrincipal principal : principals) {
            if (principal.isPrimaryGroup()) {
                gid = principal.getGid();
                counter++;
            }
        }

        if (counter == 0) {
            throw new NoSuchElementException("Subject has no primary GID");
        }
        if (counter > 1) {
            throw new IllegalArgumentException("Subject has multiple primary GIDs");
        }

        return gid;
    }

    /**
     * Returns the origin of a subject. Returns null if subject has no
     * origin.
     *
     * @param IllegalArgumentException if there is more than one origin
    */
    public static Origin getOrigin(Subject subject)
        throws IllegalArgumentException
    {
        return getUniquePrincipal(subject, Origin.class);
    }

    /**
     * Returns the DN of a subject. Returns null if subject has no DN.
     *
     * @param IllegalArgumentException if there is more than one origin
     */
    public static String getDn(Subject subject)
        throws IllegalArgumentException
    {
        GlobusPrincipal principal =
            getUniquePrincipal(subject, GlobusPrincipal.class);
        return (principal == null) ? null : principal.getName();
    }

    /**
     * Returns the primary FQANs of a subject. Returns null if subject
     * has no primary FQAN.
     *
     * @throws IllegalArgumentException if subject has more than one
     *         primary FQANs
     */
    public static String getPrimaryFqan(Subject subject)
        throws NoSuchElementException
    {
        Set<FQANPrincipal> principals =
            subject.getPrincipals(FQANPrincipal.class);
        String fqan = null;
        for (FQANPrincipal principal: principals) {
            if (principal.isPrimary()) {
                if (fqan != null) {
                    throw new IllegalArgumentException("Subject has multiple primary FQANs");
                }
                fqan = principal.getName();
            }
        }
        return fqan;
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
     * Returns the the user name of a subject. If UserNamePrincipal is
     * not defined then null is returned.
     *
     * @throw IllegalArgumentException if subject has more than one
     *        user name
     */
    public static String getUserName(Subject subject)
    {
        UserNamePrincipal principal =
            getUniquePrincipal(subject, UserNamePrincipal.class);
        return (principal == null) ? null : principal.getName();
    }

    /**
     * Converts an AuthorizationRecord to a Subject. The Subject will
     * contain the UID (UidPrincipal), GID (GidPrincipal), the mapped
     * user name (UserNamePrincipal), the DN (GlobusPrincipal), and
     * FQAN (FQANPrincipal) of the AuthorizationRecord object.
     *
     * Notice that the Subject will represent a subset of the
     * information stored in the record.
     */
    public static Subject getSubject(AuthorizationRecord record) {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(record.getUid()));

        String identity = record.getIdentity();
        if (identity != null && !identity.isEmpty()) {
            principals.add(new UserNamePrincipal(identity));
        }

        boolean primary = true;
        for (GroupList list : record.getGroupLists()) {
            String fqan = list.getAttribute();
            if (fqan != null && !fqan.isEmpty()) {
                principals.add(new FQANPrincipal(fqan, primary));
            }
            for (Group group: list.getGroups()) {
                principals.add(new GidPrincipal(group.getGid(), primary));
                primary = false;
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
     * Converts to a Subject an AuthorizationRecord. The the UID
     * (UidPrincipal), GID (GidPrincipal), the mapped user name
     * (UserNamePrincipal), the DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal) will be included in the AuthorizationRecord.
     *
     * Notice that the AuthorizationRecord will represent a subset of
     * the information stored in the subject.
     *
     * All GIDs will become part of the primary group list. The
     * primary GIDs will appear first in the primary group list.
     */
    public static AuthorizationRecord getAuthorizationRecord(Subject subject)
    {
        boolean hasUid = false;

        AuthorizationRecord record = new AuthorizationRecord();

        List<GroupList> groupLists = new LinkedList<GroupList>();

        GroupList primaryGroupList = new GroupList();
        primaryGroupList.setAuthRecord(record);
        primaryGroupList.setGroups(new ArrayList<Group>());
        groupLists.add(primaryGroupList);

        for (Principal principal: subject.getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                if (hasUid) {
                    throw new IllegalArgumentException("Cannot convert Subject with more than one UID");
                }
                hasUid = true;
                record.setUid((int) ((UidPrincipal) principal).getUid());
            } else if (principal instanceof FQANPrincipal) {
                FQANPrincipal fqanPrincipal = (FQANPrincipal) principal;
                if (fqanPrincipal.isPrimary() && primaryGroupList.getAttribute() == null) {
                    primaryGroupList.setAttribute(fqanPrincipal.getName());
                } else {
                    GroupList groupList = new GroupList();
                    groupList.setAuthRecord(record);
                    groupList.setAttribute(fqanPrincipal.getName());
                    groupList.setGroups(new ArrayList<Group>());
                    groupLists.add(groupList);
                }
            } else if (principal instanceof GidPrincipal) {
                GidPrincipal gidPrincipal = (GidPrincipal) principal;
                Group group = new Group();
                group.setGid((int) gidPrincipal.getGid());
                if (gidPrincipal.isPrimaryGroup()) {
                    primaryGroupList.getGroups().add(0, group);
                } else {
                    primaryGroupList.getGroups().add(group);
                }
            } else if (principal instanceof GlobusPrincipal) {
                record.setName(((GlobusPrincipal) principal).getName());
            } else if (principal instanceof UserNamePrincipal) {
                record.setIdentity(((UserNamePrincipal) principal).getName());
            }
        }

        if (!hasUid) {
            throw new IllegalArgumentException("Cannot convert Subject without UID");
        }

        record.setGroupLists(groupLists);
        record.setId();

        return record;
    }

    /**
     * Maps a UserAuthBase to a Subject.  The Subject will contain the
     * UID (UidPrincipal), GID (GidPrincipal), DN (GlobusPrincipal),
     * and FQAN (FQANPrincipal) principals.
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
