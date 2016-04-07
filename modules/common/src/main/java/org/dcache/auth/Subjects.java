package org.dcache.auth;

import com.google.common.net.InetAddresses;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;


public class Subjects
{
    public static final String UNKNOWN = "<unknown>";

    /**
     * Ordered list of principals considered as displayable.
     */
    private static final Class<? extends Principal>[] DISPLAYABLE = new Class[]
    {
        FullNamePrincipal.class,
        UserNamePrincipal.class,
        GlobusPrincipal.class,
        KerberosPrincipal.class,
        Origin.class,
        Principal.class
    };

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
    public static boolean isRoot(Subject subject)
    {
        return hasUid(subject, 0);
    }

    /**
     * Returns true if and only if the subject is nobody, i.e., does
     * not have a UID.
     *
     * Being nobody does not imply that the user is anonymous: The
     * subjects's identiy may have been established through some
     * authentication mechanism. However the subject could not be
     * assigned an internal identity in dCache.
     */
    public static boolean isNobody(Subject subject)
    {
        for (Principal principal: subject.getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if and only if the subject has the given user ID.
     */
    public static boolean hasUid(Subject subject, long uid)
    {
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
    public static boolean hasGid(Subject subject, long gid)
    {
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
    public static long[] getUids(Subject subject)
    {
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
     * @throws IllegalArgumentException is subject has more than one such principal
     */
    private static <T> T getUniquePrincipal(Subject subject, Class<T> type)
        throws IllegalArgumentException
    {
        T result = null;

        if( subject == null) {
            return null;
        }

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
     * @throws IllegalArgumentException if there is more than one origin
    */
    public static Origin getOrigin(Subject subject)
        throws IllegalArgumentException
    {
        return getUniquePrincipal(subject, Origin.class);
    }

    /**
     * Returns the DN of a subject. Returns null if subject has no DN.
     *
     * @throws IllegalArgumentException if there is more than one origin
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
    public static FQAN getPrimaryFqan(Subject subject)
        throws NoSuchElementException
    {
        Set<FQANPrincipal> principals =
            subject.getPrincipals(FQANPrincipal.class);
        FQAN fqan = null;
        for (FQANPrincipal principal: principals) {
            if (principal.isPrimaryGroup()) {
                if (fqan != null) {
                    throw new IllegalArgumentException("Subject has multiple primary FQANs");
                }
                fqan = principal.getFqan();
            }
        }
        return fqan;
    }

    /**
     * Returns the collection of FQANs of a subject.
     */
    public static Collection<FQAN> getFqans(Subject subject)
    {
        Collection<FQAN> fqans = new ArrayList<>();
        for (Principal principal: subject.getPrincipals()) {
            if (principal instanceof FQANPrincipal) {
                fqans.add(((FQANPrincipal) principal).getFqan());
            }
        }
        return fqans;
    }

    /**
     * Returns the the user name of a subject. If UserNamePrincipal is
     * not defined then null is returned.
     *
     * @throws IllegalArgumentException if subject has more than one
     *        user name
     */
    public static String getUserName(Subject subject)
    {
        UserNamePrincipal principal =
            getUniquePrincipal(subject, UserNamePrincipal.class);
        return (principal == null) ? null : principal.getName();
    }

    /**
     * Returns the the login name of a subject. If LoginNamePrincipal
     * is not defined then null is returned.
     *
     * @throws IllegalArgumentException if subject has more than one
     *        login name
     */
    public static String getLoginName(Subject subject)
    {
        LoginNamePrincipal principal =
            getUniquePrincipal(subject, LoginNamePrincipal.class);
        return (principal == null) ? null : principal.getName();
    }

    /**
     * Returns a displayable name derived from one of the principals
     * of the Subject.
     */
    public static String getDisplayName(Subject subject)
    {
        for (Class<? extends Principal> clazz: DISPLAYABLE) {
            Set<? extends Principal> principals = subject.getPrincipals(clazz);
            if (!principals.isEmpty()) {
                return principals.iterator().next().getName();
            }
        }
        return UNKNOWN;
    }

    /**
     * Returns the "Kerberos principal" for the user (as specified in
     * Section 2.1 of RFC 1964) if they logged in via Kerberos or null if
     * Kerberos was not used.
     * @throws IllegalArgumentException if the subject contains multiple
     * KerberosPrincipal.
     */
    public static String getKerberosName(Subject subject) throws IllegalArgumentException
    {
        KerberosPrincipal principal =
                getUniquePrincipal(subject, KerberosPrincipal.class);
        return (principal == null) ? null : principal.getName();
    }

    /**
     * Maps a UserAuthBase to a Subject.  The Subject will contain the
     * UID (UidPrincipal), GID (GidPrincipal), user name
     * (UserNamePrincipal), DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal) principals.
     *
     * @param user UserAuthBase to convert
     * @param primary Whether the groups of user are the primary groups
     */
    public static final Subject getSubject(UserAuthBase user, boolean primary)
    {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(user.UID));

        boolean isPrimary = primary;
        for (int gid: user.GIDs) {
            principals.add(new GidPrincipal(gid, isPrimary));
            isPrimary = false;
        }

        String name = user.Username;
        if (name != null && !name.isEmpty()) {
            principals.add(new UserNamePrincipal(name));
        }

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

    /**
     * Maps a UserAuthRecord to a Subject.  The Subject will contain
     * the UID (UidPrincipal), GID (GidPrincipal), user name
     * (UserNamePrincipal), DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal) principals.
     *
     * @param user UserAuthRecord to convert
     */
    public static final Subject getSubject(UserAuthRecord user)
    {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(user.UID));

        boolean primary = true;
        for (int gid: user.GIDs) {
            principals.add(new GidPrincipal(gid, primary));
            primary = false;
        }

        String name = user.Username;
        if (name != null && !name.isEmpty()) {
            principals.add(new UserNamePrincipal(name));
        }

        String dn = user.DN;
        if (dn != null && !dn.isEmpty()) {
            principals.add(new GlobusPrincipal(dn));
        }

        FQAN fqan = user.getFqan();
        if (fqan!=null) {
            String fqanstr = fqan.toString();
            if (fqanstr != null && !fqanstr.isEmpty()) {
                principals.add(new FQANPrincipal(fqanstr, true));
            }
        }
        return subject;
    }

    /**
     * Create a subject for UNIX based user record.
     *
     * @param uid
     * @param gid
     * @param gids
     */
    public static Subject of(int uid, int gid, int[] gids)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(uid));
        subject.getPrincipals().add(new GidPrincipal(gid, true));
        for (int g : gids) {
            subject.getPrincipals().add(new GidPrincipal(g, false));
        }
        return subject;
    }

    /**
     * Create a subject from a list of principals.  The principals are
     * presented as String-based representations that are parsed.  They
     * have a common format {@literal <type>:<value>} where
     * {@literal <type>} is one of name, kerberos, dn and dqan and
     * {@literal <value>} is a string representation of the principal.
     */
    public static Subject subjectFromArgs(List<String> args)
    {
        Set<Principal> principals = principalsFromArgs(args);

        Set<Object> publicCredentials = Collections.emptySet();
        Set<Object> privateCredentials = Collections.emptySet();

        return new Subject(false, principals, publicCredentials,
                privateCredentials);
    }


    public static Set<Principal> principalsFromArgs(List<String> args)
    {
        Set<Principal> principals = new HashSet<>();
        boolean isPrimaryFqan = true;

        for(String arg : args) {
            int idx = arg.indexOf(':');
            if(idx == -1) {
                throw new IllegalArgumentException("format for principals is <type>:<value>");
            }

            String type = arg.substring(0, idx);
            String value = arg.substring(idx+1);

            Principal principal;

            switch (type) {
                case "dn":
                    principal = new GlobusPrincipal(value);
                    break;
                case "kerberos":
                    principal = new KerberosPrincipal(value);
                    break;
                case "fqan":
                    principal = new FQANPrincipal(value, isPrimaryFqan);
                    isPrimaryFqan = false;
                    break;
                case "name":
                    principal = new LoginNamePrincipal(value);
                    break;
                case "origin":
                    principal = new Origin(InetAddresses.forString(value));
                    break;
                case "oidc":
                    principal = new OidcSubjectPrincipal(value);
                    break;
                case "email":
                    principal = new EmailAddressPrincipal(value);
                    break;
                default:
                    try {
                        Class principalClass = Class.forName(type);
                        Constructor principalConstructor = principalClass.getConstructor(String.class);
                        principal = (Principal)principalConstructor.newInstance(value);
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("No matching constructor found: "+type+"(String)");
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("No matching class found: "+type);
                    } catch (InvocationTargetException e) {
                        throw new IllegalArgumentException("Invocation failed: "+e.toString());
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Instantiation failed: "+e.toString());
                    } catch (IllegalAccessException e) {
                        throw new IllegalArgumentException("Access Exception: "+e.toString());
                    }
            }

            principals.add(principal);
        }
        return principals;
    }
}
