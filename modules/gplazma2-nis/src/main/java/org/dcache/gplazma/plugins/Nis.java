package org.dcache.gplazma.plugins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchResult;

import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.find;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * gPlazma plug-in which uses NIS (YP) server to provide requested information.
 *
 * Can be combined with other map/auth plugins:
 * <pre>
 *   auth optional  x509
 *   auth optional  voms
 *   map  optional  gridmap
 *   map  optional krb5
 *   map requisite nis
 *   identity requisite nis
 *   session requisite nis
 * </pre>
 *
 * @since 2.1
 */
public class Nis implements GPlazmaIdentityPlugin, GPlazmaSessionPlugin, GPlazmaMappingPlugin{

    private static final Logger _log = LoggerFactory.getLogger(Nis.class);

    /*
     * Attibute names used by NIS.
     * See: http://www.ietf.org/rfc/rfc2307.txt
     */
    private static final String GID_NUMBER_ATTRIBUTE = "gidNumber";
    private static final String HOME_DIR_ATTRIBUTE = "homeDirectory";
    private static final String UID_NUMBER_ATTRIBUTE = "uidNumber";
    private static final String COMMON_NAME_ATTRIBUTE = "cn";
    private static final String USER_ID_ATTRIBUTE = "uid";
    private static final String MEMBER_UID_ATTRIBUTE = "memberUid";

    /*
     * NIS map file names
     */
    private final static String NISMAP_GROUP_BY_GID = "system/group.bygid";
    private final static String NISMAP_PASSWORD_BY_NAME = "system/passwd.byname";
    private final static String NISMAP_GROUP_BY_NAME = "system/group.byname";
    private final static String NISMAP_PASSWORD_BY_UID = "system/passwd.byuid";

    /*
     * dcache config variables
     */
    private static final String SERVER = "gplazma.nis.server";
    private static final String DOMAIN = "gplazma.nis.domain";

    private final InitialDirContext _ctx;

    /**
     * Create a NIS identity plugin.
     * @throws NamingException
     */
    public Nis(Properties properties) throws NamingException {

        String server = properties.getProperty(SERVER);
        String domain = properties.getProperty(DOMAIN);

        Properties env = new Properties();
        env.put(DirContext.INITIAL_CONTEXT_FACTORY,
                "com.sun.jndi.nis.NISCtxFactory");
        env.put(DirContext.PROVIDER_URL, String.format("nis://%s/%s", server, domain));

        _ctx = new InitialDirContext(env);
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        boolean mapped;
        Principal principal =
                find(principals, instanceOf(UserNamePrincipal.class), null);

        checkAuthentication(principal != null, "no username principal");

        try {
            Attributes userAttr = _ctx.getAttributes(NISMAP_PASSWORD_BY_NAME + "/" + principal.getName());
            principals.add(new UidPrincipal((String) userAttr.get(UID_NUMBER_ATTRIBUTE).get()));
            principals.add(new GidPrincipal((String) userAttr.get(GID_NUMBER_ATTRIBUTE).get(), true));
            NamingEnumeration<SearchResult> groupResult = _ctx.search(NISMAP_GROUP_BY_NAME,
                    new BasicAttributes(MEMBER_UID_ATTRIBUTE, principal.getName()));
            mapped = true;
            while (groupResult.hasMore()) {
                SearchResult result = groupResult.next();
                principals.add(
                        new GidPrincipal((String) result.getAttributes().get(GID_NUMBER_ATTRIBUTE).get(), false));
            }
        } catch (NamingException e) {
            _log.debug("Failed to get mapping: {}", e.toString());
            throw new AuthenticationException("no mapping: " + e.getMessage(), e);
        }
        checkAuthentication(mapped, "no matching principal");
    }

    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException {

        String name = principal.getName();
        try {
            if (principal instanceof UserNamePrincipal) {
                Attributes userAttr = _ctx.getAttributes(NISMAP_PASSWORD_BY_NAME + "/" + name);
                return new UidPrincipal((String) userAttr.get(UID_NUMBER_ATTRIBUTE).get());
            } else if (principal instanceof GroupNamePrincipal) {
                Attributes groupAttr = _ctx.getAttributes(NISMAP_GROUP_BY_NAME + "/" + name);
                return new GidPrincipal((String) groupAttr.get(GID_NUMBER_ATTRIBUTE).get(), false);
            }

        } catch (NamingException e) {
            _log.debug("Failed to get mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException {
        String id = principal.getName();
        try {

            Set<Principal> principals = new HashSet<>();

            if (principal instanceof GidPrincipal) {
                NamingEnumeration<SearchResult> ne = _ctx.search(NISMAP_GROUP_BY_GID,
                        new BasicAttributes(GID_NUMBER_ATTRIBUTE, id));


                while (ne.hasMore()) {
                    SearchResult result = ne.next();
                    String name = (String) result.getAttributes().get(COMMON_NAME_ATTRIBUTE).get();
                    principals.add(new GroupNamePrincipal(name));
                }
            } else if (principal instanceof UidPrincipal) {
                NamingEnumeration<SearchResult> ne = _ctx.search(NISMAP_PASSWORD_BY_UID,
                        new BasicAttributes(UID_NUMBER_ATTRIBUTE, id));


                while (ne.hasMore()) {
                    SearchResult result = ne.next();
                    String name = (String) result.getAttributes().get(USER_ID_ATTRIBUTE).get();
                    principals.add(new UserNamePrincipal(name));
                }
            }
            return principals;
        } catch (NamingException e) {
            _log.debug("Failed to get reverse mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public void session(Set<Principal> authorizedPrincipals, Set<Object> attrib) throws AuthenticationException {
        Principal principal =
            find(authorizedPrincipals, instanceOf(UserNamePrincipal.class), null);
        checkAuthentication(principal != null, "no username principal");

        try {
            Attributes userAttr = _ctx.getAttributes(NISMAP_PASSWORD_BY_NAME + "/" + principal.getName());
            attrib.add(new HomeDirectory((String) userAttr.get(HOME_DIR_ATTRIBUTE).get()));
            attrib.add(new RootDirectory("/"));
            attrib.add(new ReadOnly(false));
        } catch (NamingException e) {
            throw new AuthenticationException("no mapping: "
                    + e.getMessage(), e);
        }
    }
}
