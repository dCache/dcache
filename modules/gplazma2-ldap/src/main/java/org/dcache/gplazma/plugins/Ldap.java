package org.dcache.gplazma.plugins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
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

/**
 * gPlazma plug-in which uses LDAP server to provide requested information.
 *
 * Can be combined with other map/auth plugins:
 * <pre>
 *   auth optional  x509
 *   auth optional  voms
 *   map  optional  gridmap
 *   map  optional krb5
 *   map requisite ldap
 *   identity requisite ldap
 *   session requisite ldap
 * </pre>
 *
 * Corresponding configuration in <b>dcache.conf</b>
 * <pre>
 *    gplazma.ldap.server = ldap.example.com
 *    gplazma.ldap.port = 389
 *    gplazma.ldap.organization = o=SITE,c=CONTRY
 *    gplazma.ldap.tree.people = People
 *    gplazma.ldap.tree.groups = Groups
 * </pre>
 *
 * @since 2.3
 */
public class Ldap implements GPlazmaIdentityPlugin, GPlazmaSessionPlugin, GPlazmaMappingPlugin {

    private static final Logger _log = LoggerFactory.getLogger(Ldap.class);

    /*
     * LDAP attibute names as defined by http://www.ietf.org/rfc/rfc2307.txt
     */
    private static final String GID_NUMBER_ATTRIBUTE = "gidNumber";
    private static final String HOME_DIR_ATTRIBUTE = "homeDirectory";
    private static final String UID_NUMBER_ATTRIBUTE = "uidNumber";
    private static final String COMMON_NAME_ATTRIBUTE = "cn";
    private static final String USER_ID_ATTRIBUTE = "uid";
    private static final String MEMBER_UID_ATTRIBUTE = "memberUid";

    /*
     * dcache config variables
     */
    static final String LDAP_SERVER = "gplazma.ldap.server";
    static final String LDAP_PORT = "gplazma.ldap.port";
    static final String LDAP_ORG = "gplazma.ldap.organization";
    static final String LDAP_PEOPLE_TREE = "gplazma.ldap.tree.people";
    static final String LDAP_GROUP_TREE = "gplazma.ldap.tree.groups";
    /**
     * The OU (organizational unit) to add users to
     */
    private final String peopleOU;
    /**
     * The OU (organizational unit) to add groups to
     */
    private final String groupOU;
    private final InitialDirContext _ctx;

    /**
     * Create a NIS identity plugin.
     *
     * @throws NamingException
     */
    public Ldap(Properties properties) throws NamingException {

        String server = properties.getProperty(LDAP_SERVER);
        String port = properties.getProperty(LDAP_PORT);
        String organization = properties.getProperty(LDAP_ORG);
        String peopleTree = properties.getProperty(LDAP_PEOPLE_TREE);
        String groupTree = properties.getProperty(LDAP_GROUP_TREE);


        peopleOU = String.format("ou=%s,%s", peopleTree, organization);
        groupOU = String.format("ou=%s,%s", groupTree, organization);

        Properties env = new Properties();
        env.put(DirContext.INITIAL_CONTEXT_FACTORY,
                "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(DirContext.PROVIDER_URL, String.format("ldap://%s:%s", server, port));

        _ctx = new InitialDirContext(env);
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        Principal principal =
                find(principals, instanceOf(UserNamePrincipal.class), null);
        if (principal != null) {
            try {
                NamingEnumeration<SearchResult> sResult = _ctx.search(peopleOU,
                        String.format("(%s=%s)", USER_ID_ATTRIBUTE, principal.getName()),
                        getSimplSearchControls(UID_NUMBER_ATTRIBUTE, GID_NUMBER_ATTRIBUTE));
                if (sResult.hasMore()) {
                    Attributes userAttr = sResult.next().getAttributes();
                    principals.add(new UidPrincipal((String) userAttr.get(UID_NUMBER_ATTRIBUTE).get()));
                    principals.add(new GidPrincipal((String) userAttr.get(GID_NUMBER_ATTRIBUTE).get(), true));

                    NamingEnumeration<SearchResult> groupResult = _ctx.search(groupOU,
                            new BasicAttributes(MEMBER_UID_ATTRIBUTE, principal.getName()));
                    while (groupResult.hasMore()) {
                        SearchResult result = groupResult.next();
                        principals.add(
                                new GidPrincipal((String) result.getAttributes().get(GID_NUMBER_ATTRIBUTE).get(), false));
                    }
                }
            } catch (NamingException e) {
                _log.warn("Faild to get mapping: {}", e.toString());
            }
        }
    }

    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException {

        String name = principal.getName();
        try {
            NamingEnumeration<SearchResult> sResult;
            if (principal instanceof UserNamePrincipal) {
                sResult = _ctx.search(peopleOU,
                        String.format("(%s=%s)", USER_ID_ATTRIBUTE, name),
                        getSimplSearchControls(UID_NUMBER_ATTRIBUTE));
                if (sResult.hasMore()) {
                    SearchResult rs = sResult.next();
                    return new UidPrincipal((String) rs.getAttributes().get(UID_NUMBER_ATTRIBUTE).get());
                }
            } else if (principal instanceof GroupNamePrincipal) {
                sResult = _ctx.search(groupOU,
                        String.format("(%s=%s)", COMMON_NAME_ATTRIBUTE, name),
                        getSimplSearchControls(GID_NUMBER_ATTRIBUTE));
                if (sResult.hasMore()) {
                    SearchResult rs = sResult.next();
                    return new GidPrincipal((String) rs.getAttributes().get(GID_NUMBER_ATTRIBUTE).get(), false);
                }
            }

        } catch (NamingException e) {
            _log.warn("Faild to get mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException {
        String id = principal.getName();
        try {

            Set<Principal> principals = new HashSet<>();

            if (principal instanceof GidPrincipal) {
                NamingEnumeration<SearchResult> ne = _ctx.search(groupOU,
                        new BasicAttributes(GID_NUMBER_ATTRIBUTE, id));


                while (ne.hasMore()) {
                    SearchResult result = ne.next();
                    String name = (String) result.getAttributes().get(COMMON_NAME_ATTRIBUTE).get();
                    principals.add(new GroupNamePrincipal(name));
                }
            } else if (principal instanceof UidPrincipal) {
                NamingEnumeration<SearchResult> ne = _ctx.search(peopleOU,
                        new BasicAttributes(UID_NUMBER_ATTRIBUTE, id));


                while (ne.hasMore()) {
                    SearchResult result = ne.next();
                    String name = (String) result.getAttributes().get(USER_ID_ATTRIBUTE).get();
                    principals.add(new UserNamePrincipal(name));
                }
            }
            return principals;
        } catch (NamingException e) {
            _log.warn("Faild to get reverse mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public void session(Set<Principal> authorizedPrincipals, Set<Object> attrib) throws AuthenticationException {
        Principal principal =
                find(authorizedPrincipals, instanceOf(UserNamePrincipal.class), null);
        if (principal != null) {
            try {
                NamingEnumeration<?> sResult = _ctx.search(peopleOU,
                        String.format("(%s=%s)", USER_ID_ATTRIBUTE, principal.getName()),
                        getSimplSearchControls(HOME_DIR_ATTRIBUTE));
                if (sResult.hasMore()) {
                    SearchResult rs = (SearchResult) sResult.next();
                    attrib.add(new HomeDirectory((String) rs.getAttributes().get(HOME_DIR_ATTRIBUTE).get()));
                    attrib.add(new RootDirectory("/"));
                    attrib.add(new ReadOnly(false));
                }
            } catch (NamingException e) {
                throw new AuthenticationException("no mapping: "
                        + e.getMessage(), e);
            }
        }
    }

    private SearchControls getSimplSearchControls(String... attr) {
        SearchControls constraints = new SearchControls();
        constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String attrList[] = attr;
        constraints.setReturningAttributes(attrList);
        return constraints;
    }

    public static void main(String[] args) throws NamingException, AuthenticationException, NoSuchPrincipalException {
        Properties properties = new Properties();
        properties.setProperty(LDAP_SERVER, "seds2.desy.de");
        properties.setProperty(LDAP_PORT, "489");
        properties.setProperty(LDAP_ORG, "o=DESY,c=DE");
        properties.setProperty(LDAP_PEOPLE_TREE, "People");
        properties.setProperty(LDAP_GROUP_TREE, "PosixGroups");

        Ldap ldap = new Ldap(properties);

        System.out.println(ldap.map(new UserNamePrincipal("tigran")));
        try {
            ldap.map(new UserNamePrincipal("NoSuchUser"));
            System.out.println("huh!");
        } catch (NoSuchPrincipalException e) {
            System.out.println("No Such User");
        }
        System.out.println(ldap.map(new GroupNamePrincipal("it")));
        try {
            ldap.map(new GroupNamePrincipal("NoSuchGroup"));
            System.out.println("huh!");
        } catch (NoSuchPrincipalException e) {
            System.out.println("No Such Group");
        }
        System.out.println(ldap.reverseMap(new GidPrincipal(1000, false)));
        System.out.println(ldap.reverseMap(new GidPrincipal(1070, false)));
        System.out.println(ldap.reverseMap(new UidPrincipal(3750)));
        System.out.println(ldap.reverseMap(new UidPrincipal(111111111)));
        System.out.println(ldap.reverseMap(new GidPrincipal(222222222, false)));

        Set<Principal> principals = new HashSet<>();
        Set<Object> attrs = new HashSet<>();
        principals.add(new UserNamePrincipal("tigran"));
        ldap.map(principals);
        ldap.session(principals, attrs);
        System.out.println(principals);
        System.out.println(attrs);
    }
}
