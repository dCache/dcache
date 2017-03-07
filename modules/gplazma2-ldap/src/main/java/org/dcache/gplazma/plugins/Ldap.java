/*
 * Copyright (c) 2017 - 2018 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.gplazma.plugins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;

import java.security.Principal;
import java.util.function.Predicate;
import java.util.Collection;
import java.util.Optional;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;

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
 *    gplazma.ldap.url = ldap://ldap.example.com:389/
 *    gplazma.ldap.organization = o=SITE,c=COUNTRY
 *    gplazma.ldap.tree.people = People
 *    gplazma.ldap.tree.groups = Groups
 *    gplazma.ldap.home-dir = "/"
 *    gplazma.ldap.root-dir = "%homeDirectory%" evaluates to the users home directory
 *    gplazma.ldap.group-member = "memberUid" or "uniqueMember"
 * </pre>
 *
 * @since 2.3
 */
public class Ldap implements GPlazmaIdentityPlugin, GPlazmaSessionPlugin, GPlazmaMappingPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ldap.class);

    /*
     * LDAP attibute names as defined by http://www.ietf.org/rfc/rfc2307.txt
     */
    public static final String GID_NUMBER_ATTRIBUTE = "gidNumber";
    public static final String HOME_DIR_ATTRIBUTE = "homeDirectory";
    public static final String UID_NUMBER_ATTRIBUTE = "uidNumber";
    public static final String COMMON_NAME_ATTRIBUTE = "cn";
    public static final String USER_ID_ATTRIBUTE = "uid";
    public static final String MEMBER_UID_ATTRIBUTE = "memberUid";
    public static final String UNIQUE_MEMBER_ATTRIBUTE = "uniqueMember";

    /*
     * dcache config variables
     */
    public static final String LDAP_URL = "gplazma.ldap.url";
    public static final String LDAP_USER_HOME = "gplazma.ldap.home-dir";
    public static final String LDAP_USER_ROOT = "gplazma.ldap.root-dir";
    public static final String LDAP_GROUP_MEMBER = "gplazma.ldap.group-member";

    public static final String LDAP_BINDDN = "gplazma.ldap.binddn";
    public static final String LDAP_BINDPW = "gplazma.ldap.bindpw";
    public static final String LDAP_AUTH = "gplazma.ldap.auth";

    public static final String LDAP_ORG = "gplazma.ldap.organization";
    public static final String LDAP_PEOPLE_TREE = "gplazma.ldap.tree.people";
    public static final String LDAP_GROUP_TREE = "gplazma.ldap.tree.groups";
    public static final String LDAP_USER_FILTER = "gplazma.ldap.userfilter";

    /**
     * Search control to retrieve uidNumber attribute.
     */
    private static final SearchControls SC_UID_NUMBER = buildSearchControls(UID_NUMBER_ATTRIBUTE);

    /**
     * Search control to retrieve gidNumber attribute.
     */
    private static final SearchControls SC_GID_NUMBER = buildSearchControls(GID_NUMBER_ATTRIBUTE);

    /**
     * Search control to retrieve uidNumber attribute.
     */
    private static final SearchControls SC_UID_GID_NUMBER = buildSearchControls(UID_NUMBER_ATTRIBUTE, GID_NUMBER_ATTRIBUTE);

    /**
     * Search control to retrieve all attribute.
     */
    private static final SearchControls SC_ALL = buildSearchControls((String[]) null);

    @FunctionalInterface
    /**
     * Represents a function that accepts a template string and ldap attributes
     * and returns a new string where all place holders are replaced with
     * attribute values.
     */
    private interface ReplaceKeywords {

        /**
         * Transform given template string with values from provided attributes.
         *
         * @param s string to transform.
         * @param attrs attributes that have to be used.
         * @return a string which is constructed from the template and
         * attributes.
         * @throws NamingException if attributes extraction failed.
         */
        String transform(String s, Attributes attrs) throws NamingException;
    }

    /**
     * String transformation function, which replaces %attr% in original string
     * with corresponding attribute value.
     */
    private static final ReplaceKeywords REPLACE_BY_ATTRIBUTES = (s, attrs) -> {
        NamingEnumeration<? extends Attribute> na = attrs.getAll();
        while (na.hasMore()) {
            Attribute attr = na.next();
            s = s.replaceAll("%" + attr.getID() + "%", String.valueOf(attr.get()));
        }
        return s;
    };

    /**
     * String transformation function which returns the original string.
     */
    private static final ReplaceKeywords RETURN_ORIGINAL_STRING = (s, attrs) -> s;

    @FunctionalInterface
    private interface LdapGroupSearch {

        Set<GidPrincipal> searchGroup(DirContext ctx, Principal p, String peopleOU, String groupOU) throws NamingException;
    }

    /**
     * Use {@code memberUid} attribute to discover group membership.
     */
    private static final LdapGroupSearch GROUP_BY_MEMBER_UID = (c, p, pou, gou) -> {

        NamingEnumeration<SearchResult> groupResult = c.search(gou,
                String.format("%s=%s", MEMBER_UID_ATTRIBUTE, p.getName()),
                SC_GID_NUMBER);

        return extractAttributes(groupResult, GID_NUMBER_ATTRIBUTE, s -> new GidPrincipal(s, false));
    };

    /**
     * Use {@code uniqueMember} attribute to discover group membership as
     * defined by RFC2307 and RFC2307bis.
     */
    private static final LdapGroupSearch GROUP_BY_UNIQUE_MEMBER = (c, p, pou, gou) -> {

        NamingEnumeration<SearchResult> groupResult = c.search(gou,
                String.format("%s=uid=%s,%s", UNIQUE_MEMBER_ATTRIBUTE, p.getName(), pou),
                SC_GID_NUMBER);

        return extractAttributes(groupResult, GID_NUMBER_ATTRIBUTE, s -> new GidPrincipal(s, false));
    };

    @FunctionalInterface
    /**
     * Interface of an object which can map Principal to an other Principal by
     * lookup in ldap server.
     */
    private interface OneToOnePrincipalMapper {

        Principal apply(InitialLdapContext ctx, Principal principal) throws NamingException;
    }

    @FunctionalInterface
    /**
     * Interface of an object which can map Principal to a set of Principal by
     * lookup in ldap server.
     */
    private interface OneToManyPrincipalMapper {

        Set<Principal> apply(InitialLdapContext ctx, Principal principal) throws NamingException;
    }

    /**
     * The ldap connection url.
     */
    private final String ldapUrl;

    /**
     * The OU (organizational unit) to add users to
     */
    private final String peopleOU;

    /**
     * The OU (organizational unit) to add groups to
     */
    private final String groupOU;

    /**
     * The search filter to use to locate a user's entry in the LDAP directory.
     */
    private final String userFilter;

    /**
     * Properties to initialize ldap connection.
     */
    private final Properties ldapConnectionProperties;

    /**
     * function used to execute group membership ldap query.
     */
    private final LdapGroupSearch getGroupsByUid;

    /**
     * A template to construct users home directory.
     */
    private final String userHome;

    /**
     * function which derives user's home directory;
     */
    private final ReplaceKeywords userHomeTransformation;

    /**
     * A template to construct users root directory.
     */
    private final String userRoot;

    /**
     * function which derives user's root directory;
     */
    private final ReplaceKeywords userRootTransformation;

    /**
     * Create a Ldap identity plugin.
     *
     * @param properties plugin configuration properties.
     */
    public Ldap(Properties properties) {

        ldapUrl = properties.getProperty(LDAP_URL);
        String organization = properties.getProperty(LDAP_ORG);
        String peopleTree = properties.getProperty(LDAP_PEOPLE_TREE);
        String groupTree = properties.getProperty(LDAP_GROUP_TREE);
        userFilter = properties.getProperty(LDAP_USER_FILTER);

        peopleOU = String.format("ou=%s,%s", peopleTree, organization);
        groupOU = String.format("ou=%s,%s", groupTree, organization);

        userHome = properties.getProperty(LDAP_USER_HOME);
        userHomeTransformation = userHome.matches(".*%\\w+%.*")
                ? REPLACE_BY_ATTRIBUTES : RETURN_ORIGINAL_STRING;

        userRoot = properties.getProperty(LDAP_USER_ROOT);
        userRootTransformation = userRoot.matches(".*%\\w+%.*")
                ? REPLACE_BY_ATTRIBUTES : RETURN_ORIGINAL_STRING;

        switch (properties.getProperty(LDAP_GROUP_MEMBER)) {
            case "uniqueMember":
                getGroupsByUid = GROUP_BY_UNIQUE_MEMBER;
                break;
            case "memberUid":
                getGroupsByUid = GROUP_BY_MEMBER_UID;
                break;
            default:
                throw new IllegalArgumentException("Unsuported membership schema: " + properties.getProperty(LDAP_GROUP_MEMBER));
        }

        ldapConnectionProperties = new Properties();
        ldapConnectionProperties.put(DirContext.INITIAL_CONTEXT_FACTORY,
                "com.sun.jndi.ldap.LdapCtxFactory");
        ldapConnectionProperties.put(DirContext.PROVIDER_URL, ldapUrl);

        /*
         * Use LDAP connection pooling to re-use connection.
         * Bad connections are automatically detected and removed from the pool
         * by the LDAP provider.
         * See: http://docs.oracle.com/javase/jndi/tutorial/ldap/connect/pool.html
         */
        ldapConnectionProperties.put("com.sun.jndi.ldap.connect.pool", "true");
        ldapConnectionProperties.put("com.sun.jndi.ldap.connect.pool.protocol", "plain ssl");

        if ("simple".equals(properties.getProperty(LDAP_AUTH))) {
            ldapConnectionProperties.put(Context.SECURITY_AUTHENTICATION, "simple");
            ldapConnectionProperties.put(Context.SECURITY_PRINCIPAL, properties.getProperty(LDAP_BINDDN));
            ldapConnectionProperties.put(Context.SECURITY_CREDENTIALS, properties.getProperty(LDAP_BINDPW));
        }
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        Optional<Principal> principal = findFirst(principals, UserNamePrincipal.class::isInstance);

        if (principal.isPresent()) {

            //REVISIT: if we query LDAP server to user record, then we probably have to respect the provided primary GID
            boolean hasPrimaryGid = principals.stream()
                    .filter(GidPrincipal.class::isInstance)
                    .map(GidPrincipal.class::cast)
                    .anyMatch(GidPrincipal::isPrimaryGroup);

            try (AutoCloseableLdapContext ctx = new AutoCloseableLdapContext()) {
                NamingEnumeration<SearchResult> sResult = ctx.search(peopleOU,
                        String.format(userFilter, principal.get().getName()),
                        SC_UID_GID_NUMBER);

                if (sResult.hasMore()) {
                    Attributes userAttr = sResult.next().getAttributes();
                    principals.add(new UidPrincipal((String) userAttr.get(UID_NUMBER_ATTRIBUTE).get()));
                    principals.add(new GidPrincipal((String) userAttr.get(GID_NUMBER_ATTRIBUTE).get(), !hasPrimaryGid));

                    principals.addAll(getGroupsByUid.searchGroup(ctx, principal.get(), peopleOU, groupOU));
                }
            } catch (NamingException e) {
                LOGGER.warn("Failed to get mapping: {}", e.toString());
            }
        }
    }

    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException {

        OneToOnePrincipalMapper mapper;

        if (principal instanceof UserNamePrincipal) {
            mapper = (c, p) -> {
                NamingEnumeration<SearchResult> sre = c.search(peopleOU,
                        String.format("(%s=%s)", USER_ID_ATTRIBUTE, p.getName()),
                        SC_UID_NUMBER);

                return new UidPrincipal(extractAttribute(sre, UID_NUMBER_ATTRIBUTE));
            };
        } else if (principal instanceof GroupNamePrincipal) {
            mapper = (c, p) -> {
                NamingEnumeration<SearchResult> sre = c.search(groupOU,
                        String.format("(%s=%s)", COMMON_NAME_ATTRIBUTE, p.getName()),
                        SC_GID_NUMBER);

                return new GidPrincipal(extractAttribute(sre, GID_NUMBER_ATTRIBUTE), false);
            };
        } else {
            throw new NoSuchPrincipalException(principal);
        }

        try (AutoCloseableLdapContext ctx = new AutoCloseableLdapContext()) {
            return mapper.apply(ctx, principal);
        } catch (NoSuchElementException e) {
            throw new NoSuchPrincipalException(principal);
        } catch (NamingException e) {
            LOGGER.warn("Failed to get mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException {

        OneToManyPrincipalMapper mapper;
        if (principal instanceof GidPrincipal) {
            mapper = (c, p) -> {
                NamingEnumeration<SearchResult> ne = c.search(groupOU,
                        new BasicAttributes(GID_NUMBER_ATTRIBUTE, p.getName()));

                return extractAttributes(ne, COMMON_NAME_ATTRIBUTE, GroupNamePrincipal::new);

            };
        } else if (principal instanceof UidPrincipal) {
            mapper = (c, p) -> {
                NamingEnumeration<SearchResult> ne = c.search(peopleOU,
                        new BasicAttributes(UID_NUMBER_ATTRIBUTE, p.getName()));

                return extractAttributes(ne, USER_ID_ATTRIBUTE, UserNamePrincipal::new);
            };
        } else {
            throw new NoSuchPrincipalException(principal);
        }

        try (AutoCloseableLdapContext ctx = new AutoCloseableLdapContext()) {
            return mapper.apply(ctx, principal);
        } catch (NoSuchElementException e) {
            throw new NoSuchPrincipalException(principal);
        } catch (NamingException e) {
            LOGGER.warn("Failed to get reverse mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public void session(Set<Principal> authorizedPrincipals, Set<Object> attrib) throws AuthenticationException {
        Optional<Principal> principal = findFirst(authorizedPrincipals, UserNamePrincipal.class::isInstance);
        if (principal.isPresent()) {
            // shortcut: no path transitions are required. Use provided values.
            if (userHomeTransformation == RETURN_ORIGINAL_STRING && userRootTransformation == RETURN_ORIGINAL_STRING) {
                attrib.add(new HomeDirectory(userHome));
                attrib.add(new RootDirectory(userRoot));
                return;
            }

            try (AutoCloseableLdapContext ctx = new AutoCloseableLdapContext()) {
                NamingEnumeration<SearchResult> sResult = ctx.search(peopleOU,
                        String.format(userFilter, principal.get().getName()),
                        SC_ALL);

                if (sResult.hasMore()) {
                    SearchResult rs = sResult.next();
                    Attributes attrs = rs.getAttributes();
                    attrib.add(new HomeDirectory(userHomeTransformation.transform(userHome, attrs)));
                    attrib.add(new RootDirectory(userRootTransformation.transform(userRoot, attrs)));
                } else {
                    throw new AuthenticationException("no mapping for " + principal.get());
                }
            } catch (NamingException e) {
                throw new AuthenticationException("no mapping: "
                        + e.getMessage(), e);
            }
        }
    }

    private static SearchControls buildSearchControls(String... attr) {
        SearchControls constraints = new SearchControls();
        constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
        constraints.setReturningAttributes(attr);
        return constraints;
    }

    /**
     * A {@link InitialLdapContext} which can be used in try-with-resource
     * block.
     */
    private class AutoCloseableLdapContext extends InitialLdapContext implements AutoCloseable {

        public AutoCloseableLdapContext() throws NamingException {
            super(ldapConnectionProperties, null);
        }

        @Override
        public void close() {
            try {
                super.close();
            } catch (NamingException e) {
                LOGGER.error("Failed to close LDAP context: {}", e.getMessage());
            }
        }
    }

    private static String extractAttribute(NamingEnumeration<SearchResult> sre, String attr) throws NamingException {
        if (!sre.hasMore()) {
            throw new NoSuchElementException();
        }

        SearchResult rs = sre.next();
        return (String) rs.getAttributes().get(attr).get();
    }

    /**
     * Get set of attributes extracted from the search result.
     *
     * @param <T> type of extracted type.
     * @param sre ldap search result.
     * @param attr search result attribute.
     * @param mapper mapping function to apply to each result element.
     * @return set of attributes.
     * @throws NamingException
     */
    private static <T> Set<T> extractAttributes(NamingEnumeration<SearchResult> sre, String attr, Function<String, T> mapper) throws NamingException {

        Set<T> attrs = new HashSet<>();
        while (sre.hasMore()) {
            T v = mapper.apply((String) sre.next().getAttributes().get(attr).get());
            attrs.add(v);
        }

        if (attrs.isEmpty()) {
            throw new NoSuchElementException();
        }

        return attrs;
    }

    private <T> Optional<T> findFirst(Collection<T> collection, Predicate<T> predicate) {
        return collection.stream().filter(predicate).findFirst();
    }
}
