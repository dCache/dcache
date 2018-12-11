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

import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.ldap4testing.EmbeddedServer;

import static org.dcache.gplazma.plugins.Ldap.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class LdapTest {

    private static final GroupNamePrincipal ACTOR_GROUP_PRINCIPAL = new GroupNamePrincipal("actor");
    private static final GidPrincipal ACTOR_GID_PRINCIPAL = new GidPrincipal(1001, false);

    private static final GidPrincipal OTHER_PRIMARY_GID = new GidPrincipal(2000, true);

    private static final GidPrincipal KERMIT_PRIMARY_GID_PRINCIPAL = new GidPrincipal(1000, true);
    private static final GidPrincipal KERMIT_GID_PRINCIPAL = new GidPrincipal(1000, false);
    private static final UidPrincipal KERMIT_UID_PRINCIPAL = new UidPrincipal(1000);
    private static final UserNamePrincipal KERMIT_PRINCIPAL = new UserNamePrincipal("kermit");

    private static final UserNamePrincipal BERND_PRINCIPAL = new UserNamePrincipal("bernd");
    private static final RootDirectory BERND_ROOT_DIRECTORY = new RootDirectory("/");
    private static final HomeDirectory BERND_HOME_DIRECTORY = new HomeDirectory("/home/bernd");

    private static final UserNamePrincipal NON_EXISTING_PRINCIPAL = new UserNamePrincipal("outlaw");
    private static final UidPrincipal NON_EXISTING_UID_PRINCIPAL = new UidPrincipal(111);
    private static final GidPrincipal NON_EXISTING_GID_PRINCIPAL = new GidPrincipal(111, true);

    private EmbeddedServer ldapServer;
    private Ldap plugin;

    @Before
    public void setUp() throws IOException {

        InputStream initLdiff = ClassLoader.getSystemResourceAsStream("org/dcache/gplazma/plugins/ldap/init.ldif");
        ldapServer = new EmbeddedServer(0, initLdiff);
        ldapServer.start();

        Properties properties = new Properties();
        properties.put(LDAP_URL, "ldap://localhost:" + ldapServer.getSocketAddress().getPort());
        properties.put(LDAP_ORG, "o=dcache,c=org");
        properties.put(LDAP_USER_FILTER, "(uid=%s)");
        properties.put(LDAP_PEOPLE_TREE, "people");
        properties.put(LDAP_GROUP_TREE, "group");
        properties.put(LDAP_USER_HOME, "/home/%uid%");
        properties.put(LDAP_USER_ROOT, "/");
        properties.put(LDAP_GROUP_MEMBER, "uniqueMember");
        properties.put(LDAP_TRY_UID_MAPPING, "true");

        properties.put(LDAP_AUTH, "simple");
        properties.put(LDAP_BINDDN, "uid=kermit,ou=people,o=dcache,c=org");
        properties.put(LDAP_BINDPW, "kermitTheFrog");

        plugin = new Ldap(properties);
    }

    @Test
    public void shouldReturnMatchingUidGid() throws AuthenticationException {
        Set<Principal> principals = Sets.newHashSet(KERMIT_PRINCIPAL);

        plugin.map(principals);

        assertThat("unexpected number of returned principals", principals, hasSize(4));
        assertThat("expected USERNAME not found", principals, hasItem(KERMIT_PRINCIPAL));
        assertThat("expected UID not found", principals, hasItem(KERMIT_UID_PRINCIPAL));
        assertThat("expected GID not found", principals, hasItem(KERMIT_PRIMARY_GID_PRINCIPAL));
        assertThat("expected GID not found", principals, hasItem(ACTOR_GID_PRINCIPAL));
    }

    @Test
    public void shouldReturnMatchingUidGidByUid() throws AuthenticationException {
        Set<Principal> principals = Sets.newHashSet(KERMIT_UID_PRINCIPAL);

        plugin.map(principals);

        assertThat("unexpected number of returned principals", principals, hasSize(4));
        assertThat("expected USERNAME not found", principals, hasItem(KERMIT_PRINCIPAL));
        assertThat("expected UID not found", principals, hasItem(KERMIT_UID_PRINCIPAL));
        assertThat("expected GID not found", principals, hasItem(KERMIT_PRIMARY_GID_PRINCIPAL));
        assertThat("expected GID not found", principals, hasItem(ACTOR_GID_PRINCIPAL));
    }

    @Test
    public void shouldReturnMatchingUidGidWithExistingPrimaryGid() throws AuthenticationException {
        Set<Principal> principals = Sets.newHashSet(KERMIT_PRINCIPAL, OTHER_PRIMARY_GID);

        plugin.map(principals);

        assertThat("unexpected number of returned principals", principals, hasSize(5));
        assertThat("expected USERNAME not found", principals, hasItem(KERMIT_PRINCIPAL));
        assertThat("expected UID not found", principals, hasItem(KERMIT_UID_PRINCIPAL));
        assertThat("expected GID not found", principals, hasItem(OTHER_PRIMARY_GID));
        assertThat("expected GID not found", principals, hasItem(KERMIT_GID_PRINCIPAL));
        assertThat("expected GID not found", principals, hasItem(ACTOR_GID_PRINCIPAL));
    }

    @Test
    public void shouldDoNothingForNonExisting() throws AuthenticationException {
        Set<Principal> principals = Sets.newHashSet(NON_EXISTING_PRINCIPAL);
        plugin.map(principals);
        assertThat("unexpected number of returned principals", principals, hasSize(1));
        assertThat("expected USERNAME not found", principals, hasItem(NON_EXISTING_PRINCIPAL));
    }

    @Test
    public void shouldReturnCorrectMappedUid() throws NoSuchPrincipalException {
        Principal mapped = plugin.map(KERMIT_PRINCIPAL);
        assertEquals("unexpected mapping", KERMIT_UID_PRINCIPAL, mapped);
    }

    @Test(expected = NoSuchPrincipalException.class)
    public void shouldFailMappingForNonExisting() throws NoSuchPrincipalException {
        plugin.map(NON_EXISTING_PRINCIPAL);
    }

    @Test
    public void shouldReturnSetContaningUserNameOnReverseMapping() throws NoSuchPrincipalException {
        Set<Principal> rmap = plugin.reverseMap(KERMIT_UID_PRINCIPAL);
        assertThat("Expceted principal not found", rmap, hasItem(KERMIT_PRINCIPAL));
    }

    @Test
    public void shouldReturnSetContaningGroupNameOnReverseMapping() throws NoSuchPrincipalException {
        Set<Principal> rmap = plugin.reverseMap(new GidPrincipal(1001, false));
        assertThat("Expceted principal not found", rmap, hasItem(ACTOR_GROUP_PRINCIPAL));
    }

    @Test
    public void shouldReturnSerializableSetOnReverseMapping() throws NoSuchPrincipalException {
        Set<Principal> rmap = plugin.reverseMap(KERMIT_UID_PRINCIPAL);

        assertThat("Expecting serializable set", instanceOf(Serializable.class));
    }

    @Test(expected = NoSuchPrincipalException.class)
    public void shouldFailReverseMappingForNonExistingUid() throws NoSuchPrincipalException {
        plugin.reverseMap(NON_EXISTING_UID_PRINCIPAL);
    }

    @Test(expected = NoSuchPrincipalException.class)
    public void shouldFailReverseMappingForNonExistingGid() throws NoSuchPrincipalException {
        plugin.reverseMap(NON_EXISTING_GID_PRINCIPAL);
    }

    @Test
    public void shouldReturnUserHomeAndRoot() throws AuthenticationException {
        Set<Principal> principals = Sets.newHashSet(BERND_PRINCIPAL);
        Set<Object> attrs = new HashSet<>();

        plugin.session(principals, attrs);
        assertThat("expected HOME attribute not found", attrs, hasItem(BERND_HOME_DIRECTORY));
        assertThat("expected ROOT attribute not found", attrs, hasItem(BERND_ROOT_DIRECTORY));
    }

    @Test
    public void shouldAllowLogin() throws AuthenticationException {
        Set<Principal> principals = new HashSet<>();

        plugin.authenticate(Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("kermit", "kermitTheFrog")),
                principals);

        assertThat("Expected principal not found", principals, hasItem(KERMIT_PRINCIPAL));
    }

    @After
    public void tearDown() {
        ldapServer.stop();
    }
}
