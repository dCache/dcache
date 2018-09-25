/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gplazma.roles;

import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.attributes.Role;
import org.dcache.auth.attributes.UnassertedRole;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.util.PrincipalSetMaker;

import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class RolesPluginTest
{
    private class PluginAssertion
    {
        Long adminGid;
        Long observerGid;

        public PluginAssertion withAdminGid(long gid)
        {
            adminGid = gid;
            return this;
        }

        public PluginAssertion withObserverGid(long gid)
        {
            observerGid = gid;
            return this;
        }

        public void realise()
        {
            Properties p = new Properties();
            if (adminGid != null) {
                p.setProperty(RolesPlugin.ADMIN_GID_PROPERTY_NAME,
                              Long.toString(adminGid));
            }
            if (observerGid != null) {
                p.setProperty(RolesPlugin.OBSERVER_GID_PROPERTY_NAME,
                              Long.toString(observerGid));
            }
            plugin = new RolesPlugin(p);
        }
    }

    RolesPlugin plugin;
    Set<Object> attributes;

    @Test
    public void testUndefinedRoles() throws Exception
    {
        given(rolesPlugin());

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(0));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
        assertThat(attributes, not(hasItem(new Role("observer"))));
        assertThat(attributes, not(hasItem(new UnassertedRole("observer"))));
    }

    @Test
    public void testNonAdminNoDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
    }

    @Test
    public void testAdminByPrimaryGidNoDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(10));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, hasItem(new UnassertedRole("admin")));
    }

    @Test
    public void testAdminByNonPrimaryGidNoDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000).withGid(10));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, hasItem(new UnassertedRole("admin")));
    }

    @Test
    public void testObserverByNonPrimaryGidNoDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000).withGid(10));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
        assertThat(attributes, hasItem(new UnassertedRole("observer")));
    }

    @Test(expected=AuthenticationException.class)
    public void testNonAdminDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000).withDesiredRole("admin"));
    }

    @Test(expected=AuthenticationException.class)
    public void testNonObserverDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(0).withDesiredRole("observer"));
    }

    @Test
    public void testAdminAndObserverRolesWithSameGid() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(0));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000)
                                                  .withGid(0));

        assertThat(attributes, hasItem(new UnassertedRole("admin")));
        assertThat(attributes, hasItem(new UnassertedRole("observer")));
        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, not(hasItem(new Role("observer"))));
    }

    @Test
    public void testDesiredAdminWithObserverRolesWithSameGid() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(0));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000)
                                                  .withGid(0).withDesiredRole("admin"));

        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
        assertThat(attributes, hasItem(new UnassertedRole("observer")));
        assertThat(attributes, hasItem(new Role("admin")));
        assertThat(attributes, not(hasItem(new Role("observer"))));
    }

    @Test
    public void testDesiredObserverWithAdminRolesWithSameGid() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(0));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000)
                                                  .withGid(0).withDesiredRole("observer"));

        assertThat(attributes, not(hasItem(new UnassertedRole("observer"))));
        assertThat(attributes, hasItem(new UnassertedRole("admin")));
        assertThat(attributes, hasItem(new Role("observer")));
        assertThat(attributes, not(hasItem(new Role("admin"))));
    }

    @Test
    public void testAdminByPrimaryGidDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(10).withDesiredRole("admin"));

        assertThat(attributes, hasItem(new Role("admin")));
        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
    }

    @Test
    public void testObserverByPrimaryGidDesiredAdminRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(10)
                                                  .withGid(0).withGid(1000)
                                                  .withDesiredRole("admin"));

        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
        assertThat(attributes, hasItem(new Role("admin")));
        assertThat(attributes, not(hasItem(new Role("observer"))));
        assertThat(attributes, hasItem(new UnassertedRole("observer")));
    }

    @Test
    public void testObserverByPrimaryGidDesiredObserverRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(10)
                                                  .withGid(0).withGid(1000)
                                                  .withDesiredRole("observer"));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, hasItem(new UnassertedRole("admin")));
        assertThat(attributes, hasItem(new Role("observer")));
        assertThat(attributes, not(hasItem(new UnassertedRole("observer"))));
    }

    @Test
    public void testObserverByGidDesiredObserverRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000)
                                                  .withGid(0).withGid(10)
                                                  .withDesiredRole("observer"));

        assertThat(attributes, not(hasItem(new Role("admin"))));
        assertThat(attributes, hasItem(new UnassertedRole("admin")));
        assertThat(attributes, hasItem(new Role("observer")));
        assertThat(attributes, not(hasItem(new UnassertedRole("observer"))));
    }

    @Test
    public void testObserverByGidDesiredAdminRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(0).withObserverGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000)
                                                  .withGid(0).withGid(10)
                                                  .withDesiredRole("admin"));

        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
        assertThat(attributes, hasItem(new Role("admin")));
        assertThat(attributes, not(hasItem(new Role("observer"))));
        assertThat(attributes, hasItem(new UnassertedRole("observer")));
    }

    @Test
    public void testAdminByNonPrimaryGidDesiredRoleLogin() throws Exception
    {
        given(rolesPlugin().withAdminGid(10));

        whenInvokingSessionWith(aSetOfPrincipals().withPrimaryGid(1000).withGid(10).withDesiredRole("admin"));

        assertThat(attributes, hasItem(new Role("admin")));
        assertThat(attributes, not(hasItem(new UnassertedRole("admin"))));
    }

    private void whenInvokingSessionWith(PrincipalSetMaker principals)
            throws AuthenticationException
    {
        attributes = new HashSet<>();
        plugin.session(principals.build(), attributes);
    }

    private void given(PluginAssertion assertion)
    {
        assertion.realise();
    }

    private PluginAssertion rolesPlugin()
    {
        return new PluginAssertion();
    }
}
