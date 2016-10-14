/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.util;

import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import static org.hamcrest.Matchers.*;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import org.dcache.gplazma.util.IGTFInfo.Status;
import org.dcache.gplazma.util.IGTFInfo.Version;

import static org.dcache.gplazma.util.IGTFInfo.Type.POLICY;
import static org.dcache.gplazma.util.IGTFInfo.Type.TRUST_ANCHOR;
import static org.junit.Assert.*;

public class IGTFInfoTest
{

    @Before
    public void setup()
    {
    }

    @Test
    public void shouldBuildMinimumValidProfile() throws Exception
    {
        IGTFInfo.Builder builder = IGTFInfo.builder(POLICY);
        builder.setVersion("1.78-1");
        builder.setRequires("TestService = 1.78-1");
        builder.setSubjectDN("\"/DC=org/DC=example/DC=test-ca/CN=Test CA\", " +
                "\"/DC=org/DC=example/DC=test-ca/CN=Other CA\"");
        IGTFInfo policy = builder.build();

        assertThat(policy.getAlias(), is(equalTo(null)));
        assertThat(policy.getCAUrl(), is(equalTo(null)));
        assertThat(policy.getCRLUrls(), is(empty()));
        assertThat(policy.getEmail(), is(equalTo(null)));
        assertThat(policy.getObsoletes(), is(equalTo(null)));
        assertThat(policy.getPolicyUrl(), is(equalTo(null)));
        assertThat(policy.getPolicyRequires(), hasEntry("TestService", "1.78-1"));
        assertThat(policy.getSHA1FP0(), is(equalTo(null)));
        assertThat(policy.getStatus(), is(equalTo(null)));
        assertThat(policy.getSubjectDNs(), hasItems(
                new GlobusPrincipal("/DC=org/DC=example/DC=test-ca/CN=Test CA"),
                new GlobusPrincipal("/DC=org/DC=example/DC=test-ca/CN=Other CA")));
        assertThat(policy.getUrl(), is(equalTo(null)));
        assertThat(policy.getVersion(), is(equalTo(new Version("1.78-1"))));
    }

    @Test
    public void shouldBuildFullValidProfile() throws Exception
    {
        IGTFInfo.Builder builder = IGTFInfo.builder(POLICY);
        builder.setAlias("ca-policy-test");
        builder.setEmail("test-ca@example.org");
        builder.setObsoletes("AIST, APAC");
        builder.setRequires("TestService = 1.78-1, TestOther = 2.89-3");
        builder.setSubjectDN("\"/DC=org/DC=example/DC=test-ca/CN=Test CA\", " +
                "\"/DC=org/DC=example/DC=test-ca/CN=Other CA\"");
        builder.setUrl("http://www.test-ca.example.org");
        builder.setVersion("1.78-1");
        IGTFInfo policy = builder.build();

        assertThat(policy.getAlias(), is(equalTo("ca-policy-test")));
        assertThat(policy.getCAUrl(), is(equalTo(null)));
        assertThat(policy.getCRLUrls(), is(empty()));
        assertThat(policy.getEmail(), is(equalTo(URI.create("mailto:test-ca@example.org"))));
        assertThat(policy.getObsoletes(), hasItems("AIST", "APAC"));
        assertThat(policy.getPolicyUrl(), is(equalTo(null)));
        assertThat(policy.getPolicyRequires(), allOf(
                hasEntry("TestService", "1.78-1"),
                hasEntry("TestOther", "2.89-3")));
        assertThat(policy.getSHA1FP0(), is(equalTo(null)));
        assertThat(policy.getStatus(), is(equalTo(null)));
        assertThat(policy.getSubjectDNs(), hasItems(
                new GlobusPrincipal("/DC=org/DC=example/DC=test-ca/CN=Test CA"),
                new GlobusPrincipal("/DC=org/DC=example/DC=test-ca/CN=Other CA")));
        assertThat(policy.getUrl(), is(equalTo(URI.create("http://www.test-ca.example.org"))));
        assertThat(policy.getVersion(), is(equalTo(new Version("1.78-1"))));
    }

    @Test
    public void shouldBuildMinimumValidTrustAnchor() throws Exception
    {
        IGTFInfo.Builder builder = IGTFInfo.builder(TRUST_ANCHOR);
        builder.setAlias("ca-policy-test");
        builder.setVersion("1.78-1");
        builder.setEmail("ca@test-ca.example.org");
        builder.setStatus("experimental");
        builder.setRequires("TestService");
        builder.setSubjectDN("\"/DC=org/DC=example/DC=test-ca/CN=Test CA\"");
        IGTFInfo policy = builder.build();

        assertThat(policy.getAlias(), is(equalTo("ca-policy-test")));
        assertThat(policy.getCAUrl(), is(equalTo(null)));
        assertThat(policy.getCRLUrls(), is(empty()));
        assertThat(policy.getEmail(), is(equalTo(URI.create("mailto:ca@test-ca.example.org"))));
        assertThat(policy.getObsoletes(), is(equalTo(null)));
        assertThat(policy.getPolicyUrl(), is(equalTo(null)));
        assertThat(policy.getTrustAnchorRequires(), hasItem("TestService"));
        assertThat(policy.getSHA1FP0(), is(equalTo(null)));
        assertThat(policy.getStatus(), is(equalTo(Status.EXPERIMENTAL)));
        assertThat(policy.getSubjectDN(),
                is(equalTo(new GlobusPrincipal("/DC=org/DC=example/DC=test-ca/CN=Test CA"))));
        assertThat(policy.getUrl(), is(equalTo(null)));
        assertThat(policy.getVersion(), is(equalTo(new Version("1.78-1"))));
    }

}
