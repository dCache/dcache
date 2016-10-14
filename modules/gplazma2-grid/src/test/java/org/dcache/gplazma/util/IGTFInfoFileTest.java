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

import org.junit.Test;

import static org.junit.Assert.*;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import static org.hamcrest.Matchers.*;

import org.junit.Before;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import org.dcache.gplazma.util.IGTFInfo.Status;
import org.dcache.gplazma.util.IGTFInfo.Version;

import static org.dcache.gplazma.util.IGTFInfo.Type.POLICY;
import static org.dcache.gplazma.util.IGTFInfo.Type.TRUST_ANCHOR;


public class IGTFInfoFileTest
{
    private FileSystem fs;
    private Path file;
    private IGTFInfoFile infoFile;
    private Optional<IGTFInfo> optionalInfo;
    private IGTFInfo info;

    @Before
    public void setup() throws Exception
    {
        fs = Jimfs.newFileSystem(Configuration.unix());
    }

    @Test
    public void shouldParseSimplifiedPolicyLcg() throws Exception
    {
        givenPolicyWithContent(
                "# @(#)ca-policy-lcg.info",
                "# auto-generated on 20161005",
                "alias = ca-policy-lcg",
                "version = 1.78-1",
                "requires = \\",
                "  AAACertificateServices = 1.78-1, \\",
                "  AEGIS = 1.78-1",
                "subjectdn = \\",
                "  \"/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services\", \\",
                "  \"/C=RS/O=AEGIS/CN=AEGIS-CA\"",
                "obsoletes = \\",
                "  AIST, \\",
                "  APAC");

        whenReadFile();

        assertThat(optionalInfo.isPresent(), is(true));
        assertThat(info.getType(), is(equalTo(POLICY)));
        assertThat(info.getAlias(), is(equalTo("ca-policy-lcg")));
        assertThat(info.getVersion(), is(equalTo(new IGTFInfo.Version("1.78-1"))));
        assertThat(info.getPolicyRequires(), allOf(
                hasEntry("AAACertificateServices", "1.78-1"),
                hasEntry("AEGIS", "1.78-1")));
        assertThat(info.getSubjectDNs(), hasItems(
                new GlobusPrincipal("/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services"),
                new GlobusPrincipal("/C=RS/O=AEGIS/CN=AEGIS-CA")));
        assertThat(info.getObsoletes(), hasItems("AIST", "APAC"));
    }

    @Test
    public void shouldParseMicsPolicyLcg() throws Exception
    {
        givenPolicyWithContent(
                "# @(#)policy-igtf-mics.info - IGTF mics authorities",
                "# Generated Wednesday, 05 Oct, 2016",
                "version = 1.78",
                "requires = AAACertificateServices = 1.78, \\",
                "    cilogon-silver = 1.78, \\",
                "    TERENAeSciencePersonalCA = 1.78, \\",
                "    TERENAeSciencePersonalCA2 = 1.78, \\",
                "    UTNAAAClient = 1.78, \\",
                "    TERENAeSciencePersonalCA3 = 1.78, \\",
                "    HPCI = 1.78",
                "subjectdn = \"/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services\", \\",
                "    \"/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Silver CA 1\", \\",
                "    \"/C=NL/O=TERENA/CN=TERENA eScience Personal CA\", \\",
                "    \"/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 2\", \\",
                "    \"/C=US/ST=UT/L=Salt Lake City/O=The USERTRUST Network/OU=http://www.usertrust.com/CN=UTN-USERFirst-Client Authentication and Email\", \\",
                "    \"/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 3\", \\",
                "    \"/C=JP/O=NII/OU=HPCI/CN=HPCI CA\"",
                "obsoletes = TACC-MICS, \\",
                "    NCSA-mics");

        whenReadFile();

        assertThat(optionalInfo.isPresent(), is(true));
        assertThat(info.getType(), is(equalTo(POLICY)));
        assertThat(info.getAlias(), is(equalTo(null)));
        assertThat(info.getVersion(), is(equalTo(new IGTFInfo.Version("1.78"))));
        assertThat(info.getPolicyRequires(), allOf(
                hasEntry("AAACertificateServices", "1.78"),
                hasEntry("cilogon-silver", "1.78"),
                hasEntry("TERENAeSciencePersonalCA", "1.78"),
                hasEntry("TERENAeSciencePersonalCA2", "1.78"),
                hasEntry("UTNAAAClient", "1.78"),
                hasEntry("TERENAeSciencePersonalCA3", "1.78"),
                hasEntry("HPCI", "1.78")));
        assertThat(info.getSubjectDNs(), hasItems(
                new GlobusPrincipal("/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services"),
                new GlobusPrincipal("/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Silver CA 1"),
                new GlobusPrincipal("/C=NL/O=TERENA/CN=TERENA eScience Personal CA"),
                new GlobusPrincipal("/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 2"),
                new GlobusPrincipal("/C=US/ST=UT/L=Salt Lake City/O=The USERTRUST Network/OU=http://www.usertrust.com/CN=UTN-USERFirst-Client Authentication and Email"),
                new GlobusPrincipal("/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 3"),
                new GlobusPrincipal("/C=JP/O=NII/OU=HPCI/CN=HPCI CA")));
        assertThat(info.getObsoletes(), hasItems("TACC-MICS", "NCSA-mics"));
    }

    @Test
    public void shouldParseGermanGrid() throws Exception
    {
        givenTrustAnchorWithContent(
                "#",
                "# @(#)$Id: dd4b34ea.info,v 1.6 2015/06/18 09:44:54 pmacvsdg Exp $",
                "# Information for CA GermanGrid",
                "#   obtained from dd4b34ea in GermanGrid/",
                "alias = GermanGrid",
                "url = http://grid.fzk.de/cgi-bin/welcome_ca.pl",
                "crl_url = http://gridka-ca.kit.edu/crl/gridka-crl.pem",
                "email = GridKa-CA@kit.edu",
                "status = accredited:classic",
                "version = 1.78",
                "sha1fp.0 = 82:A7:F9:7C:39:CD:21:18:9E:0E:39:27:51:D6:05:AC:A7:F6:BD:BD",
                "subjectdn = \"/C=DE/O=GermanGrid/CN=GridKa-CA\"");

        whenReadFile();

        assertThat(optionalInfo.isPresent(), is(true));
        assertThat(info.getType(), is(equalTo(TRUST_ANCHOR)));
        assertThat(info.getAlias(), is(equalTo("GermanGrid")));
        assertThat(info.getUrl(), is(equalTo(URI.create("http://grid.fzk.de/cgi-bin/welcome_ca.pl"))));
        assertThat(info.getCRLUrls(), hasItem(URI.create("http://gridka-ca.kit.edu/crl/gridka-crl.pem")));
        assertThat(info.getEmail(), is(equalTo(URI.create("mailto:GridKa-CA@kit.edu"))));
        assertThat(info.getStatus(), is(equalTo(Status.ACCREDITED_CLASSIC)));
        assertThat(info.getVersion(), is(equalTo(new Version("1.78"))));
        assertThat(info.getSHA1FP0(), is(equalTo(new BigInteger("82A7F97C39CD21189E0E392751D605ACA7F6BDBD", 16))));
        assertThat(info.getSubjectDN(), is(equalTo(new GlobusPrincipal("/C=DE/O=GermanGrid/CN=GridKa-CA"))));
    }

    private void givenPolicyWithContent(String... lines) throws IOException
    {
        file = fs.getPath("/etc/grid-security/certificates/policy-example.info");
        withContents(lines);
    }

    private void givenTrustAnchorWithContent(String... lines) throws IOException
    {
        file = fs.getPath("/etc/grid-security/certificates/trust-anchor.info");
        withContents(lines);
    }

    private void withContents(String... lines) throws IOException
    {
        Files.createDirectories(file.getParent());
        Files.write(file, Arrays.asList(lines));
        infoFile = new IGTFInfoFile(file);
    }

    private void whenReadFile() throws IOException, IGTFInfo.ParserException
    {
        optionalInfo = infoFile.get();
        info = optionalInfo.orElse(null);
    }
}
