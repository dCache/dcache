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

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import org.dcache.auth.IGTFPolicyPrincipal;
import org.dcache.auth.IGTFStatusPrincipal;
import org.dcache.auth.LoA;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;

public class IGTFInfoDirectoryTest
{
    private FileSystem fs;
    private Path directory;
    private IGTFInfoDirectory info;
    private Set<Principal> caPrincipals;

    @Before
    public void setup() throws Exception
    {
        fs = Jimfs.newFileSystem(Configuration.unix());
        directory = fs.getPath("/etc/grid-security/certificates");
        Files.createDirectories(directory);
        info = new IGTFInfoDirectory(directory);
    }

    @Test
    public void shouldFindComodoAAAWithMicsAndMicsPolicy() throws Exception
    {
        givenIGTFPolicySubset();

        whenLoginWithCa("/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services");

        assertThat(caPrincipals, hasItem(new IGTFStatusPrincipal("mics", true, Optional.of(LoA.IGTF_AP_MICS))));
        assertThat(caPrincipals, hasItem(new IGTFPolicyPrincipal("igtf-mics")));
        assertThat(caPrincipals, hasItem(new IGTFPolicyPrincipal("ca-policy-lcg")));
    }

    private void givenIGTFPolicySubset() throws IOException
    {
        givenFile("policy-igtf-lcg.info",
                "# NOTE: this file is based on the real file but has been ",
                "#       edited for brevity.",
                "alias = ca-policy-lcg",
                "version = 1.78-1",
                "requires = \\",
                "  AAACertificateServices = 1.78-1, \\",
                "  AEGIS = 1.78-1, \\",
                "  pkIRISGrid = 1.78-1, \\",
                "  seegrid-ca-2013 = 1.78-1",
                "subjectdn = \\",
                "  \"/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services\", \\",
                "  \"/C=JP/O=NII/OU=HPCI/CN=HPCI CA\", \\",
                "  \"/C=NL/O=TERENA/CN=TERENA eScience Personal CA\", \\",
                "  \"/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 2\", \\",
                "  \"/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 3\", \\",
                "  \"/C=US/ST=UT/L=Salt Lake City/O=The USERTRUST Network/OU=http://www.usertrust.com/CN=UTN-USERFirst-Client Authentication and Email\", \\",
                "  \"/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Silver CA 1\"",
                "obsoletes = \\",
                "  AIST, \\",
                "  APAC, \\",
                "  UniandesCA, \\",
                "  ncsa-gridshib-ca");
        givenFile("policy-igtf-mics.info",
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
        givenFile("AAACertificateServices.info",
                "#",
                "# @(#)$Id: 75680d2e.info,v 1.4 2015/06/18 09:44:56 pmacvsdg Exp $",
                "# Information for CA AAACertificateServices",
                "#   ",
                "alias = AAACertificateServices",
                "url = https://www.terena.org/activities/tcs/",
                "ca_url =  http://crt.comodoca.com/AAACertificateServices.crt",
                "crl_url = http://crl.comodoca.com/AAACertificateServices.crl",
                "policy_url = https://www.terena.org/activities/tcs/repository/",
                "email = tcs-pma@terena.org",
                "status = accredited:mics",
                "version = 1.78",
                "sha1fp.0 = D1:EB:23:A4:6D:17:D6:8F:D9:25:64:C2:F1:F1:60:17:64:D8:E3:49",
                "subjectdn = \"/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services\"");
        givenFile("cilogon-silver.info",
                "alias = cilogon-silver",
                "url = http://ca.cilogon.org/",
                "ca_url = https://cilogon.org/cilogon-silver.pem",
                "crl_url = http://crl-cilogon.ncsa-security.net/cilogon-silver.crl;http://crl.cilogon.org/cilogon-silver.crl",
                "email = ca@cilogon.org",
                "status = accredited:mics",
                "version = 1.78",
                "sha1fp.0 = 39:1C:E0:48:9A:BB:B1:0A:DA:DF:DD:A6:7C:C2:96:87:1A:83:6F:92",
                "subjectdn = \"/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Silver CA 1\"");
        givenFile("TERENAeSciencePersonalCA.info",
                "#",
                "# @(#)$Id: 169d7f9c.info,v 1.4 2015/06/18 09:44:56 pmacvsdg Exp $",
                "# Information for CA TERENAeSciencePersonalCA",
                "#   ",
                "alias = TERENAeSciencePersonalCA",
                "url = https://www.terena.org/activities/tcs/",
                "ca_url = http://crt.tcs.terena.org/TERENAeSciencePersonalCA.crt",
                "crl_url = http://crl.tcs.terena.org/TERENAeSciencePersonalCA.crl",
                "policy_url = https://www.terena.org/activities/tcs/repository/",
                "requires = UTNAAAClient",
                "email = tcs-pma@terena.org",
                "status = accredited:mics",
                "version = 1.78",
                "sha1fp.0 = 7C:F0:F3:2C:72:04:4A:01:7E:7C:87:13:68:94:75:57:92:37:A5:BD",
                "subjectdn = \"/C=NL/O=TERENA/CN=TERENA eScience Personal CA\"");
        givenFile("UTNAAAClient.info",
                "#",
                "# @(#)$Id: 9ec3a561.info,v 1.4 2015/06/18 09:44:56 pmacvsdg Exp $",
                "# Information for CA UTNAAAClient",
                "#   ",
                "alias = UTNAAAClient",
                "url = https://www.terena.org/activities/tcs/",
                "ca_url = http://crt.usertrust.com/UTNAAAClient_CA.crt",
                "crl_url = http://crl.usertrust.com/UTN-USERFirst-ClientAuthenticationandEmail.crl",
                "policy_url = https://www.terena.org/activities/tcs/repository/",
                "requires = AAACertificateServices",
                "email = tcs-pma@terena.org",
                "status = accredited:mics",
                "version = 1.78",
                "sha1fp.0 = E6:A6:7A:FD:55:3B:5B:CB:E9:01:AA:B1:0F:A9:4C:A8:25:66:BC:27",
                "subjectdn = \"/C=US/ST=UT/L=Salt Lake City/O=The USERTRUST Network/OU=http://www.usertrust.com/CN=UTN-USERFirst-Client Authentication and Email\"");
        givenFile("TERENAeSciencePersonalCA2.info",
                "# @(#)$Id: ac2d1719.info,v 1.2 2015/06/18 09:44:56 pmacvsdg Exp $",
                "# /C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience SSL CA 2",
                "alias = TERENAeSciencePersonalCA2",
                "requires = UTN-USERTrust-RSA-CA",
                "url = https://www.terena.org/activities/tcs/",
                "ca_url = http://crt.tcs.terena.org/TERENAeSciencePersonalCA2.crt",
                "crl_url = http://crl.tcs.terena.org/TERENAeSciencePersonalCA2.crl",
                "policy_url = https://www.terena.org/activities/tcs/repository/",
                "email = tcs-pma@terena.org",
                "status = accredited:mics",
                "version = 1.78",
                "sha1fp.0 = 8A:34:55:B4:DE:A6:5D:C4:B2:52:D2:94:52:40:CC:07:0C:C7:D4:E4",
                "subjectdn = \"/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 2\"");
        givenFile("TERENAeSciencePersonalCA3.info",
                "#",
                "# @(#)$Id: e732ef10.info,v 1.3 2015/06/18 09:44:56 pmacvsdg Exp $",
                "# Information for C=NL, ST=Noord-Holland, L=Amsterdam, O=TERENA, CN=TERENA eScience Personal CA 3",
                "#",
                "alias = TERENAeSciencePersonalCA3",
                "url = https://www.terena.org/activities/tcs/",
                "ca_url = http://cacerts.digicert.com/TERENAeSciencePersonalCA3.crt",
                "crl_url = http://crl4.digicert.com/TERENAeSciencePersonalCA3.crl;http://crl3.digicert.com/TERENAeSciencePersonalCA3.crl",
                "policy_url = https://www.terena.org/activities/tcs/repository/",
                "requires = DigiCertAssuredIDRootCA-Root",
                "email = tcs-pma@terena.org",
                "status = accredited:mics",
                "version = 1.78",
                "sha1fp.0 = B1:3C:DB:C0:6C:11:38:35:C9:54:20:F7:A0:D7:8F:51:34:6D:72:7E",
                "subjectdn = \"/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 3\"");
        givenFile("HPCI.info",
                "#",
                "# @(#)$Id: 61cd35bd.info,v 1.3 2015/06/18 09:44:53 pmacvsdg Exp $",
                "#",
                "alias = HPCI",
                "crl_url = http://www.hpci.nii.ac.jp/ca/hpcica.crl",
                "ca_url = https://www.hpci.nii.ac.jp/ca/hpcica.pem",
                "email = hpci-ca-support@nii.ac.jp",
                "status = accredited:mics",
                "url = https://www.hpci.nii.ac.jp/ca/",
                "policy_url = https://www.hpci.nii.ac.jp/ca/CP_CPS.html",
                "version = 1.78",
                "sha1fp.0 = 6D:75:9E:4D:E2:65:8D:BA:98:BD:3E:89:53:3E:0A:2F:D4:DB:38:34",
                "subjectdn = \"/C=JP/O=NII/OU=HPCI/CN=HPCI CA\"");
    }

    private void givenFile(String name, String... contents) throws IOException
    {
        Path file = directory.resolve(name);
        Files.write(file, asList(contents));
    }

    private void whenLoginWithCa(String ca)
    {
        caPrincipals = info.getPrincipals(new GlobusPrincipal(ca));
    }
}
