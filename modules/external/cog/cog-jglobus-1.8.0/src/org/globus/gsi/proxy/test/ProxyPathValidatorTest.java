/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi.proxy.test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.security.cert.X509Certificate;
import java.io.File;
import java.security.cert.X509CRL;
import org.globus.gsi.CertUtil;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.SigningPolicy;
import org.globus.gsi.SigningPolicyParser;
import org.globus.gsi.CertificateRevocationLists;
import org.globus.gsi.proxy.ProxyPolicyHandler;
import org.globus.gsi.proxy.ProxyPathValidator;
import org.globus.gsi.proxy.ProxyPathValidatorException;
import org.globus.gsi.proxy.ext.ProxyCertInfo;
import org.globus.gsi.proxy.ext.ProxyPolicy;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class ProxyPathValidatorTest extends TestCase {
    
    public static final String BASE = "org/globus/gsi/proxy/test/";
    
    public static String [][] certs = { 
	{String.valueOf(GSIConstants.CA), "globusca.pem"},
	{String.valueOf(GSIConstants.EEC), "usercert.pem"},
	{String.valueOf(GSIConstants.GSI_2_PROXY), "gsi2fullproxy.pem"},
	{String.valueOf(GSIConstants.GSI_2_LIMITED_PROXY), "gsi2limitedproxy.pem"},
	// double
	{String.valueOf(GSIConstants.GSI_2_LIMITED_PROXY), "gsi2limited2xproxy.pem"},
	{String.valueOf(GSIConstants.GSI_3_IMPERSONATION_PROXY), "gsi3impersonationproxy.pem"},
	{String.valueOf(GSIConstants.GSI_3_INDEPENDENT_PROXY), "gsi3independentproxy.pem"},
	{String.valueOf(GSIConstants.GSI_3_LIMITED_PROXY), "gsi3limitedproxy.pem"},
	{String.valueOf(GSIConstants.GSI_3_RESTRICTED_PROXY), "gsi3restrictedproxy.pem"},
	// double
	{String.valueOf(GSIConstants.GSI_3_IMPERSONATION_PROXY), "gsi3impersonation2xproxy.pem"},
	{String.valueOf(GSIConstants.GSI_3_INDEPENDENT_PROXY), "gsi3independent2xproxy.pem"},
	// pathLen = 0
	{String.valueOf(GSIConstants.GSI_3_IMPERSONATION_PROXY), "gsi3impersonationp0proxy.pem"},
	// pathLen = 1
	{String.valueOf(GSIConstants.GSI_3_INDEPENDENT_PROXY), "gsi3independentp1proxy.pem"},
	// pathLen = 2
	{String.valueOf(GSIConstants.CA), "testca.pem"},
	{String.valueOf(GSIConstants.EEC), "testeec1.pem"},
	{String.valueOf(GSIConstants.EEC), "testeec2.pem"},
	// pathLen = 1
	{String.valueOf(GSIConstants.CA), "testca2.pem"},
	{String.valueOf(GSIConstants.GSI_3_IMPERSONATION_PROXY), "testgsi3proxy.pem"},
	// for CRL test
	{String.valueOf(GSIConstants.CA), "testca3.pem"},
	{String.valueOf(GSIConstants.EEC), "crl_usercert.pem"},
	{String.valueOf(GSIConstants.GSI_2_PROXY), 
	 "crl_proxy.pem"},
	// 21 (all good)
	{String.valueOf(GSIConstants.CA), "ca1cert.pem"},
	{String.valueOf(GSIConstants.EEC), "user1ca1.pem"},
	{String.valueOf(GSIConstants.EEC), "user2ca1.pem"},
	{String.valueOf(GSIConstants.EEC), "user3ca1.pem"},
	// 25
	{String.valueOf(GSIConstants.CA), "ca2cert.pem"},
        // must be revoked (in ca2crl.r0)
	{String.valueOf(GSIConstants.EEC), "user1ca2.pem"}, 
        // must be revoked (in ca2crl.r0)
	{String.valueOf(GSIConstants.EEC), "user2ca2.pem"}, 
	{String.valueOf(GSIConstants.EEC), "user3ca2.pem"}
    };
    
    public static String [] badCerts = {
	"-----BEGIN CERTIFICATE-----\n" + 
	"MIICFTCCAX6gAwIBAgIDClb3MA0GCSqGSIb3DQEBBAUAMGIxCzAJBgNVBAYTAlVT\n" +
	"MQ8wDQYDVQQKEwZHbG9idXMxJDAiBgNVBAoTG0FyZ29ubmUgTmF0aW9uYWwgTGFi\n" +
	"b3JhdG9yeTEMMAoGA1UECxMDTUNTMQ4wDAYDVQQDEwVnYXdvcjAeFw0wMjEyMTgw\n" +
	"NzEzNDhaFw0wMjEyMTgxOTE4NDhaMIGCMQswCQYDVQQGEwJVUzEPMA0GA1UEChMG\n" +
	"R2xvYnVzMSQwIgYDVQQKExtBcmdvbm5lIE5hdGlvbmFsIExhYm9yYXRvcnkxDDAK\n" +
	"BgNVBAsTA01DUzEOMAwGA1UEAxMFZ2F3b3IxDjAMBgNVBAMTBXByb3h5MQ4wDAYD\n" +
	"VQQDEwVwcm94eTBaMA0GCSqGSIb3DQEBAQUAA0kAMEYCQQCplfu3OZH5AfYgoYKi\n" +
	"KFmGZnbj3+ZwJm45B6Ef7qwW7Le7FP4eirljObqijgn8ao0gGqy38LYbaTntToqX\n" +
	"iy5fAgERMA0GCSqGSIb3DQEBBAUAA4GBAKnNy0VPDzzD6++7i9a/yegPX2+OVI6C\n" +
	"7oss1/4sSw2gfn/q8qNiGdt1kr4W3JJACdjgnik8fokNS7pDMdXKi3Wx6E0HhgKz\n" +
	"eRIm5r6Vj7nshVBAv60Xmfju3yaOZsDnj8p0t8Fjc8ekeZowLEdRn7PCEQPylMOp\n" +
	"2puR03MaPiFj\n" +
	"-----END CERTIFICATE-----"
	,
	"-----BEGIN CERTIFICATE-----\n" + 
	"MIICBDCCAW2gAwIBAgIDAx4rMA0GCSqGSIb3DQEBBAUAMGIxCzAJBgNVBAYTAlVT\n" +
	"MQ8wDQYDVQQKEwZHbG9idXMxJDAiBgNVBAoTG0FyZ29ubmUgTmF0aW9uYWwgTGFi\n" +
	"b3JhdG9yeTEMMAoGA1UECxMDTUNTMQ4wDAYDVQQDEwVnYXZvcjAeFw0wMjEyMTgw\n" +
	"NzIxMThaFw0wMjEyMTgxOTI2MThaMHIxCzAJBgNVBAYTAlVTMQ8wDQYDVQQKEwZH\n" +
	"bG9idXMxJDAiBgNVBAoTG0FyZ29ubmUgTmF0aW9uYWwgTGFib3JhdG9yeTEMMAoG\n" +
	"A1UECxMDTUNTMQ4wDAYDVQQDEwVnYXdvcjEOMAwGA1UEAxMFcHJveHkwWjANBgkq\n" +
	"hkiG9w0BAQEFAANJADBGAkEAx2fp80b+Yo0zCwjYJdIjzn0N3ezzcD2h2bAr/Nop\n" +
	"w/H6JB4heiVGMeydMlSJHyI7J/s5l8k39G/KVrBGT9tRJwIBETANBgkqhkiG9w0B\n" +
	"AQQFAAOBgQCRRvTdW6Ddn1curWm515l/GoAoJ76XBFJWfusIZ9TdwE8hlkRpK9Bd\n" +
	"Rrao4Z2YO+e3UItn45Hs+8gzx+jBB1AduTUor603Z8AXaNbF/c+gz62lBWlcmZ2Y\n" +
	"LzuUWgwZLd9HdA2YBgCcT3B9VFmBxcnPjGOwWT29ZUtyy2GXFtzcDw==\n" +
	"-----END CERTIFICATE-----"
    };

    public static String [] testCerts = {
	"-----BEGIN CERTIFICATE-----\n" + 
	"MIIB7zCCAVigAwIBAgICAbowDQYJKoZIhvcNAQEEBQAwVzEbMBkGA1UEChMSZG9l\n" +
	"c2NpZW5jZWdyaWQub3JnMQ8wDQYDVQQLEwZQZW9wbGUxJzAlBgNVBAMTHlZpamF5\n" +
	"YSBMYWtzaG1pIE5hdGFyYWphbiAxNzkwODAeFw0wMzAxMTcyMjExMjJaFw0wMzAx\n" +
	"MTgxMDE2MjJaMGcxGzAZBgNVBAoTEmRvZXNjaWVuY2VncmlkLm9yZzEPMA0GA1UE\n" +
	"CxMGUGVvcGxlMScwJQYDVQQDEx5WaWpheWEgTGFrc2htaSBOYXRhcmFqYW4gMTc5\n" +
	"MDgxDjAMBgNVBAMTBXByb3h5MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBANGP+xct\n" +
	"lDYMPm11QKnACvqs95fbPRehvUi6/dizZ+VrDOU1OTUoXA0t6HRgtmJ8XthEUKxU\n" +
	"MVsxjXtoZOzfuFECAwEAATANBgkqhkiG9w0BAQQFAAOBgQBqFTcN/qqvTnyI4z26\n" +
	"lv1lMTuRIjL9l6Ug/Kwxuzjpl088INky1myFPjKsWMYzh9nXIQg9gg2dJTno5JHB\n" +
	"++u0Fw2iNrTjswu4hvqYZn+LoSGchH2XyCUssuOWCbW4IkN8/Xzfre2oC2EieECC\n" +
	"w+jjGhcqPrxvkHh8xXYroqA0Sg==\n" +
	"-----END CERTIFICATE-----"
	,
	"-----BEGIN CERTIFICATE-----\n" + 
	"MIIDLDCCAhSgAwIBAgICAbowDQYJKoZIhvcNAQEFBQAwdTETMBEGCgmSJomT8ixk\n" +
	"ARkWA25ldDESMBAGCgmSJomT8ixkARkWAmVzMSAwHgYDVQQLExdDZXJ0aWZpY2F0\n" +
	"ZSBBdXRob3JpdGllczEZMBcGA1UECxMQRE9FIFNjaWVuY2UgR3JpZDENMAsGA1UE\n" +
	"AxMEcGtpMTAeFw0wMjA5MjMyMzQ2NDRaFw0wMzA5MjMyMzQ2NDRaMFcxGzAZBgNV\n" +
	"BAoTEmRvZXNjaWVuY2VncmlkLm9yZzEPMA0GA1UECxMGUGVvcGxlMScwJQYDVQQD\n" +
	"Ex5WaWpheWEgTGFrc2htaSBOYXRhcmFqYW4gMTc5MDgwgZ8wDQYJKoZIhvcNAQEB\n" +
	"BQADgY0AMIGJAoGBAORYHsPQU3yVlTsC/29CDoEYF82PVlolQk5s+1m6A7m3VvML\n" +
	"TKh4ja6cKtq7C5rBUIWdyklkU3eXSSmiAzjJrVOmfWK3RR465A5tfvJLmXKWaq3U\n" +
	"7SvI6v3vx4Jzy4MJs46TDAr4v9JRJG2yshoxruRy2gDsn4F5NfLLevDNwzSLAgMB\n" +
	"AAGjaDBmMBEGCWCGSAGG+EIBAQQEAwIF4DAOBgNVHQ8BAf8EBAMCBPAwHwYDVR0j\n" +
	"BBgwFoAUVBeIygPBOSa4VabEmfQrAqu+AOkwIAYDVR0RBBkwF4EVdmlqYXlhbG5A\n" +
	"bWF0aC5sYmwuZ292MA0GCSqGSIb3DQEBBQUAA4IBAQC/dxf5ZuSrNrxslHUZfDle\n" +
	"V8SPnX5roBUOuO2EPpEGYHB25Ca+TEi0ra0RSRuZfGmY13/aS6CzjBF+6GED9MLo\n" +
	"6UdP1dg994wpGZ2Mj0dZoGE7we10NrSvFAS3u7uXrTTegeJoDpo1k9YVsOkK9Lu9\n" +
	"Sg+EztnMGa1BANWf779Qws5J9xUR2Nip0tBkV3IRORcBx0CoZzQnDIWyppmnkza2\n" +
	"mhgEv6CXYYB4ucCFst0P2Q3omcWrtHexoueMGOV6PtLFBst5ReOaZWU+q2D30t3b\n" +
	"GFITa0aayXTlb6gWgo3z/O/K5GZS5jF+BA3j1e8IhxqeibT1rVHF4W4ZMjGhBcwa\n" +
	"-----END CERTIFICATE-----" 
	,
	"-----BEGIN CERTIFICATE-----\n" +
	"MIIEqjCCBBOgAwIBAgIBLzANBgkqhkiG9w0BAQUFADBbMRkwFwYDVQQKExBET0Ug\n" +
	"U2NpZW5jZSBHcmlkMSAwHgYDVQQLExdDZXJ0aWZpY2F0ZSBBdXRob3JpdGllczEc\n" +
	"MBoGA1UEAxMTQ2VydGlmaWNhdGUgTWFuYWdlcjAeFw0wMTEyMjEyMzQ4MzdaFw0w\n" +
	"NDAxMTAyMzQ4MzdaMHUxEzARBgoJkiaJk/IsZAEZFgNuZXQxEjAQBgoJkiaJk/Is\n" +
	"ZAEZFgJlczEgMB4GA1UECxMXQ2VydGlmaWNhdGUgQXV0aG9yaXRpZXMxGTAXBgNV\n" +
	"BAsTEERPRSBTY2llbmNlIEdyaWQxDTALBgNVBAMTBHBraTEwggEiMA0GCSqGSIb3\n" +
	"DQEBAQUAA4IBDwAwggEKAoIBAQDhgzoAt5viFffXWG6P0KSf/dO0mrEbgpuKIHDa\n" +
	"RdHkxJGaoBgRO2D+YV4Wh+JcKlz64v2ScYHCgGbKoaE+cGM/O06xkLCV0pyT4Xvj\n" +
	"6/R80jqwzzRw8aYz9iE/wjljK1ehb+oJ6TJlnotCVBd7TlHODYfXXblt67/Uk1uu\n" +
	"4l17jCdfk4mUn/2Bdeae4EMibj7Vc1dkPkyY47ZADTeFXMNDyp4yGFeIDZQ6h+YH\n" +
	"27+t1/TDuEH1R4PpklRpSbppGprI8hv2P6uEKTySjAEkww9xVzenN6oULeafFJuS\n" +
	"t6Ui6BFxc1OuxMq/s0PDiFh8bPMhzJWBfzaNPHnYrFDWcDwHAgMBAAGjggHeMIIB\n" +
	"2jAOBgNVHQ8BAf8EBAMCAYYwHQYDVR0OBBYEFFQXiMoDwTkmuFWmxJn0KwKrvgDp\n" +
	"MB8GA1UdIwQYMBaAFJvOT/K8vVhwMdXyMg5+nr3iURTnMA8GA1UdEwEB/wQFMAMB\n" +
	"Af8wgY8GA1UdHwSBhzCBhDCBgaAaoBiGFmh0dHA6Ly9lbnZpc2FnZS5lcy5uZXSB\n" +
	"AgDsol+kXTBbMRkwFwYDVQQKExBET0UgU2NpZW5jZSBHcmlkMSAwHgYDVQQLExdD\n" +
	"ZXJ0aWZpY2F0ZSBBdXRob3JpdGllczEcMBoGA1UEAxMTQ2VydGlmaWNhdGUgTWFu\n" +
	"YWdlcjCB5AYDVR0gBIHcMIHZMIHWBgoqhkiG90wDBgQBMIHHMF8GCCsGAQUFBwIC\n" +
	"MFMwJhYfRVNuZXQgKEVuZXJneSBTY2llbmNlcyBOZXR3b3JrKTADAgEBGilFU25l\n" +
	"dC1ET0UgU2NpZW5jZSBHcmlkIENlcnRpZmljYXRlIFBvbGljeTBkBggrBgEFBQcC\n" +
	"ARZYaHR0cDovL2VudmlzYWdlLmVzLm5ldC9FbnZpc2FnZSUyMERvY3MvRE9FU0cl\n" +
	"MjBDQSUyMENlcnRpZmljYXRlJTIwUG9saWN5JTIwYW5kJTIwQ1BTLnBkZjANBgkq\n" +
	"hkiG9w0BAQUFAAOBgQCaAdUregqwmCJG6j/h6uK2bTpcfa/SfpaYwsTy+zlf5r4P\n" +
	"iY/wIRN0ZjJ4RrJQ/WUH16onNwb87JnYe0V4JYhATAOnp/5y9kl+iC4XvHBioVxm\n" +
	"3sEADL40WAVREWBGZnyFqysXAEGfk+Wg7um5FzCwi6380GASKY0VujQG03f6Pg==\n" +
	"-----END CERTIFICATE-----" 
    };
    

    // Globus CA signing policy. Using globusca.pem and usercert.pem
    public static String signingPolicy = 
        "# Globus CA rights\naccess_id_CA      X509         '/C=US/O=Globus/CN=Globus Certification Authority'\npos_rights        globus        CA:sign\ncond_subjects     globus       '\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"'\n# End of ca-signing-policy.conf";

    // Globus CA signing policy that causes usercert.pem to violate
    // the policy
    public static String signingPolicyViolation = 
        "# Globus CA rights\naccess_id_CA      X509         '/C=US/O=Globus/CN=Globus Certification Authority'\npos_rights        globus        CA:sign\ncond_subjects     globus       '\"/C=usa/O=Globus/*\"  \"/C=USA/O=Globus/*\"'\n# End of ca-signing-policy.conf";

    // Globus CA signing policy without relevant signing policy
    public static String signingPolicySansPolicy = 
        "# Globus CA rights\naccess_id_CA      nonX509         '/C=US/O=Globus/CN=Globus Certification Authority'\npos_rights        globus        CA:sign\ncond_subjects     globus       '\"/C=usa/O=Globus/*\"  \"/C=USA/O=Globus/*\"'\n# End of ca-signing-policy.conf";

    public static X509Certificate [] goodCertsArr;

    static {
	try {
	    goodCertsArr = initCerts();
	} catch (Exception e) {
	    throw new RuntimeException("Failed to load certs: " + e.getMessage());
	}
    }

    public ProxyPathValidatorTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(ProxyPathValidatorTest.class);
    }

    public static X509Certificate[] initCerts() throws Exception {
	X509Certificate [] goodCertsArr = new X509Certificate[certs.length];
	ClassLoader loader = ProxyPathValidatorTest.class.getClassLoader();
	for (int i=0;i<certs.length;i++) {
	    String name = BASE + certs[i][1];
	    InputStream in = loader.getResourceAsStream(name);
	    if (in == null) {
		throw new Exception("Unable to load: " + name);
	    }
	    goodCertsArr[i] = CertUtil.loadCertificate(in);
	}
	return goodCertsArr;
    }

    public void testValidateGsi2PathGood() throws Exception {
	X509Certificate [] chain = null;

	// EEC, CA
	chain = new X509Certificate[] {goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], false);
	
	// proxy, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[2], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], false);

	// limited proxy, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[3], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], true);

	// double limited proxy, limited proxy, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[4], goodCertsArr[3], 
				       goodCertsArr[1], goodCertsArr[0]};
	
	validateChain(chain, goodCertsArr[1], true);
    }


    private void validateChain(X509Certificate [] chain,
			       X509Certificate expectedIdentity,
			       boolean expectedLimited) 
	throws Exception {
	TestProxyPathValidator v = new TestProxyPathValidator();
	v.validate(chain); 
	assertEquals(expectedLimited, v.isLimited());
	assertEquals(expectedIdentity, v.getIdentityCertificate());
    }

    private void validateRejectLimitedCheck() throws Exception {

	X509Certificate [] chain = null;
	// limited proxy, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[3], goodCertsArr[1], 
				       goodCertsArr[0]};
	TestProxyPathValidator v = new TestProxyPathValidator();
	v.setRejectLimitedProxyCheck(true);
	try {
	    v.validate(chain);
	    fail("Validation did not throw exception");
	} catch (ProxyPathValidatorException exp) {
	    exp.printStackTrace();
	    assertEquals(ProxyPathValidatorException.LIMITED_PROXY_ERROR,
			 exp.getErrorCode());
	}
	v.setRejectLimitedProxyCheck(false);
	v.validate(chain); 
	assertTrue(v.isLimited());

	// a proxy chain with no limited proxy
	chain = new X509Certificate[] {goodCertsArr[2], goodCertsArr[1], 
				       goodCertsArr[0]};
	TestProxyPathValidator v1 = new TestProxyPathValidator();
	v1.setRejectLimitedProxyCheck(true);
	v1.validate(chain);
	assertFalse(v1.isLimited());
	// Parnoid check.
	v1.setRejectLimitedProxyCheck(false);
	v1.validate(chain);
    }

    public void testValidateGsi3PathGood() throws Exception {
	X509Certificate [] chain = null;

	// GSI 3 PC impersonation, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[5], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], false);

	// GSI 3 PC independent, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[6], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[6], false);

	// GSI 3 PC imperson limited, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[7], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], true);

	// GSI 3 PC impersonation, GSI 3 PC limited impersonation, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[9], goodCertsArr[7], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], true);

	// GSI 3 PC impersonation, GSI 3 PC impersonation, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[9], goodCertsArr[5], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[1], false);

	// GSI 3 PC indepedent, GSI 3 PC independent, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[6], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[10], false);

	// GSI 3 PC impersonation, GSI 3 PC independent, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[9], goodCertsArr[6], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[6], false);

	// GSI 3 PC indepedent, GSI 3 PC limited impersonation, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[7], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, goodCertsArr[10], false);
    }

    public void testValidatePathWithRestrictedProxy() throws Exception {
	X509Certificate [] chain = null;

	// GSI 3 PC restricted, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[8], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, ProxyPathValidatorException.UNKNOWN_POLICY);

	// // GSI 3 PC impersonation, GSI 3 PC restricted, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[9], goodCertsArr[8], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, ProxyPathValidatorException.UNKNOWN_POLICY);

	TestProxyPathValidator v = new TestProxyPathValidator();
	v.setProxyPolicyHandler("1.3.6.1.4.1.3536.1.1.1.8", new ProxyPolicyHandler() {
		public void validate(ProxyCertInfo info, X509Certificate[] path, int index)
		    throws ProxyPathValidatorException {
		    ProxyPolicy policy = info.getProxyPolicy();
		    String pol = policy.getPolicyAsString();
		    assertEquals("<AllPermissions...>\r\n", pol);
		}
	    });
	chain = new X509Certificate[] {goodCertsArr[8], goodCertsArr[1], goodCertsArr[0]};
	v.validate(chain);
    }

    private void validateChain(X509Certificate [] chain)
	throws Exception {
	validateChain(chain, ProxyPathValidatorException.FAILURE);
    }

    private void validateChain(X509Certificate [] chain,
			       int expectedErrorCode) 
	throws Exception {
	TestProxyPathValidator v = new TestProxyPathValidator();
	try {
	    v.validate(chain);
	    fail("Did not throw exception as expected");
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    assertEquals(expectedErrorCode, e.getErrorCode());
	}
    }

    public void testValidatePathBad() throws Exception {
	X509Certificate [] chain = null;

	// proxy, CA
	chain = new X509Certificate[] {goodCertsArr[5], goodCertsArr[0]};
	validateChain(chain);

	// user, proxy, CA
	chain = new X509Certificate[] {goodCertsArr[1], goodCertsArr[2], goodCertsArr[0]};
	validateChain(chain);

	// user, user, CA
	chain = new X509Certificate[] {goodCertsArr[1], goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain);

	// user, CA, user
	chain = new X509Certificate[] {goodCertsArr[1], goodCertsArr[0], goodCertsArr[1]};
	validateChain(chain);
    }
    
    public void testValidatePathMixedProxy() throws Exception {
	X509Certificate [] chain = null;

	// GSI 3 PC, GSI 2 PC, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[6], goodCertsArr[2], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain);

	// GSI 2 PC, GSI 3 PC, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[2], goodCertsArr[6], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain);
    }

    public void testValidatePathProxyPathConstraint() throws Exception {
	X509Certificate [] chain = null;

	// GSI 3 PC pathlen=0, GSI 3 PC, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[11], goodCertsArr[10], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain,  goodCertsArr[10], false);

	// GSI 3 PC, GSI 3 PC pathlen=0, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[11], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain);


	// GSI 3 PC, GSI 3 PC pathlen=1, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[12], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain,  goodCertsArr[10], false);

	// GSI 3 PC, GSI 3 PC, GSI 3 PC pathlen=1, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[9], goodCertsArr[12], 
				       goodCertsArr[1], goodCertsArr[0]};
	validateChain(chain, ProxyPathValidatorException.PATH_LENGTH_EXCEEDED);
    }
    
    public void testValidatePathCAPathConstraint() throws Exception {
	X509Certificate [] chain = null;

	// should all be OK

	// EEC, CA (pathlen=0)
	chain = new X509Certificate[] {goodCertsArr[15], goodCertsArr[16]};
	validateChain(chain,  goodCertsArr[15], false);

	// GSI 2 limited PC, EEC, CA (pathlen=0)
	chain = new X509Certificate[] {goodCertsArr[3], goodCertsArr[15], goodCertsArr[16]};
	validateChain(chain,  goodCertsArr[15], true);

	// GSI 3 PC, EEC, CA (pathlen=0)
	chain = new X509Certificate[] {goodCertsArr[17], goodCertsArr[15], goodCertsArr[16]};
	validateChain(chain,  goodCertsArr[15], false);

	// GSI 2 limited PC, EEC, CA (pathlen=0), CA (pathlen=2), CA (pathlen=2)
	chain = new X509Certificate[] {goodCertsArr[3], goodCertsArr[15], 
				       goodCertsArr[16], goodCertsArr[13],
				       goodCertsArr[13]};
	validateChain(chain,  goodCertsArr[15], true);

	// these should fail

	// EEC, CA (pathlen=0), CA (pathlen=0)
	chain = new X509Certificate[] {goodCertsArr[15], goodCertsArr[16], goodCertsArr[16]};
	validateChain(chain, ProxyPathValidatorException.PATH_LENGTH_EXCEEDED);

	// GSI 2 limited PC, EEC, CA (pathlen=0), CA (pathlen=2), CA (pathlen=2), CA (pathlen=2)
	chain = new X509Certificate[] {goodCertsArr[3], goodCertsArr[15], 
				       goodCertsArr[16], goodCertsArr[13],
				       goodCertsArr[13], goodCertsArr[13]};
	validateChain(chain, ProxyPathValidatorException.PATH_LENGTH_EXCEEDED);

	// GSI 3 PC, GSI 3 PC pathlen=1, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[12], 
				       goodCertsArr[1], goodCertsArr[13]};
	validateChain(chain,  goodCertsArr[10], false);

	// GSI 3 PC, GSI 3 PC, GSI 3 PC pathlen=1, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[10], goodCertsArr[12], 
				       goodCertsArr[1], goodCertsArr[13]};
	validateChain(chain, ProxyPathValidatorException.PATH_LENGTH_EXCEEDED);

	// GSI 3 PC, GSI 3 PC pathlen=0, EEC, CA
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[11], 
				       goodCertsArr[1], goodCertsArr[13]};
	validateChain(chain,  ProxyPathValidatorException.FAILURE);
    }

     public void testValidateChain() throws Exception {
	X509Certificate [] chain = null;
	
	// everything ok chain. this also tests signing policy for the
	// credentials used to run the test, since actual proxy path
	// validator is used and not test.
	chain = GlobusCredential.getDefaultCredential().getCertificateChain();

	TrustedCertificates trusted = TrustedCertificates.getDefault();
	X509Certificate [] trustedCerts = trusted.getCertificates();
        SigningPolicy[] policies = trusted.getSigningPolicies();

	ProxyPathValidator v = new ProxyPathValidator();
	v.validate(chain, trustedCerts, null, policies);
	assertEquals(false, v.isLimited());
	assertEquals(chain[1], v.getIdentityCertificate());

	// unknown ca
	v.reset();
	try {
	    v.validate(chain, (X509Certificate[])null, null);
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    assertEquals(ProxyPathValidatorException.UNKNOWN_CA,
                         e.getErrorCode());
	}

	// issuer vs subject do not match
	chain = new X509Certificate[] {goodCertsArr[10], goodCertsArr[1], goodCertsArr[0]};
	v.reset();
	try {
	    v.validate(chain, new X509Certificate[] {goodCertsArr[0]}, null,
                       policies);
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    assertEquals(ProxyPathValidatorException.FAILURE, e.getErrorCode());
	}

	// proxy cert expired
	chain = new X509Certificate[] {goodCertsArr[3], goodCertsArr[1], goodCertsArr[0]};
	v.reset();
	try {
	    v.validate(chain, new X509Certificate[] {goodCertsArr[0]}, null,
                       policies);
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    assertEquals(ProxyPathValidatorException.FAILURE, e.getErrorCode());
	}
    }

    public void testKeyUsage() throws Exception {
	X509Certificate [] certsArr = new X509Certificate[testCerts.length];
	for (int i=0;i<certsArr.length;i++) {
	    certsArr[i] = 
		CertUtil.loadCertificate(new ByteArrayInputStream(testCerts[i].getBytes()));
	}

	X509Certificate [] chain = null;

	// certArr[1] - has key usage but certSign is off - but it sings proxy
	// certArr[2] - has key usage and certSing is on
	chain = new X509Certificate[] {certsArr[0], certsArr[1], certsArr[2]};
	validateChain(chain, certsArr[1], false);
    }

    public void testNoBasicConstraintsExtension() throws Exception {
	X509Certificate [] chain = null;
	// EEC, EEC, CA - that should fail
	chain = new X509Certificate[] {goodCertsArr[1], goodCertsArr[1], 
                                       goodCertsArr[0]};
	validateChain(chain);
        
	TestProxyPathValidator v = new TestProxyPathValidator();
	TrustedCertificates trustedCert =
	    new TrustedCertificates(new X509Certificate[] {goodCertsArr[1]},
                                    new SigningPolicy[] { 
                                        new SigningPolicy("foo", null)});
	
	// this makes the PathValidator think the chain is:
	// CA, CA, CA - which is ok. irrelevant to signing policy.
	try {
	    v.validate(chain, trustedCert);
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    fail("Unexpected exception: " + e.getMessage());
	}
    }

    // removed date validity check.
    // FIXME
    public void testCrlsChecks() throws Exception {

	X509Certificate[] chain = null;
	// chain of good certs
	chain = GlobusCredential.getDefaultCredential().getCertificateChain();
	CertificateRevocationLists certRevLists = 
	    CertificateRevocationLists.getCertificateRevocationLists("src" + 
				       File.separator + BASE + "testca3.rpem, "
				       + "src" + File.separator + BASE);
	assertTrue(certRevLists != null);

	TrustedCertificates trustedCerts = TrustedCertificates.getDefault();
	X509CRL[] crls = certRevLists.getCrls();
	assertTrue(crls != null);
        ProxyPathValidator validator = new ProxyPathValidator();
	try {
	    validator.validate(chain, 
			       trustedCerts.getCertificates(),
                               certRevLists,
                               trustedCerts.getSigningPolicies());
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    fail("Unexpected exception: " + e.getMessage());
	}

	validator.reset();

        // remove signing policy checks and validity checks
        TestProxyPathValidator tvalidator = new TestProxyPathValidator();

	// ca1 ca1user1 good chain
	chain = new X509Certificate[] {goodCertsArr[22], goodCertsArr[21]};
	certRevLists = 
	    CertificateRevocationLists.getCertificateRevocationLists("src" + 
				       File.separator + BASE + "testca3.rpem"
				       + ",src" + File.separator + BASE);
	try {
	    tvalidator.validate(chain, new X509Certificate[] 
                {goodCertsArr[21]}, certRevLists, 
                                trustedCerts.getSigningPolicies());
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    fail("Unexpected exception: " + e.getMessage());
	}

	tvalidator.reset();

	// ca1 ca1user2 good chain
	chain = new X509Certificate[] {goodCertsArr[23], goodCertsArr[21]};
	try {
	    tvalidator.validate(chain, new X509Certificate[]
                {goodCertsArr[21]}, certRevLists, 
                                trustedCerts.getSigningPolicies());
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    fail("Unexpected exception: " + e.getMessage());
	}

	tvalidator.reset();
		
	// ca2 user1 bad chain
       	chain = new X509Certificate[] {goodCertsArr[26], goodCertsArr[25]};
	try {
	    tvalidator.validate(chain, new X509Certificate[] 
                {goodCertsArr[25]}, certRevLists,
                                trustedCerts.getSigningPolicies());
            fail("Validation did not throw exception");
	} catch (ProxyPathValidatorException crlExp) {
	    crlExp.printStackTrace();
	    assertEquals(ProxyPathValidatorException.REVOKED, 
			 crlExp.getErrorCode());
	}

	tvalidator.reset();

	// ca2 user2 bad chain
       	chain = new X509Certificate[] {goodCertsArr[27], goodCertsArr[25]};
	try {
	    tvalidator.validate(chain, new X509Certificate[] 
                {goodCertsArr[25]}, certRevLists, 
                                trustedCerts.getSigningPolicies());
	    fail("Validation did not throw exception");
	} catch (ProxyPathValidatorException crlExp) {
	    crlExp.printStackTrace();
	    assertEquals(ProxyPathValidatorException.REVOKED, 
			 crlExp.getErrorCode());
	}

	tvalidator.reset();

	// ca2 user3 good chain
       	chain = new X509Certificate[] {goodCertsArr[28], goodCertsArr[25]};
	try {
	    tvalidator.validate(chain, new X509Certificate[] 
                {goodCertsArr[25]}, certRevLists,
                                trustedCerts.getSigningPolicies());
	} catch (ProxyPathValidatorException e) {
	    e.printStackTrace();
	    fail("Unexpected exception: " + e.getMessage());
	}
    }

    public void testSigningPolicy() throws Exception {
        
	X509Certificate [] chain = null;

        SigningPolicy policy = SigningPolicyParser.getPolicy(new StringReader(signingPolicy), "/C=US/O=Globus/CN=Globus Certification Authority") ;       

        TestProxyPathValidator validator = new TestProxyPathValidator(true);
        chain = new X509Certificate[] {goodCertsArr[1], goodCertsArr[0]};
        TrustedCertificates tc = 
            new TrustedCertificates(new X509Certificate[] { goodCertsArr[0] },
                                    new SigningPolicy[] { policy });
        validator.validate(chain, tc);

        policy = SigningPolicyParser.getPolicy(new StringReader(signingPolicyViolation), "/C=US/O=Globus/CN=Globus Certification Authority") ;       
        tc = new TrustedCertificates(new X509Certificate[] { goodCertsArr[0] },
                                     new SigningPolicy[] { policy });

        boolean expOccured = false;
        try {
            validator.validate(chain, tc);
        } catch (ProxyPathValidatorException exp) {
            exp.printStackTrace();
            expOccured = true;
	    assertEquals(ProxyPathValidatorException.SIGNING_POLICY_VIOLATION,
			 exp.getErrorCode());
        }
         assertTrue(expOccured);        
        
        policy = SigningPolicyParser.getPolicy(new StringReader(signingPolicySansPolicy), "/C=US/O=Globus/CN=Globus Certification Authority") ;       
        tc = new TrustedCertificates(new X509Certificate[] { goodCertsArr[0] },
                                     new SigningPolicy[] { policy });

        expOccured = false;
        try {
            validator.validate(chain, tc);
        } catch (ProxyPathValidatorException exp) {
            exp.printStackTrace();
            expOccured = true;
	    assertEquals(ProxyPathValidatorException.NO_SIGNING_POLICY,
			 exp.getErrorCode());
        }
         assertTrue(expOccured);

        validator.validate(chain, tc, null, Boolean.FALSE);
    }

    // for testing only to disable validity checking
    class TestProxyPathValidator extends ProxyPathValidator {

        boolean policyChk = false;

        TestProxyPathValidator() {
            super();
            policyChk = false;
        }

        TestProxyPathValidator(boolean checkSigningPolicy) {
            policyChk = checkSigningPolicy;
        }

	public void validate(X509Certificate [] certPath) 
	    throws ProxyPathValidatorException {
	    super.validate(certPath);
	}

	public void validate(X509Certificate [] certPath,
			     TrustedCertificates trustedCerts) 
	    throws ProxyPathValidatorException {
	    super.validate(certPath, trustedCerts);
	}

        protected void checkValidity(X509Certificate cert) 
            throws ProxyPathValidatorException {
        }

	public void validate(X509Certificate [] certPath,
			     TrustedCertificates trustedCerts,
                             CertificateRevocationLists crlsList,
                             Boolean enforceSigningPolicy)
	    throws ProxyPathValidatorException {
            
	    super.validate(certPath, trustedCerts, crlsList, 
                           enforceSigningPolicy);
	}
        
        protected boolean checkCRLValidity(X509CRL crl) {
            return true;
        }    
        
        // Disabling signing policy by default for testing other
        // validation pieces
        protected void checkSigningPolicy(X509Certificate certificate,
                                          TrustedCertificates trustedCerts,
                                          Boolean enforcePolicy) 
            throws ProxyPathValidatorException {
            if (policyChk) {
                super.checkSigningPolicy(certificate, trustedCerts,
                                         enforcePolicy);
            }
        }
    }
    
}
