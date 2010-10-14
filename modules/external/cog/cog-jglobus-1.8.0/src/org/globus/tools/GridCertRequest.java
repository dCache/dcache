/**
 * Copyright (c) 2003, National Research Council of Canada
 * All rights reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, and/or sell copies 
 * of the Software, and to permit persons to whom the Software is furnished to
 * do so, subject to the following conditions:
 * 
 * The above copyright notice(s) and this licence appear in all copies of the 
 * Software or substantial portions of the Software, and that both the above 
 * copyright notice(s) and this license appear in supporting documentation.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
 * NONINFRINGEMENT OF THIRD PARTY RIGHTS. IN NO EVENT SHALL THE 
 * COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE LIABLE 
 * FOR ANY CLAIM, OR ANY DIRECT, INDIRECT, SPECIAL OR CONSEQUENTIAL 
 * DAMAGES, OR ANY DAMAGES WHATSOEVER (INCLUDING, BUT NOT 
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS 
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWSOEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN AN ACTION OF 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
 * OTHERWISE) ARISING IN ANY WAY OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OF THE SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * Except as contained in this notice, the name of a copyright holder shall NOT
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without specific prior written authorization. 
 * Title to copyright in this software and any associated documentation will at
 * all times remain with copyright holders.
 */
package org.globus.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;

import org.bouncycastle.asn1.DERConstructedSet;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.util.encoders.Base64;

import org.globus.common.Version;
import org.globus.common.CoGProperties;
import org.globus.gsi.CertUtil;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.bc.X509NameHelper;
import org.globus.util.PEMUtils;
import org.globus.util.Util;
import org.globus.util.ConfigUtil;

/**
 * GridCertRequest Command Line Client.
 * 
 * @author <a href="mailto:jean-claude.cote@nrc-cnrc.gc.ca">Jean-Claude Cote</a>
 */
public final class GridCertRequest {

    public static final String USAGE =
        "\n"
	+ "\ngrid-cert-request [-help] [ options ...]"
	+ "\n"
	+ "\n  Example Usage:"
	+ "\n"
	+ "\n    Creating a user certifcate:"
	+ "\n      grid-cert-request"
	+ "\n"
	+ "\n    Creating a host or gatekeeper certifcate:"
	+ "\n      grid-cert-request -host [my.host.fqdn]"
	+ "\n"
	+ "\n    Creating a LDAP server certificate:"
	+ "\n      grid-cert-request -service ldap -host [my.host.fqdn]"
	+ "\n"
	+ "\n  Options:"
	+ "\n"
	+ "\n    -version           : Display version"
	+ "\n    -?, -h, -help,     : Display usage"
	+ "\n    -usage"
	+ "\n    -cn <name>,        : Common name of the user"
	+ "\n    -commonname <name>"
	+ "\n    -service <service> : Create certificate for a service. Requires"
	+ "\n                         the -host option and implies that the generated"
	+ "\n                         key will not be password protected (ie implies -nopw)."
	+ "\n    -host <FQDN>       : Create certificate for a host named <FQDN>"
	+ "\n    -dir <directory>   : Changes the directory the private key and certificate"
	+ "\n                         request will be placed in. By default user certificates"
	+ "\n                         are placed in " + System.getProperty("user.home") + File.separator + ".globus"
	+ "\n                         directory. On Unix machines, host certificates are"
	+ "\n                         placed in /etc/grid-security directory and service"
	+ "\n                         certificates are placed in /etc/grid-security/<service>."
	+ "\n                         On Windows machines they are placed in the same"
	+ "\n                         location as the user certificates."
	+ "\n    -prefix <prefix>   : Causes the generated files to be named"
	+ "\n                         <prefix>cert.pem, <prefix>key.pem and"
	+ "\n                         <prefix>cert_request.pem"
	+ "\n    -nopw,             : Create certificate without a password"
	+ "\n    -nodes,"
	+ "\n    -nopassphrase,"
	+ "\n    -verbose           : Don't clear the screen <<Not used>>"
	+ "\n    -int[eractive]     : Prompt user for each component of the DN"
	+ "\n    -force             : Overwrites preexisting certifictes"
	+ "\n    -caEmail <address> : CA email address, if request is to be mailed to CA"
	+ "\n    -orgBaseDN <dn>    : The base DN of this organization (in LDAP format)";
	
    private static final String MESSAGE =
        "A certificate request and private key will be created."
	+ "\nYou will be asked to enter a PEM pass phrase."
	+ "\nThis pass phrase is akin to your account password,"
	+ "\nand is used to protect your key file."
	+ "\nIf you forget your pass phrase, you will need to"
	+ "\nobtain a new certificate.\n";

    private static String caEmail = null;

    private static String cn = null;

    private static boolean interactive = false;
    private static boolean verbose = false;

    private static boolean noPswd = false;
    private static String dir = null;
    private static boolean force = false;
    private static String prefix = null;
    private static boolean debug = false;

    public static void main(String[] args) {

        parseCmdLine(args);

	File certDir = new File(dir);

	// Create dir if does not exists.
	if (!certDir.exists()) {
	    // if fails exit
	    if (!certDir.mkdirs()) {
		exit("Unable to create " + certDir + " directory.", 1);
	    }
	}
    
	// Make sure it's a directory.
	if (!certDir.isDirectory()) {
	    exit("The directory " + certDir + " specified is not a directory.", 2);
        }
        
        // Make sure we can write to it.
	if (!certDir.canWrite()) {
	    exit("Can't write to " + certDir, 3);
        }

	File certFile = new File(certDir, prefix + "cert.pem");
	File keyFile = new File(certDir, prefix + "key.pem");
	File certRequestFile = new File(certDir, prefix + "cert_request.pem");

        // Check not to overwrite any of these files.
	if (!force) {
	    boolean fileExists = false;
	    if (keyFile.exists()) {
		System.err.println(keyFile + " exists");
		fileExists = true;
	    }
	    if (certFile.exists()) {
		System.err.println(certFile + " exists");
		fileExists = true;
	    }
	    if (certRequestFile.exists()) {
		System.err.println(certRequestFile + " exists");
		fileExists = true;
	    }

	    if (fileExists) {
		exit("If you wish to overwrite, run the script again with -force.", 4);
	    }
        }

        String password = null;

        if (!noPswd) {
            // Get password from user.                
            int attempts = 0;
	    boolean passOK = false;
	    
            System.out.println(MESSAGE);

            while (attempts < 3) {

                password = Util.getPrivateInput("Enter PEM pass phrase: ");
		if (password.length() < 4) {
		    System.out.println("Phrase is too short, needs to be at least 4 chars");
		    attempts++;
		    continue;
		}
		
                String password2 =
                    Util.getPrivateInput("Verifying password - Enter PEM pass phrase: ");
		
                if (password.compareTo(password2) == 0) {
		    passOK = true;
		    break;
		} else {
                    System.out.println("Verify failure");
		    attempts++;
		}
            }

	    if (!passOK) {
		exit("Too many attempts", 5);
	    }
        }

        // Generate cert request.            
	try {
	    genCertificateRequest(cn,
				  caEmail, 
				  password,
				  keyFile,
				  certFile,
				  certRequestFile);
	} catch (Exception e) {
	    System.err.println("Error generating cert request: " + 
			       e.getMessage());
	    if (debug) {
		e.printStackTrace();
	    }
	    System.exit(6);
	}
    }

    private static void exit(String msg, int errorCode) {
	System.err.println("Error: " + msg);
	System.exit(errorCode);
    }

    private static void exit(String msg) {
	exit(msg, 1);
    }

    protected static void parseCmdLine(String[] args) {

	String hostName = null;
	String service = null;
	String orgBaseDN = null;
	String name = System.getProperty("user.name");

	for (int i = 0; i < args.length; i++) {
	    if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-help")
		       || args[i].equalsIgnoreCase("-h")
		       || args[i].equalsIgnoreCase("-?")) {
		exit(USAGE, 0);
	    } else if (args[i].equalsIgnoreCase("-cn")
		       || args[i].equalsIgnoreCase("-commonname")) {
		i++;
		if (i == args.length) {
		    exit("-cn requires an argument");
		} else {
		    // common name specified
		    name = args[i];
		}
	    } else if (args[i].equalsIgnoreCase("-service")) {
		i++;
		if (i == args.length) {
		    exit("-service requires an argument");
		} else {
		    // user certificate directory specified
		    service = args[i];
		}
	    } else if (args[i].equalsIgnoreCase("-host")) {
		i++;
		if (i == args.length) {
		    exit("-host requires an argument");
		} else {
		    // host name specified
		    hostName = args[i];
		}
	    } else if (args[i].equalsIgnoreCase("-dir")) {
		i++;
		if (i == args.length) {
		    exit("-dir requires an argument");
		} else {
		    // user certificate directory specified
		    dir = args[i];
		}
	    } else if (args[i].equalsIgnoreCase("-prefix")) {
		i++;
		if (i == args.length) {
		    exit("-prefix requires an argument");
		} else {
		    prefix = args[i];
		}
	    } else if (args[i].equalsIgnoreCase("-nopw")
		       || args[i].equalsIgnoreCase("-nodes")
		       || args[i].equalsIgnoreCase("-nopassphrase")) {
		// no password
		noPswd = true;
	    } else if (args[i].equalsIgnoreCase("-verbose")) {
		verbose = true;
	    } else if (args[i].equalsIgnoreCase("-int") || 
                       args[i].equalsIgnoreCase("-interactive")) {
		// interactive mode
		interactive = true;
	    } else if (args[i].equalsIgnoreCase("-force")) {
		// overwrite existing credentials
		force = true;
	    } else if (args[i].equalsIgnoreCase("-debug")) {
		// overwrite existing credentials
		debug = true;
	    } else if (args[i].equalsIgnoreCase("-caEmail")) {
		i++;
		if (i == args.length) {
		    exit("-caEmail requires an argument");
		} else {
		    caEmail = args[i];
		}
	    } else if (args[i].equalsIgnoreCase("-orgBaseDN")) {
		i++;
		if (i == args.length) {
		    exit("-orgBaseDN requires an argument");
		} else {
		    orgBaseDN = args[i];
		}
	    } else {
		exit("argument #"
		     + (i+1)
		     + "("
		     + args[i]
		     + ") : unknown");
	    }
	}

	if (orgBaseDN == null) {
	    CoGProperties props = CoGProperties.getDefault();
	    orgBaseDN = props.getProperty("orgBaseDN");
	    if (orgBaseDN == null) {
		if (interactive) {
		    orgBaseDN = getOrgName();
		} else {
		    // just a default
		    orgBaseDN = "O=Grid";
		}
	    }
	}
	
	if (service != null) {
	    if (hostName == null) {
		exit("-host required");
	    } else {
		cn = orgBaseDN + ", CN=" + service + "/" + hostName;
		noPswd = true;
		if (prefix == null) {
		    prefix = service;
		}
		if (dir == null && ConfigUtil.getOS() == ConfigUtil.UNIX_OS) {
		    dir = "/etc/grid-security/" + service;
		}
	    }
	} else if (hostName != null) {
	    cn = orgBaseDN + ", CN=host/" + hostName;
	    noPswd = true;
	    if (prefix == null) {
		prefix = "host";
	    }
	    if (dir == null && ConfigUtil.getOS() == ConfigUtil.UNIX_OS) {
		dir = "/etc/grid-security";
	    }
	} else {
	    cn = orgBaseDN + ", CN=" + name;
	    if (prefix == null) {
		prefix = "user";
	    }
	}

	if (dir == null) {
	    dir = System.getProperty("user.home") + 
		File.separator + 
		".globus";
	}
    }
    
    /**
     * Generates a encrypted private key and certificate request.
     */
    static public void genCertificateRequest(String dname,
                                             String emailAddressOfCA,
                                             String password,
                                             File keyFile,
                                             File certFile,
                                             File certReqFile)
        throws Exception {

        String sigAlgName = "MD5WithRSA";
        String keyAlgName = "RSA";
	
	CertUtil.init();

	X509Name name = new X509Name(dname);

	String certSubject = X509NameHelper.toString(name);

	System.out.println("Generating a 1024 bit RSA private key");

        // Generate a new key pair.
        KeyPairGenerator keygen = KeyPairGenerator.getInstance(keyAlgName);
	keygen.initialize(1024);
        KeyPair keyPair = keygen.genKeyPair();
        PrivateKey privKey = keyPair.getPrivate();
        PublicKey pubKey = keyPair.getPublic();

        // Generate the certificate request.        
        DERConstructedSet derSet = new DERConstructedSet();
        PKCS10CertificationRequest request =
            new PKCS10CertificationRequest(
                sigAlgName,
                name,
                pubKey,
                derSet,
                privKey);
	
        // Save the certificate request to a .pem file.
        byte[] data = request.getEncoded();
	byte[] encodedData = Base64.encode(data);
	
        PrintStream ps = null;

	try {
	    ps = new PrintStream(new FileOutputStream(certReqFile));

            boolean caEmail = false;

            if ((emailAddressOfCA != null) && 
                (emailAddressOfCA.length() > 0)) {
                caEmail = true;
                ps.print("\n\n" + "Please mail the following certificate request to " + emailAddressOfCA);
            } else {
                ps.print("\n\n" + "Please send the following certificate request to the Certificate Authority (CA). Refer to CA instructions for details on to send the request.");
            }
            ps.print("\n\n"
                     + "==================================================================\n"
                     + "\n"
                     + "Certificate Subject:\n"
                     + "\n"
                     + certSubject
                     + "\n"
                     + "\n"
                     + "The above string is known as your user certificate subject, and it \n"
                     + "uniquely identifies this user.\n"
                     + "\n"
                     + "To install this user certificate, please save this e-mail message\n"
                     + "into the following file.\n"
                     + "\n"
                     + "\n"
                     + certReqFile.getAbsolutePath()
                     + "\n"
                     + "\n"
                     + "\n"
                     + "      You need not edit this message in any way. Simply \n"
                     + "      save this e-mail message to the file.\n"
                     + "\n"
                     + "\n"
                     + "If you have any questions about the certificate contact\n"
                     + "the Certificate Authority");
            if (caEmail) {
                ps.print("at " + emailAddressOfCA);
            }
            ps.print("\n\n");
	    PEMUtils.writeBase64(ps,
				 "-----BEGIN CERTIFICATE REQUEST-----",
				 encodedData,
				 "-----END CERTIFICATE REQUEST-----");
	} finally {
	    if (ps != null) {
		ps.close();
	    }
	}
	
        // Save private key to a .pem file.
        OpenSSLKey key = new BouncyCastleOpenSSLKey(privKey);
        if (password != null) {
            key.encrypt(password);
        }
	// this will set the permissions correctly already
        key.writeTo(keyFile.getAbsolutePath());

        // Create an empty cert file.
        certFile.createNewFile();

	System.out.println("A private key and a certificate request has been generated with the subject:");
	System.out.println();
	System.out.println(certSubject);
	System.out.println();

	System.out.println("The private key is stored in " + 
			   keyFile.getAbsolutePath());
	System.out.println("The request is stored in " +
			   certReqFile.getAbsolutePath());

    }

    private static String getOrgName() {

	System.out.println("-----");
	System.out.println("You are about to be asked to enter information that will be incorporated");
	System.out.println("into your certificate request.");
	System.out.println("What you are about to enter is what is called a Distinguished Name or a DN.");
	System.out.println("Enter organization DN by entering individual component names and their values.");
	System.out.println("The component name can be one of: " + X509Name.DefaultLookUp.keySet());
	System.out.println("-----");

	StringBuffer orgName = new StringBuffer();
	String component = null;
	while ( (component = getComponent()) != null ) {
	    if (orgName.length() != 0) {
		orgName.append(", ");
	    }
	    orgName.append(component);
	}
	
	if (orgName.length() == 0) {
	    exit("Invalid organization DN");
	}
	
	return orgName.toString();
    }

    private static String getComponent() {
	String component = null;
	
	while (true) {
	    component = Util.getInput("Enter name component: ");
	    if (component == null || component.trim().length() == 0) {
		return null;
	    }
	    component = component.trim();
	    if (X509Name.DefaultLookUp.get(component.toLowerCase()) == null) {
		System.out.println("Invalid component name");
	    } else {
		break;
	    }
	}

	component = component.toUpperCase();
	
	String value = Util.getInput("Enter '" + component + "' value: ");
	if (value == null || value.trim().length() == 0) {
	    return null;
	}
	return component + "=" + value.trim();
    }
	
	
	
}
