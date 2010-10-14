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
package org.globus.tools;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.File;

import org.globus.common.CoGProperties;
import org.globus.common.Version;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.CertUtil;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.proxy.ext.ProxyPolicy;
import org.globus.gsi.proxy.ext.ProxyCertInfo;
import org.globus.gsi.proxy.ext.ProxyCertInfoExtension;
import org.globus.gsi.proxy.ext.GlobusProxyCertInfoExtension;
import org.globus.gsi.proxy.ProxyPolicyHandler;
import org.globus.gsi.proxy.ProxyPathValidator;
import org.globus.gsi.proxy.ProxyPathValidatorException;
import org.globus.util.Util;


/* ###########################################################################
 * This is a command-line tool and was specifically designed to be used as so.
 * Do not use it as a library.
 * ######################################################################## */

/** 
 * Initializes/creates a new globus proxy. This is a command-line tool. Please
 * do not use it as a library.
 */
public abstract class ProxyInit {

    public static final String GENERIC_POLICY_OID = "1.3.6.1.4.1.3536.1.1.1.8";

    private static final String message =
	"\n" +
	"Syntax: java ProxyInit [options]\n" +
	"        java ProxyInit -help\n\n" +
	"    Options:\n" +
	"    -help | -usage\t\tDisplays usage.\n" +
	"    -version\t\t\tDisplays version.\n" +
	"    -debug\t\t\tEnables extra debug output.\n" +
	"    -verify\t\t\tPerforms proxy verification tests (default).\n" +
        "    -pwstdin\t\t\tAllows passphrase from stdin.\n" +
	"    -noverify\t\t\tDisables proxy verification tests.\n" +
	"    -quiet | -q\t\t\tQuiet mode, minimal output\n" + 
	"    -limited\t\t\tCreates a limited proxy.\n" +
        "    -independent\t\tCreates a independent globus proxy.\n" +
	"    -old\t\t\tCreates a legacy globus proxy.\n" +
        "    -valid <seconds>\t\tProxy is valid for S seconds (default:12 " +
        "hours)\n" +
        "    -valid <hours:minutes>\tProxy is valid for H hours and M \n" +
        "                          \tminutes. (default: 12 hours)\n" +
	"    -hours <hours>\t\tProxy is valid for H hours (default:12).\n" +
	"    -bits <bits>\t\tNumber of bits in key {512|1024|2048|4096}.\n" +
	"    -globus\t\t\tPrints user identity in globus format.\n" +
        "    -policy <policyfile>\tFile containing policy to store in the\n" +
        "                        \tProxyCertInfo extension\n" +
	"    -pl <oid>\t\t\tOID string for the policy language.\n" +
	"    -policy-language <oid>\tused in the policy file.\n" +
	"    -path-length <l>\t\tAllow a chain of at most l proxies to be \n" +
	"                    \t\tgenerated from this one\n" +
	"    -cert <certfile>\t\tNon-standard location of user certificate\n" + 
	"    -key <keyfile>\t\tNon-standard location of user key\n" + 
	"    -out <proxyfile>\t\tNon-standard location of new proxy cert.\n" +
	"    -pkcs11\t\t\tEnables the PKCS11 support module. The\n" +
	"           \t\t\t-cert and -key arguments are used as labels\n" +
	"           \t\t\tto find the credentials on the device.\n" +
        "    -rfc\t\t\tCreates RFC 3820 compliant proxy. (Default)\n" +
        "    -draft\t\t\tCreates RFC draft compliant proxy\n";
    
    protected X509Certificate[] certificates;
    protected int bits  = 512;
    protected int lifetime = 3600 * 12;

    protected ProxyCertInfo proxyCertInfo = null;
    protected int proxyType;

    protected boolean quiet = false;
    protected boolean debug = false;
    protected boolean stdin = false;

    protected GlobusCredential proxy = null;

    public abstract void init(String [] args);
    public abstract void loadCertificates(String args);
    public abstract void loadKey(String arg);
    public abstract void sign();

    public X509Certificate getCertificate() {
        return this.certificates[0];
    }

    public void dispose() {
    }

    // verifies the proxy credential
    public void verify()
	throws Exception {
	
	TrustedCertificates trustedCerts =
	    TrustedCertificates.getDefaultTrustedCertificates();
	
	if (trustedCerts == null || 
	    trustedCerts.getCertificates() == null ||
	    trustedCerts.getCertificates().length == 0) {
	    throw new Exception("Unable to load CA ceritificates");
	}

	ProxyPathValidator validator = new ProxyPathValidator();

	if (proxyCertInfo != null) {
	    String oid = 
		proxyCertInfo.getProxyPolicy().getPolicyLanguage().getId();
	    validator.setProxyPolicyHandler(oid, new ProxyPolicyHandler() {
		    public void validate(ProxyCertInfo proxyCertInfo,
					 X509Certificate[] certPath,
					 int index)
			throws ProxyPathValidatorException {
			// ignore policy - this is just for proxy init case
			System.out.println("Proxy verify: Ignoring proxy policy");
			if (debug) {
			    String policy = 
				new String(proxyCertInfo.getProxyPolicy().getPolicy());
			    System.out.println("Policy:");
			    System.out.println(policy);
			}
		    }
		});
	}

	validator.validate(proxy.getCertificateChain(),
			   trustedCerts.getCertificates(),
                           null,
                           trustedCerts.getSigningPolicies());

    }

    public void setBits(int bits) {
	this.bits = bits;
    }

    public void setLifetime(int lifetime) {
	this.lifetime = lifetime;
    }

    public void setProxyType(int proxyType) {
	this.proxyType = proxyType;
    }

    public void setProxyCertInfo(ProxyCertInfo proxyCertInfo) {
	this.proxyCertInfo = proxyCertInfo;
    }

    public void setDebug(boolean debug) {
	this.debug = debug;
    }

    public void setQuiet(boolean quiet) {
	this.quiet = quiet;
    }

    public void setStdin(boolean stdin) {
	this.stdin = stdin;
    }
    
    public void createProxy(String cert,
			    String key,
			    boolean verify,
			    boolean globusStyle,
			    String proxyFile) {

	init(new String [] {cert, key});
	
	loadCertificates(cert);

	if (!quiet) {
	    String dn = null;
	    if (globusStyle) {
		dn = CertUtil.toGlobusID(getCertificate().getSubjectDN());
	    } else {
		dn = getCertificate().getSubjectDN().getName();
	    }
	    System.out.println("Your identity: " + dn);
	}
	
	loadKey(key);
	
	if (debug) {
	    System.out.println("Using " + bits + " bits for private key");
	}
	
	if (!quiet) {
	    System.out.println("Creating proxy, please wait...");
	}
	
	sign();
	
	if (verify) {
	    try {
		verify();
		System.out.println("Proxy verify OK");
	    } catch(Exception e) {
		System.out.println("Proxy verify failed: " + e.getMessage());
		if (debug) {
		    e.printStackTrace();
		}
		System.exit(-1);
	    }
	}

	if (debug) {
	    System.out.println("Saving proxy to: " + proxyFile);
	}
	
	if (!quiet) {
	    System.out.println("Your proxy is valid until " + 
			       proxy.getCertificateChain()[0].getNotAfter());
	}

	OutputStream out = null;
	try {
            File file = Util.createFile(proxyFile);
	    // set read only permissions
	    if (!Util.setOwnerAccessOnly(proxyFile)) {
		System.err.println("Warning: Please check file permissions for your proxy file.");
	    }
	    out = new FileOutputStream(file);
	    // write the contents
	    proxy.save(out);
        } catch (SecurityException e) {
	    System.err.println("Failed to save proxy to a file: " + 
			       e.getMessage());
	    System.exit(-1);            
	} catch (IOException e) {
	    System.err.println("Failed to save proxy to a file: " + 
			       e.getMessage());
	    System.exit(-1);
	} finally {
	    if (out != null) {
		try { out.close(); } catch(Exception e) {}
	    }
	}

	dispose();
    }
    
    public static void main(String args[]) {
    
	int bits         = 512;
	int lifetime     = 3600 * 12;
	boolean debug    = false;
	boolean quiet    = false;
	boolean verify   = true;
	boolean pkcs11   = false;
	boolean limited  = false;
	int pathLen      = -1;
	int proxyType    = -1;
        // 0 is old, 1 is Globus (draft compliant) oid, 2 is rfc oid
        int oid          = 2;
	String policyLanguage = null;
	String policyFile = null;
        boolean stdin     = false;
        boolean independent = false;
   
	CoGProperties properties = CoGProperties.getDefault();

	boolean globusStyle = false;
	String proxyFile    = properties.getProxyFile();
	String keyFile      = null;
	String certFile     = null;
	
	for (int i = 0; i < args.length; i++) {
	    if (args[i].equalsIgnoreCase("-hours")) {
		if (i+1 >= args.length) {
		    argError("-hours argument missing");
		}
		lifetime = 3600 * Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-bits")) {
		if (i+1 >= args.length) {
		    argError("-bits argument missing");
		}
		bits = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-pwstdin")) {
                stdin = true;
	    } else if (args[i].equalsIgnoreCase("-limited")) {
		limited = true;
	    } else if (args[i].equalsIgnoreCase("-independent")) {
                independent = true;
	    } else if (args[i].equalsIgnoreCase("-old")) {
                oid = 0;
	    } else if (args[i].equalsIgnoreCase("-path-length")) {
		if (i+1 >= args.length) {
		    argError("-path-length argument missing");
		}
		pathLen = Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-pl") ||
		       args[i].equalsIgnoreCase("-policy-language")) {
		if (i+1 >= args.length) {
		    argError("-policy-language argument missing");
		}
		policyLanguage = args[++i];
	    } else if (args[i].equalsIgnoreCase("-policy")) {
		if (i+1 >= args.length) {
		    argError("-policy argument missing");
		}
		policyFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-debug")) {
		debug = true;
	    } else if (args[i].equalsIgnoreCase("-verify")) {
		verify = true;
	    } else if (args[i].equalsIgnoreCase("-noverify")) {
		verify = false;
	    } else if (args[i].equalsIgnoreCase("-out")) {
		if (i+1 >= args.length) {
		    argError("-out argument missing");
		}
		proxyFile = args[++i];
	    } else if (args[i].equals("-q") ||
		       args[i].equalsIgnoreCase("-quiet")) {
		quiet = true;
	    } else if (args[i].equalsIgnoreCase("-globus")) {
		globusStyle = true;
	    } else if (args[i].equalsIgnoreCase("-pkcs11")) {
		pkcs11 =  true;
	    } else if (args[i].equalsIgnoreCase("-key")) {
		if (i+1 >= args.length) {
		    argError("-key argument missing");
		}
		keyFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-cert")) {
		if (i+1 >= args.length) {
		    argError("-cert argument missing");
		}
		certFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-valid")) {
                String validity = args[++i];
                int delimiterIndex = validity.indexOf(":");
                if (delimiterIndex == -1) {
                    lifetime = Integer.parseInt(validity);
                } else {
                    String hours = validity.substring(0, delimiterIndex);
                    String minutes = validity.substring(delimiterIndex+1,
                                                        validity.length());
                    int hoursInt = Integer.parseInt(hours);
                    int minsInt = Integer.parseInt(minutes);
                    lifetime = (minsInt * 60) + (hoursInt * 60 * 60);
                }
	    } else if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.err.println(message);
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-draft")) {
                oid = 1;
            } else if (args[i].equalsIgnoreCase("-rfc")) {
                oid = 2;
            } else {
                argError("Argument not recognized: " + args[i]);
		break;
	    }
	}
    
	if (proxyFile == null) {
	    error("Proxy file not specified.");
	}
	
	boolean restricted = (policyFile != null || policyLanguage != null);

    //	if (proxyType == GSIConstants.GSI_3_INDEPENDENT_PROXY) {
	if (independent) {
	    if (oid == 0) {
		error("-old and -independent are exclusive");
	    }
	    if (limited) {
		error("-limited and -independent are exclusive");
	    }
	    if (restricted) {
		error("-indepedent and -policy or -pl are exclusive");
	    }
            if (oid == 1) {
                proxyType = GSIConstants.GSI_3_INDEPENDENT_PROXY;
            } else {
                // oid has to be 2
                proxyType = GSIConstants.GSI_4_INDEPENDENT_PROXY;
            }
	}

	if (restricted) {
	    if (oid == 0) {
		error("-old and -policy or -pl are exclusive");
	    }
	    if (limited) {
		error("-limited and -policy or -pl are exclusive");
	    }
	    // XXX: check if proxyType == -1?
            if (oid == 1) {
                proxyType = GSIConstants.GSI_3_RESTRICTED_PROXY;
            } else {
                // oid has to be 2
                proxyType = GSIConstants.GSI_4_RESTRICTED_PROXY;
            }
	}

	if (proxyType == -1) {
	    if (oid == 1) {
		proxyType = (limited) ?
		    GSIConstants.GSI_3_LIMITED_PROXY :
		    GSIConstants.GSI_3_IMPERSONATION_PROXY;
	    } else if (oid == 2) {
		proxyType = (limited) ?
		    GSIConstants.GSI_4_LIMITED_PROXY :
		    GSIConstants.GSI_4_IMPERSONATION_PROXY;
            } else {
		proxyType = (limited) ? 
		    GSIConstants.GSI_2_LIMITED_PROXY :
		    GSIConstants.GSI_2_PROXY;
	    }
	}

	ProxyInit init = null;
	if (pkcs11) {
	    if (keyFile == null) {
		if (certFile == null) {
		    keyFile = certFile = properties.getDefaultPKCS11Handle();
		} else {
		    keyFile = certFile;
		}
	    } else {
		if (certFile == null) {
		    certFile = keyFile;
		}
	    }
	    try {
		Class iClass = 
		    Class.forName("org.globus.pkcs11.tools.PKCS11ProxyInit");
		init = (ProxyInit)iClass.newInstance();
	    } catch (ClassNotFoundException e) {
		System.err.println("Error: Failed to load PKCS11 module.");
		System.exit(-1);
	    } catch (InstantiationException e) {
		System.err.println("Error: Failed to instantiate PKCS11 module: " + e.getMessage());
		System.exit(-1);
	    } catch (IllegalAccessException e) {
		System.err.println("Error: Failed to initialize PKCS11 module: " + e.getMessage());
		System.exit(-1);
	    }
	} else {
	    if (keyFile == null) {
		keyFile = properties.getUserKeyFile();
	    }
	    if (certFile == null) {
		certFile = properties.getUserCertFile();
	    }
	    init = new DefaultProxyInit();
	}

	if (debug) {
	    System.err.println("Files used: ");
	    System.err.println("  proxy     : " + ((proxyFile == null) ? "none" : proxyFile));
	    System.err.println("  user key  : " + ((keyFile == null) ? "none" : keyFile));
	    System.err.println("  user cert : " + ((certFile == null) ? "none" : certFile));
	}
	
	CertUtil.init();

	ProxyCertInfo proxyCertInfo = null;

	if ((CertUtil.isGsi3Proxy(proxyType)) || 
            (CertUtil.isGsi4Proxy(proxyType))) {
	    ProxyPolicy policy = null;
            if (CertUtil.isLimitedProxy(proxyType)) {
		policy = new ProxyPolicy(ProxyPolicy.LIMITED);
            } else if (CertUtil.isIndependentProxy(proxyType)) {
		policy = new ProxyPolicy(ProxyPolicy.INDEPENDENT);
            } else if (CertUtil.isImpersonationProxy(proxyType)) {
                // since limited has already been checked, this should work.
		policy = new ProxyPolicy(ProxyPolicy.IMPERSONATION);
	    } else if ((proxyType == GSIConstants.GSI_3_RESTRICTED_PROXY) ||
                       (proxyType == GSIConstants.GSI_4_RESTRICTED_PROXY)) {
		if (policyFile == null) {
		    error("Policy file required.");
		}
		if (policyLanguage == null) {
		    policyLanguage = GENERIC_POLICY_OID;
		}
		byte [] policyData = null;
		try {
		    policyData = readPolicyFile(policyFile);
		} catch (IOException e) {
		    error("Failed to load policy file: " + e.getMessage());
		}
		policy = new ProxyPolicy(policyLanguage,
					 policyData);
	    } else {
		throw new IllegalArgumentException("Invalid proxyType");
	    }
	    if (pathLen >= 0) {
		proxyCertInfo = new ProxyCertInfo(pathLen, policy);
	    } else {
		proxyCertInfo = new ProxyCertInfo(policy);
	    }
	}
    
        init.setBits(bits);
	init.setLifetime(lifetime);

	init.setProxyType(proxyType);
	init.setProxyCertInfo(proxyCertInfo);
    
	init.setDebug(debug);
	init.setQuiet(quiet);
	init.setStdin(stdin);

	init.createProxy(certFile,
			 keyFile,
			 verify,
			 globusStyle,
			 proxyFile);
    }
    
    private static void argError(String error) {
	System.err.println("Error: " + error);
	System.err.println();
	System.err.println("Usage: java ProxyInit [-help][-limited][-hours H] ...");
	System.err.println();
	System.err.println("Use -help to display full usage");
	System.exit(1);
    }

    protected static void error(String error) {
	System.err.println("Error: " + error);
	System.exit(1);
    }
    
    private static byte[] readPolicyFile(String file) 
	throws IOException {
	File f = new File(file);
	FileInputStream in = new FileInputStream(f);
	byte [] data = new byte[(int)f.length()];
	int left = data.length;
	int off = 0;
	int bytes = 0;
	try {
	    while (left > 0) {
		bytes = in.read(data, off, left);
		if (bytes == -1) {
		    throw new EOFException();
		}
		off += bytes;
		left -= bytes;
	    }
	} finally {
	    if (in != null) {
		in.close();
	    }
	}
	return data;
    }
    
}

class DefaultProxyInit extends ProxyInit {

    private PrivateKey userKey = null;
    
    public void init(String [] args) {
	verify(args[1], "User key");
	verify(args[0], "User certificate");
    }
   
    public void verify() throws Exception {
	RSAPublicKey pkey = (RSAPublicKey)getCertificate().getPublicKey();
	RSAPrivateKey prkey = (RSAPrivateKey)userKey;
	
	if (!pkey.getModulus().equals(prkey.getModulus())) {
	    throw new Exception("Certificate and private key specified do not match");
	}
	
	super.verify();
    }

    private static void verify(String file, String msg) {
	if (file == null) error(msg + " not specified.");
	
	File f = new File(file);
	if (!f.exists() || f.isDirectory()) error(msg + " not found.");
    }

    public void loadCertificates(String arg) {
	try {
	    certificates = CertUtil.loadCertificates(arg);
	} catch(IOException e) {
	    System.err.println("Error: Failed to load cert: " + arg);
	    System.exit(-1);
	} catch(GeneralSecurityException e) {
	    System.err.println("Error: Unable to load user certificate: " +
			       e.getMessage());
	    System.exit(-1);
	}
    }

    public void loadKey(String arg) {
	try {
	    OpenSSLKey key = new BouncyCastleOpenSSLKey(arg);
	    
	    if (key.isEncrypted()) {
                String prompt = (quiet) ?
                    "Enter GRID pass phrase: " :
                    "Enter GRID pass phrase for this identity: ";
		
                String pwd = (stdin) ?  
                    Util.getInput(prompt) : 
                    Util.getPrivateInput(prompt);

		if (pwd == null) {
		    System.exit(1);
		}
		
		key.decrypt(pwd);
	    }
	    
	    userKey = key.getPrivateKey();
	    
	} catch(IOException e) {
	    System.err.println("Error: Failed to load key: " + arg);
	    System.exit(-1);
	} catch(GeneralSecurityException e) {
	    System.err.println("Error: Wrong pass phrase");
	    if (debug) {
		e.printStackTrace();
	    }
	    System.exit(-1);
	}
    }
    
    public void sign() {
	try {
	    BouncyCastleCertProcessingFactory factory =
		BouncyCastleCertProcessingFactory.getDefault();

	    X509ExtensionSet extSet = null;
	    if (proxyCertInfo != null) {
		extSet = new X509ExtensionSet();
                if (CertUtil.isGsi4Proxy(proxyType)) {
                    // RFC compliant OID
                    extSet.add(new ProxyCertInfoExtension(proxyCertInfo));
                } else {
                    // old OID
                    extSet
                        .add(new GlobusProxyCertInfoExtension(proxyCertInfo));
                }
	    }

	    proxy = factory.createCredential(certificates,
					     userKey,
					     bits,
					     lifetime,
					     proxyType,
					     extSet);
	} catch (GeneralSecurityException e) {
	    System.err.println("Failed to create a proxy: " + e.getMessage());
	    System.exit(-1);
	}
    }
    
}
