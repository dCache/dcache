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

import org.globus.gsi.CertUtil;
import org.globus.gsi.GlobusCredential;
import org.globus.util.Util;
import org.globus.common.CoGProperties;
import org.globus.common.Version;

import java.security.cert.X509Certificate;

/** Retruns information about the proxy.
 *<pre>
 * Syntax: java ProxyInfo [options]
 *         java ProxyInfo -help
  * Options
  * -help | -usage
  *     Displays usage
  * -f <proxyfile> | -file <proxyfile>
  *     Non-standard location of proxy
  * [printoptions]
  *     Prints information about proxy
  * -exists [options] | -e [options]
  *     Returns 0 if valid proxy exists, 1 otherwise
  * -globus
  *     Prints information in globus format
  * [printoptions]
  *     -subject              Distinguished name (DN) of subject
  *     -issuer               DN of issuer (certificate signer)
  *     -type                 Type of proxy (full or limited)
  *     -timeleft             Time (in seconds) until proxy expires
  *     -strength             Key size (in bits)
  *     -all                  All above options in a human readable format
  "
  * [options to -exists]      (if none are given, H = B = 0 are assumed)
  *     -hours H       (-h)   time requirement for proxy to be valid
  *     -bits  B       (-b)   strength requirement for proxy to be valid
  *</pre>
  */
public class ProxyInfo {

    private static final int SUBJECT  = 2;
    private static final int ISSUER   = 4;
    private static final int TYPE     = 8;
    private static final int TIME     = 16;
    private static final int STRENGTH = 32;
    private static final int TEXT     = 64;
    private static final int IDENTITY = 128;
    private static final int PATH     = 256;
    private static final int PATH_LENGTH = 512;
    
    private static final String message =
	"\n" + 
	"Syntax: java ProxyInfo [options]\n" +
	"        java ProxyInfo -help\n\n" +
	"\tOptions:\n" +
	"\t-help | usage\n" + 
	"\t\tDisplays usage.\n" +
	"\t-file <proxyfile>  (-f)\n" + 
	"\t\tNon-standard location of proxy.\n" +
	"\t[printoptions]\n" + 
	"\t\tPrints information about proxy.\n" +
	"\t-exists [options]  (-e) \n" + 
	"\t\tReturns 0 if valid proxy exists, 1 otherwise.\n" +
	"\t-globus \n" + 
	"\t\tPrints information in globus format\n\n" +
	"\t[printoptions]\n" +
	"\t-subject\n" + 
	"\t\tDistinguished name (DN) of subject.\n" +
	"\t-issuer\n" + 
	"\t\tDN of issuer (certificate signer).\n" +
	"\t-identity \n" + 
	"\t\tDN of the identity represented by the proxy.\n" +
	"\t-type \n" + 
	"\t\tType of proxy.\n" +
	"\t-timeleft\n" + 
	"\t\tTime (in seconds) until proxy expires.\n" +
	"\t-strength\n" + 
	"\t\tKey size (in bits)\n " +
	"\t-all\n" + 
	"\t\tAll above options in a human readable format.\n" +
	"\t-text\n" + 
	"\t\tAll of the certificate.\n" +
	"\t-path\n" + 
	"\t\tPathname of proxy file.\n" +
	"\n" +
	"\t[options to -exists] (if none are given, H = B = 0 are assumed)\n" +
	"\t-hours H     (-h) \n" + 
	"\t\ttime requirement for proxy to be valid.\n" +
	"\t-bits  B     (-b) \n" + 
	"\t\tstrength requirement for proxy to be valid\n" +
    "\t-length\n" +
    "\t\tpath length of the proxy\n\n";
    
    public static void main(String args[]) {
    
	String file         = null;
	int options         = 0;
	int bits            = 0;
	int hours           = 0;
	boolean globusStyle = false;
	boolean exists      = false;
	boolean debug       = false;
	
	for (int i = 0; i < args.length; i++) {
	    if (args[i].equalsIgnoreCase("-f") || 
		args[i].equalsIgnoreCase("-file")) {
		if (i+1 >= args.length) {
		    error("-file argument missing");
		}
		file = args[++i];
	    } else if (args[i].equalsIgnoreCase("-subject")) {
		options |= SUBJECT;
	    } else if (args[i].equalsIgnoreCase("-issuer")) {
		options |= ISSUER;
	    } else if (args[i].equalsIgnoreCase("-identity")) {
		options |= IDENTITY;
	    } else if (args[i].equalsIgnoreCase("-type")) {
		options |= TYPE;
	    } else if (args[i].equalsIgnoreCase("-timeleft")) {
		options |= TIME;
	    } else if (args[i].equalsIgnoreCase("-strength")) {
		options |= STRENGTH;
	    } else if (args[i].equalsIgnoreCase("-text")) {
		options |= TEXT;
	    } else if (args[i].equalsIgnoreCase("-path")) {
		options |= PATH;
        } else if (args[i].equalsIgnoreCase("-length")) {
        options |= PATH_LENGTH;
        } else if (args[i].equalsIgnoreCase("-all")) {
		options |= Integer.MAX_VALUE;
		options ^= TEXT;
	    } else if (args[i].equalsIgnoreCase("-globus")) {
		globusStyle = true;
	    } else if (args[i].equalsIgnoreCase("-exists")) {
		exists = true;
	    } else if (args[i].equalsIgnoreCase("-bits")) {
		if (i+1 >= args.length) {
		    error("-bits argument missing");
		}
		bits = Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-hours")) {
		if (i+1 >= args.length) {
		    error("-hours argument missing");
		}
		hours = Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.err.println(message);
		System.exit(1);
	    } else {
		error("Argument not recognized : " + args[i]);
		break;
	    }
	}
	
	GlobusCredential proxy = null;

	try {
	    if (file == null) {
		file = CoGProperties.getDefault().getProxyFile();
	    }
	    proxy = new GlobusCredential(file);
	} catch(Exception e) {
	    System.err.println("Unable to load the user proxy : " + e.getMessage());
	    System.exit(1);
	}
    
	if (exists) {
	    if (bits > 0 && proxy.getStrength() < bits) System.exit(1);
	    if (hours > 0 && (proxy.getTimeLeft()/3600) < hours) System.exit(1);
	    System.exit(0);
	}
    
	if (options == 0) {
	    options |= Integer.MAX_VALUE;
	    options ^= TEXT;
	    options ^= PATH;
	}

	if ((options & SUBJECT) != 0) {
	    String dn = (globusStyle) ? 
		CertUtil.toGlobusID(proxy.getCertificateChain()[0].getSubjectDN()) : 
		proxy.getSubject();
	    System.out.println("subject    : " + dn);
	}
    
	if ((options & ISSUER) != 0) {
	    String dn = (globusStyle) ? 
		CertUtil.toGlobusID(proxy.getCertificateChain()[0].getIssuerDN()) :
		proxy.getIssuer();
	    System.out.println("issuer     : " + dn);
	}

	if ((options & IDENTITY) != 0) {
	    String dn = null;
            if (globusStyle) {
		dn = proxy.getIdentity();
            } else {
		X509Certificate cert = proxy.getIdentityCertificate();
                dn = (cert == null) ?
                    "failed to determine certificate identity" :
                    cert.getSubjectDN().toString();
            }
	    System.out.println("identity   : " + dn);
	}
    
	if ((options & TYPE) != 0) {
	    int type = proxy.getProxyType();
            String typeStr = (type == -1) ?
                "failed to determine certificate type" :
                CertUtil.getProxyTypeAsString(type);
            System.out.println("type       : " + typeStr);
	}

	if ((options & STRENGTH) != 0) {
	    System.out.println("strength   : " + proxy.getStrength() + " bits");
	}

	if ((options & PATH) != 0) {
	    System.out.println("path       : " + file);
	}

	if ((options & TIME) != 0) {
	    String tm = (globusStyle) ? 
		formatTimeSecGlobus(proxy.getTimeLeft()) :
		Util.formatTimeSec(proxy.getTimeLeft());
	    System.out.println("timeleft   : " + tm);
	}
	
	if ((options & TEXT) != 0) {
	    System.out.println(proxy.getCertificateChain()[0]);
	}

    if ((options & PATH_LENGTH) != 0) {
        int pathLength =  proxy.getPathConstraint();
        if (pathLength == Integer.MAX_VALUE) {
            System.out.println("path length: infinity");
        } else {
            System.out.println("path length: " + pathLength);
        }
    }
    }

    private static void error(String error) {
	System.err.println("Error: " + error);
	System.err.println();
	System.err.println("Usage: java ProxyInfo [-help][-f proxyfile][-subject]...");
	System.err.println();
	System.err.println("Use -help to display full usage");
	System.exit(1);
    }

    private static String formatTimeSecGlobus(long time) {
	StringBuffer str = new StringBuffer();
	long tt;
	
	tt = (time / 3600);
	if (tt == 0) {
	    str.append("00");
	} else {
	    if (tt < 10) str.append("0");
	    str.append(tt);
	    time -= tt*3600;
	}
	
	str.append(":");
	
	tt = (time / 60);
	if (tt == 0) {
	    str.append("00");
	} else {
	    if (tt < 10) str.append("0");
	    str.append(tt);
	    time -= tt*60;
	}
	
	str.append(":");
	
	if (tt == 0) {
	    str.append("00");
	} else {
	    if (time < 10) str.append("0");
	    str.append(time);
	}
	
	return str.toString();
    }

}
