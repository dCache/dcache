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

import java.security.cert.X509Certificate;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.globus.gsi.CertUtil;
import org.globus.common.CoGProperties;
import org.globus.common.Version;

/** Returns information about the Cert
 * <pre>
 * Syntax: java CertInfo [-help] [-file certfile] [-all] [-subject] [...]
 * Displays certificate information. Unless the optional -file
 * argument is given, the default location of the file containing the
 * certficate is assumed:
 *   --  Config.getUserCertFile() 
 * Options
 *   -help, -usage                Display usage
 *   -version                     Display version
 *   -file certfile               Use 'certfile' at non-default location
 *   -globus                      Prints information in globus format
 * Options determining what to print from certificate
 *   -all                        Whole certificate
 *   -subject                    Subject string of the cert
 *   -issuer                     Issuer
 *   -startdate                  Validity of cert: start date
 *   -enddate                    Validity of cert: end date
 * </pre>
 */
public class CertInfo {

    private static final int SUBJECT  = 2;
    private static final int ISSUER   = 4;
    private static final int SDATE    = 8;
    private static final int EDATE    = 16;
    private static final int ALL      = 32;
    
    private static String message =
	"\n" +
	"Syntax: java CertInfo [-help] [-file certfile] [-all] [-subject] [...]\n\n" +
	"\tDisplays certificate information. Unless the optional \n" + 
	"\tfile argument is given, the default location of the file\n" +
	"\tcontaining the certficate is assumed:\n\n" +
	"\t  -- " + CoGProperties.getDefault().getUserCertFile() + "\n\n" + 
	"\tOptions\n" +
	"\t-help | -usage\n" + 
	"\t\tDisplay usage.\n" +
	"\t-version\n" + 
	"\t\tDisplay version.\n" +
	"\t-file certfile\n" + 
	"\t\tUse 'certfile' at non-default location.\n" +
	"\t-globus\n" + 
	"\t\tPrints information in globus format.\n\n" +
	"\tOptions determining what to print from certificate\n\n" +
	"\t-all\n" + 
	"\t\tWhole certificate.\n" +
	"\t-subject\n" + 
	"\t\tSubject string of the cert.\n" +
	"\t-issuer\n" + 
	"\t\tIssuer.\n" +
	"\t-startdate\n" + 
	"\t\tValidity of cert: start date.\n" +
	"\t-enddate\n" + 
	"\t\tValidity of cert: end date.\n\n";

    public static void main(String args[]) {
	
	String file         = null;
	int options         = 0;
	boolean error       = false;
	boolean globusStyle = false;
	boolean debug       = false;
	
	for (int i = 0; i < args.length; i++) {
	    if (args[i].equalsIgnoreCase("-file")) {
		file = args[++i];
	    } else if (args[i].equalsIgnoreCase("-subject")) {
		options |= SUBJECT;
	    } else if (args[i].equalsIgnoreCase("-issuer")) {
		options |= ISSUER;
	    } else if (args[i].equalsIgnoreCase("-startdate")) {
		options |= SDATE;
	    } else if (args[i].equalsIgnoreCase("-enddate")) {
		options |= EDATE;
	    } else if (args[i].equalsIgnoreCase("-all")) {
		options |= ALL;
	    } else if (args[i].equalsIgnoreCase("-globus")) {
		globusStyle = true;
	    } else if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.err.println(message);
		System.exit(1);
	    } else {
		System.err.println("Error: argument not recognized : " + args[i]);
		error = true;
	    }
    }
    
	if (error) {
	    System.err.println("\nUsage: java CertInfo [-help] [-file certfile] [-all] [-subject] [...]\n");
	    System.err.println("Use -help to display full usage.");
	    System.exit(1);
	}
    
	if (file == null) {
	    file = CoGProperties.getDefault().getUserCertFile();
	}

	X509Certificate cert = null;

	try {
	    cert = CertUtil.loadCertificate(file);
	} catch(Exception e) {
	    System.err.println("Unable to load the certificate : " + e.getMessage());
	    System.exit(1);
	}
    

	if (options == 0) {
	    options = SUBJECT | ISSUER | SDATE | EDATE;
	}

	if ((options & SUBJECT) != 0) {
	    String dn = null;
	    if (globusStyle) {
		dn = CertUtil.toGlobusID(cert.getSubjectDN());
	    } else {
		dn = cert.getSubjectDN().getName();
	    }
	    System.out.println("subject     : " + dn);
	}
    
	if ((options & ISSUER) != 0) {
	    String dn = null;
	    if (globusStyle) {
		dn = CertUtil.toGlobusID(cert.getIssuerDN());
	    } else {
		dn = cert.getIssuerDN().getName();
	    }
	    System.out.println("issuer      : " + dn);
	}
    
	TimeZone tz   = null;
	DateFormat df = null;
	if (globusStyle) {
	    tz = TimeZone.getTimeZone("GMT");
	    df = new SimpleDateFormat("MMM dd HH:mm:ss yyyy z");
	    df.setTimeZone(tz);
	}

	if ((options & SDATE) != 0) {
	    String dt = null;
	    if (globusStyle) {
		dt = df.format(cert.getNotBefore());
	    } else {
		dt = cert.getNotBefore().toString();
	    }
	    System.out.println("start date  : " + dt);
	}
	
	if ((options & EDATE) != 0) {
	    String dt = null;
	    if (globusStyle) {
		dt = df.format(cert.getNotAfter());
	    } else {
		dt = cert.getNotAfter().toString();
	    }
	    System.out.println("end date    : " + dt);
	}
	
	if ((options & ALL) != 0) {
	    System.out.println("certificate :");
	    System.out.println(cert.toString());
	}
    }
    
}
