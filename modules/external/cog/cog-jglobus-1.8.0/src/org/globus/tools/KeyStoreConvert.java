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

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.security.PrivateKey;
import java.security.KeyStore;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.CertUtil;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.common.Version;
import org.globus.common.CoGProperties;
import org.globus.util.Util;

public class KeyStoreConvert {

    public static final String DEFAULT_ALIAS         = "globus";
    public static final String DEFAULT_PASSWORD      = "globus";
    public static final String DEFAULT_KEYSTORE_FILE = "globus.jks";

    private static final String message =
	"\n" +
	"Syntax: java KeyStoreConvert [options]\n" +
	"        java KeyStoreConvert -help\n\n" +
	"\tConverts Globus credentials (user key and certificate) into \n" +
	"\tJava keystore format (JKS format supported by Sun).\n\n" + 
	"\tOptions\n" +
	"\t-help | -usage\n" +
	"\t\tDisplays usage.\n" +
	"\t-version\n" +
	"\t\tDisplays version.\n" +
	"\t-debug\n" +
	"\t\tEnables extra debug output.\n" +
	"\t-cert     <certfile>\n" +
	"\t\tNon-standard location of user certificate.\n" +
	"\t-key      <keyfile>\n" +
	"\t\tNon-standard location of user key.\n" +
	"\t-alias    <alias>\n" +
	"\t\tKeystore alias entry. Defaults to '" +DEFAULT_ALIAS + "'\n" +
	"\t-password <password>\n" +
	"\t\tKeystore password. Defaults to '" +DEFAULT_PASSWORD + "'\n" +
	"\t-out      <keystorefile>\n" +
	"\t\tLocation of the Java keystore file. Defaults to\n" +
	"\t\t'" + DEFAULT_KEYSTORE_FILE + "'\n\n";
    
    public static void main(String args[]) {
	
	CoGProperties props = CoGProperties.getDefault();

	boolean error       = false;
	boolean debug       = false;
	String alias        = DEFAULT_ALIAS;
	String password     = DEFAULT_PASSWORD;
	String keyFile      = props.getUserKeyFile();
	String certFile     = props.getUserCertFile();
	String keyStoreFile = DEFAULT_KEYSTORE_FILE;

	for (int i = 0; i < args.length; i++) {
	    if (args[i].equalsIgnoreCase("-debug")) {
		debug = true;
	    } else if (args[i].equalsIgnoreCase("-out")) {
		keyStoreFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-key")) {
		keyFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-cert")) {
		certFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-alias")) {
		alias = args[++i];
	    } else if (args[i].equalsIgnoreCase("-pwd") ||
		       args[i].equalsIgnoreCase("-password")) {
		password = args[++i];
	    } else if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.err.println(message);
		System.exit(1);
	    } else {
		System.err.println("Error: Argument not recognized: " + args[i]);
		error = true;
	    }
	}
	
	if (error) return;

	if (keyStoreFile == null) {
	    System.err.println("Error: Java key store output file is not specified.");
	    return;
	}

	if (debug) {
	    System.out.println("### Current settings ###");
	    System.out.println("       Certificate file : " + certFile);
	    System.out.println("        SSLeay key file : " + keyFile);
	    System.out.println("     Java keystore file : " + keyStoreFile);
	    System.out.println("        Key entry Alias : " + alias);
	    System.out.println(" Java keystore password : " + password);
	}

	File f = new File(keyStoreFile);
	if (f.exists()) {
	    System.err.println("Error: Output file (" + keyStoreFile + ") already exists.");
	    return;
	}
	
	int rs = createKeyStore(certFile,
				keyFile,
				alias,
				password,
				keyStoreFile,
				debug);
	
	// Workaround to fix JNI bug (noticeable on some RedHat 6.1 and 7.1 systems)
	// for a description of the bug see http://java.sun.com/j2se/1.3/relnotes.html
	// and there grep for "ERROR REPORT"
	// gavin McCance <mccance@a5.ph.gla.ac.uk>
	
	System.exit(rs);
    }

    private static int createKeyStore(String certFile,
				      String keyFile,
				      String alias,
				      String password,
				      String keyStoreFile,
				      boolean debug) {
	
	X509Certificate [] certs = new X509Certificate[1];
	PrivateKey key = null;
	
	try {
	    certs[0] = CertUtil.loadCertificate(certFile);
	} catch(Exception e) {
	    System.err.println("Failed to load certificate: " + e.getMessage());
	    return -1;
	}
	
	try {
	    OpenSSLKey sslkey = new BouncyCastleOpenSSLKey(keyFile);
	    
	    if (sslkey.isEncrypted()) {
		String pwd = Util.getPrivateInput("Enter pass phrase: ");
		
		if (pwd == null) {
		    // user canceled
		    return -2;
		}
		
		sslkey.decrypt(pwd);
	    }
	    
	    key = sslkey.getPrivateKey();
	    
	} catch(IOException e) {
	    System.err.println("Failed to load key: " + e.getMessage());
	    return -1;
	} catch(GeneralSecurityException e) {
	    System.err.println("Error: Wrong pass phrase");
	    if (debug) {
		e.printStackTrace();
	    }
	    return -1;
	}
	
	System.out.println("Creating Java keystore...");

	FileOutputStream out = null;
	
	try {
	    KeyStore ks = KeyStore.getInstance("JKS", "SUN");
	    ks.load(null, null);
	    // this takes a while for some reason
	    ks.setKeyEntry(alias, key, password.toCharArray(), certs);
	    out = new FileOutputStream(keyStoreFile);
	    ks.store(out, password.toCharArray());
	} catch(Exception e) {
	    System.err.println("Failed to create Java key store: " + e.getMessage());
	    return -1;
	} finally {
	    if (out != null) {
		try { out.close(); } catch(IOException ee) {}
	    }
	}
	
	System.out.println("Java keystore file (" + keyStoreFile + 
			   ") successfully created.");

	return 0;
    }
    
}
