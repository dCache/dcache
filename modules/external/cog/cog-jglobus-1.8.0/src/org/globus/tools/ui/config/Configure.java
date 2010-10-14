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
package org.globus.tools.ui.config;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import org.globus.gsi.CertUtil;

public class Configure {

    // --------- CERTIFICATE VERIFICATION ---------

    public static X509Certificate loadAndVerifyCertificate(String file)
	throws Exception {
	
	X509Certificate cert = null;
	String msg = null;
	
	try {
	    cert = CertUtil.loadCertificate(file);
	    cert.checkValidity();
	} catch (IOException e) {
	    msg = "Unable to load certificate : " + e.getMessage();
	} catch (GeneralSecurityException e) {
	    msg = "Invalid certificate : " + e.getMessage();
	}
	
	if (msg != null) {
	    throw new Exception(msg);
	} else {
	    return cert;
	}
    }


    public static X509Certificate verifyUserCertificate(String file)
	throws Exception {
	
	X509Certificate cert = loadAndVerifyCertificate(file);
	
	if (cert.getSubjectDN().equals(cert.getIssuerDN())) {
	    throw new Exception("This is a self-signed certificate.");
	}
	
	return cert;
    }
    
    public static X509Certificate verifyCertWithCA(String file,
						   X509Certificate usercert)
	throws Exception {
	
	X509Certificate cert = loadAndVerifyCertificate(file);
	    
	if (!cert.getSubjectDN().equals(cert.getIssuerDN())) {
	    throw new Exception("This is NOT a self-signed certificate.");
	}

	try {
	    // verify if this is the right ca cert...
	    usercert.verify(cert.getPublicKey());
	} catch(GeneralSecurityException e) {
	    throw new Exception("User certificate probably not signed by this" +
				" CA : " + e.getMessage());
	}

	return cert;
    }

}



