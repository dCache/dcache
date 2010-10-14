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
package org.globus.gsi.ptls;

import java.util.Vector;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import org.globus.gsi.SigningPolicy;
import org.globus.gsi.TrustedCertificates;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A PureTLS-optimized version of the <code>TrustedCertificates</code> code.
 */
public class PureTLSTrustedCertificates extends TrustedCertificates {

    private static Log logger = 
	LogFactory.getLog(PureTLSTrustedCertificates.class.getName());

    private static PureTLSTrustedCertificates ptlsCerts = null;

    private TrustedCertificates tc;
    private Vector certList = null;

    protected PureTLSTrustedCertificates() {
    }

    public PureTLSTrustedCertificates(TrustedCertificates tc) {
	this.tc = tc;
    }
    
    protected void setTrustedCertificates(TrustedCertificates tc) {
	this.tc = tc;
    }

    public X509Certificate[] getCertificates() {
	return this.tc.getCertificates();
    }
    
    public X509Certificate getCertificate(String subject) {
	return this.tc.getCertificate(subject);
    }

    public SigningPolicy[] getSigningPolicies() {
	return this.tc.getSigningPolicies();
    }

    public SigningPolicy getSigningPolicy(String subject) {
        return this.tc.getSigningPolicy(subject);
    }

    public void refresh() {
        reload(null);
    }

    public synchronized void reload(String locations) {
	this.tc.reload(locations);
    }

    /**
     * Returns the trusted certificates as a Vector of X509Cert objects.
     */
    public synchronized Vector getX509CertList() {
	if (this.tc.isChanged() || this.certList == null) {
	    try {
		this.certList = 
		    PureTLSUtil.certificateChainToVector(getCertificates());
	    } catch (GeneralSecurityException e) {
		logger.debug("Failed to convert certificates", e);
	    }
	}
	return this.certList;
    }
    
    public static synchronized PureTLSTrustedCertificates 
	getDefaultPureTLSTrustedCertificates() {
	if (ptlsCerts == null) {
	    ptlsCerts = new PureTLSTrustedCertificates();
	}
	ptlsCerts.setTrustedCertificates(TrustedCertificates.getDefault());
	return ptlsCerts;
    }
}
