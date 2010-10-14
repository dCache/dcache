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
package org.globus.gsi;

import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Vector;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Collection;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.io.FilenameFilter;

import org.globus.common.CoGProperties;
import org.globus.util.TimestampEntry;
import org.globus.gsi.ptls.PureTLSUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

/**
 * Class that reads in and maintains trusted certificates and signing
 * policy associated with the CAs.
 */
public class TrustedCertificates implements Serializable {
    
    private static Log logger =
        LogFactory.getLog(TrustedCertificates.class.getName());

    public static final CertFilter certFileFilter = new CertFilter();

    private static TrustedCertificates trustedCertificates = null;

    // DN is in the format in certificates
    private Map certSubjectDNMap;
    private Map certFileMap;
    private boolean changed;

    // DN is in Globus format here, without any reversal.
    private Map policyDNMap;
    private Map policyFileMap;

    // Vector of X.509 Certificate objects
    private Vector certList;

    /**
     * Default signing policy suffix. The files are expected to be
     * <caHash>.signing_policy in the same directory as the trusted 
     * certificates.
     */
    public static String SIGNING_POLICY_FILE_SUFFIX = ".signing_policy";
    
    protected TrustedCertificates() {}
    
    public TrustedCertificates(X509Certificate [] certs) {
        this(certs, null);
    }

    public TrustedCertificates(X509Certificate [] certs,
                               SigningPolicy[] policies) {

        // FIXME: this could cause NPE
        this.certSubjectDNMap = new HashMap();
        for (int i=0;i<certs.length;i++) {
            String dn = certs[i].getSubjectDN().toString();
            this.certSubjectDNMap.put(dn,certs[i]);
        }
        
        if (policies != null) {
            this.policyDNMap = new HashMap();        
            for (int i=0; i<policies.length; i++) {
                if (policies[i] != null) {
                    this.policyDNMap.put(policies[i].getCaSubject(), 
                                         policies[i]);
                }
            }
        }
    }

    /**
     * Returns the trusted certificates as a Vector of X509Certificate objects.
     */
    public synchronized Vector getX509CertList() throws GeneralSecurityException {
	if (certList == null) {
            certList = 
                PureTLSUtil.certificateChainToVector(getCertificates());
	}
	return certList;
    }

    public X509Certificate[] getCertificates() {
        if (this.certSubjectDNMap == null) {
            return null;
        }
        Collection certs = this.certSubjectDNMap.values();
        X509Certificate [] retCerts = new X509Certificate[certs.size()];
        Iterator iterator = certs.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            retCerts[i++] = (X509Certificate)iterator.next();
        }
        return retCerts;
    }
    
    public X509Certificate getCertificate(String subject) {
        if (this.certSubjectDNMap == null) {
            return null;
        }
        return (X509Certificate)this.certSubjectDNMap.get(subject);
    }

    /**
     * Returns all signing policies 
     */
    public SigningPolicy[] getSigningPolicies() {

        if (this.policyDNMap == null) {
            return null;
        }

        Collection values = this.policyDNMap.values();
        SigningPolicy[] policies = new SigningPolicy[values.size()];
        Iterator iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            policies[i++] = (SigningPolicy)iterator.next();
        }
        return policies;
    }

    /**
     * Returns signing policy associated with the given CA subject.
     * 
     * @param subject
     *        CA's subject DN for which signing policy is
     *        required. The DN should be in Globus format (with slashes) and
     *        not reversed. See CertUtil.toGlobusID();
     * @return 
     *        Signing policy object associated with the CA's DN. Null
     *        if no policy was configured. SigningPolicy object might not
     *        have any applicable policy if none was configured or none was
     *        found in the policy file configured.
     */
    public SigningPolicy getSigningPolicy(String subject) {
        
        if (this.policyDNMap == null) {
            return null;
        }

        return (SigningPolicy) this.policyDNMap.get(subject);
    }

    /** 
     * Loads X509 certificates and signing policy files from specified 
     * locations. The locations can be either files or
     * directories. The directories will be automatically traversed
     * and all files in the form of <i>hashcode.number</i> and will be
     * loaded automatically as trusted certificates. An attempt will
     * be made to load signing policy for the CA associated with
     * that hashcode from <hashcode>.signing_policy. If policy file is
     * not found, no error will be thrown, only path validation code
     * enforces the signing policy requirement.
     *
     * @param locations a list of certificate files/directories to load 
     *                  the certificates from. The locations are comma 
     *                  separated.
     *
     * @return <code>java.security.cert.X509Certificate</code> an array 
     *         of loaded certificates
     */    
    public static X509Certificate[] loadCertificates(String locations) {
        TrustedCertificates tc = TrustedCertificates.load(locations);
        return (tc == null) ? null : tc.getCertificates();
    }

    public static TrustedCertificates load(String locations) {
        TrustedCertificates tc = new TrustedCertificates();
        tc.reload(locations);
        return tc;
    }

    public static FilenameFilter getCertFilter() {
        return certFileFilter;
    }
    
    public static class CertFilter implements FilenameFilter {
        public boolean accept(File dir, String file) {
            int length = file.length();
            if (length > 2 && 
                file.charAt(length-2) == '.' &&
                file.charAt(length-1) >= '0' && 
                file.charAt(length-1) <= '9') return true;
            return false;
        }
    }

    public void refresh() {
        reload(null);
    }

    public synchronized void reload(String locations) {
        if (locations == null) {
            return;
        }

        this.changed = false;

        StringTokenizer tokens = new StringTokenizer(locations, ",");
        File caFile            = null;

        Map newCertFileMap = new HashMap();
        Map newCertSubjectDNMap = new HashMap();
        Map newSigningFileMap = new HashMap();
        Map newSigningDNMap = new HashMap();

        while(tokens.hasMoreTokens()) {
            caFile = new File(tokens.nextToken().toString().trim());

            if (!caFile.canRead()) {
                logger.debug("Cannot read: " + caFile.getAbsolutePath());
                continue;
            }
            
            if (caFile.isDirectory()) {
                String[] caCertFiles = caFile.list(getCertFilter());
                if (caCertFiles == null) {
                    logger.debug("Cannot load certificates from " + 
                                 caFile.getAbsolutePath() + " directory.");
                } else {
                    logger.debug("Loading certificates from " + 
                                 caFile.getAbsolutePath() + " directory.");
                    for (int i = 0; i < caCertFiles.length; i++) {
                        String caFilename = caFile.getPath() + 
                            File.separatorChar + caCertFiles[i];
                        File caFilenameFile = new File(caFilename);
                        String policyFilename = 
                            getPolicyFileName(caFilename);
                        File policyFile = new File(policyFilename);
                        if (caFilenameFile.canRead()) {
                            loadCert(caFilename,
                                     caFilenameFile.lastModified(),
                                     newCertFileMap, newCertSubjectDNMap,
                                     policyFilename, 
                                     policyFile.lastModified(),
                                     newSigningFileMap, newSigningDNMap);
                        } else {
                            logger.debug("Cannot read: " + 
                                         caFilenameFile.getAbsolutePath());
                        }
                    }
                }
            } else {
                String caFilename = caFile.getAbsolutePath();
                String policyFilename = 
                    getPolicyFileName(caFilename);
                File policyFile = new File(policyFilename);
                loadCert(caFilename,
                         caFile.lastModified(),
                         newCertFileMap, newCertSubjectDNMap,
                         policyFilename, 
                         policyFile.lastModified(),
                         newSigningFileMap, newSigningDNMap);
            }
        }
        
        // in case certificates were removed
        if (!this.changed && 
            this.certFileMap != null && 
            this.certFileMap.size() != newCertFileMap.size()) {
            this.changed = true;
        }

        this.certFileMap = newCertFileMap;
        this.certSubjectDNMap = newCertSubjectDNMap;

        this.policyFileMap = newSigningFileMap;
        this.policyDNMap = newSigningDNMap;

	if (this.changed) {
	    this.certList = null;
	}
    }

    /**
     * Method loads a certificate/signing policy provided a mapping
     * for it is<br>
     * a) Not already in the HashMap
     * b) In the HashMap, but
     *    - mapped to null object
     *    - the CertEntry has a modified time that is older that latest time
     */
    private void loadCert(String certPath, long latestLastModified, 
                          Map newCertFileMap, Map newCertSubjectDNMap, 
                          String policyPath, long policyModified, 
                          Map newPolicyFileMap, Map newPolicyDNMap) {

        X509Certificate cert = null;
        
        if (this.certFileMap == null) {
            this.certFileMap = new HashMap();
        }

        if (this.policyFileMap == null) {
            this.policyFileMap = new HashMap();
        }

        TimestampEntry certEntry = 
            (TimestampEntry)this.certFileMap.get(certPath);
        TimestampEntry policyEntry =
            (TimestampEntry)this.policyFileMap.get(policyPath);
        try {
            if (certEntry == null) {
                logger.debug("Loading " + certPath + " certificate.");
                cert = CertUtil.loadCertificate(certPath);
                String caDN = cert.getSubjectDN().getName();
                certEntry = new TimestampEntry();
                certEntry.setValue(cert);
                certEntry.setLastModified(latestLastModified);
                certEntry.setDescription(caDN);
                this.changed = true;
                // load signing policy file
                logger.debug("Loading " + policyPath + " signing policy.");
                policyEntry = getPolicyEntry(policyPath, policyModified, caDN);
            } else if (latestLastModified > certEntry.getLastModified()) {
                logger.debug("Reloading " + certPath + " certificate.");
                cert = CertUtil.loadCertificate(certPath);
                String caDN = cert.getSubjectDN().getName();
                certEntry.setValue(cert);
                certEntry.setLastModified(latestLastModified);
                certEntry.setDescription(caDN);
                this.changed = true;
                if (policyModified > policyEntry.getLastModified()) {
                    logger.debug("Reloading " + policyPath 
                                 + " signing policy.");
                    policyEntry = getPolicyEntry(policyPath, policyModified, 
                                                 caDN);
                }
            } else {
                logger.debug("Certificate " + certPath + " is up-to-date.");
                cert = (X509Certificate)certEntry.getValue();
                String caDN = cert.getSubjectDN().getName();
                if (policyModified > policyEntry.getLastModified()) {
                    logger.debug("Reloading " + policyPath 
                                 + " signing policy.");
                    policyEntry = getPolicyEntry(policyPath, policyModified, 
                                                 caDN);
                }
            }
            newCertFileMap.put(certPath, certEntry);
            newCertSubjectDNMap.put(certEntry.getDescription(), cert);
            newPolicyFileMap.put(policyPath, policyEntry);
            newPolicyDNMap.put(policyEntry.getDescription(), 
                               policyEntry.getValue());
        } catch (SigningPolicyParserException e) {
            logger.warn("Signing policy " + policyPath + " failed to load. "
                        + e.getMessage());
            logger.debug("Signing policy load error", e);
        } catch (IOException e) {
            logger.warn("Certificate " + certPath + " failed to load." 
                        + e.getMessage());
            logger.debug("Certificate load error", e);
        } catch (Exception e) {
            logger.warn("Certificate " + certPath + " or Signing policy "
                        + policyPath + " failed to load. " + e.getMessage());
            logger.debug("Certificate/Signing policy load error.", e);
        }
    }
    
    /**
     * The signing policy file is parsed and an entry to store in a
     * map is created here.
     */
    private TimestampEntry getPolicyEntry(String policyPath, 
                                          long policyModified, String caDN) 
        throws SigningPolicyParserException {

        logger.debug("Policy path " + policyPath);
        logger.debug("caDN as is " + caDN);

        // CA DN in signing policy files are in Globus format. There
        // is no reason to reverse it, but the format needs to use
        // slashes rather than comma.
        String globusDN = CertUtil.toGlobusID(caDN, true);

        SigningPolicy policy = null;
        File policyFile = new File(policyPath);
        if (policyFile.exists()) {
            policy = SigningPolicyParser.getPolicy(policyPath, globusDN);
        }
        
        TimestampEntry policyEntry = new TimestampEntry();
        policyEntry.setValue(policy);
        policyEntry.setLastModified(policyModified);
        policyEntry.setDescription(globusDN);
        return policyEntry;
    }

    /**
     * Signing policy name is created as <hashcode>.signing_policy.
     */
    private String getPolicyFileName(String caFileName) {
        return caFileName.substring(0, caFileName
                                    .lastIndexOf(".")) + 
            SIGNING_POLICY_FILE_SUFFIX ;
    }

    /**
     * Indicates if the last reload caused new certificates to be loaded or
     * existing certificates to be reloaded or any certificates removed
     */
    public boolean isChanged() {
        return this.changed;
    }

    /**
     * Obtains the default set of trusted certificates and signing policy
     *
     * @return TrustedCertificates object.
     */
    public static synchronized TrustedCertificates 
        getDefaultTrustedCertificates() {

        return getDefault();
    }

    /**
     * Sets the default set of trusted certificates to use.
     *
     * @param trusted the new set of trusted certificates to use.
     */    
    public static void 
        setDefaultTrustedCertificates(TrustedCertificates trusted) {

        trustedCertificates = trusted;
    }
    
    /**
     * Obtains the default set of trusted certificates and signing policy
     *
     * @return TrustedCertificates object. 
     */
    public static synchronized TrustedCertificates getDefault() {
        if (trustedCertificates == null) {
            trustedCertificates = new DefaultTrustedCertificates();
        }
        trustedCertificates.refresh();
        return trustedCertificates;
    }
    
    private static class DefaultTrustedCertificates 
        extends TrustedCertificates {
        
        public void refresh() {
            reload(CoGProperties.getDefault().getCaCertLocations());
        }
    }

    public String toString() {
        String returnStr = "";
        if (this.certSubjectDNMap == null) {
            returnStr =  "Certificate list is empty.";
        } else {
            returnStr = this.certSubjectDNMap.toString();
        }
        
        if (this.policyDNMap == null) {
            returnStr = returnStr + "Signing policy list is empty.";
        } else {
            returnStr = returnStr + this.policyDNMap.toString();
        }
        return returnStr;
    }
}

