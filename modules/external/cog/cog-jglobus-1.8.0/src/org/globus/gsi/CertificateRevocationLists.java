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

import java.security.cert.X509CRL;
import java.util.Map;
import java.util.Iterator;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.StringTokenizer;
import java.io.File;
import java.io.FilenameFilter;

import org.globus.common.CoGProperties;
import org.globus.util.TimestampEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CertificateRevocationLists {
    
    private static Log logger =
        LogFactory.getLog(CertificateRevocationLists.class.getName());

    public static final CrlFilter crlFileFilter = new CrlFilter();

    // the list of ca cert locations needed for getDefaultCRL call
    private static String prevCaCertLocations = null;
    // the default crl locations list derived from prevCaCertLocations
    private static String defaultCrlLocations = null;
    private static CertificateRevocationLists defaultCrl  = null;
    
    private Map crlFileMap;
    private Map crlIssuerDNMap;

    private CertificateRevocationLists() {}

    public X509CRL[] getCrls() {
        if (this.crlIssuerDNMap == null) {
            return null;
        }
        Collection crls = this.crlIssuerDNMap.values();
        X509CRL[] retCrls = new X509CRL[crls.size()];
        Iterator iterator = crls.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            retCrls[i] = (X509CRL)iterator.next();
            i++;
        }
        return retCrls;
    }

    public X509CRL getCrl(String issuerName) {
        if (this.crlIssuerDNMap == null) {
            return null;
        }
        return (X509CRL)this.crlIssuerDNMap.get(issuerName);
    }

    public static FilenameFilter getCrlFilter() {
        return crlFileFilter;
    }
    
    public static class CrlFilter implements FilenameFilter {
        public boolean accept(File dir, String file) {
            int length = file.length();
            if (length > 3 && 
                file.charAt(length-3) == '.' &&
                file.charAt(length-2) == 'r' &&
                file.charAt(length-1) >= '0' && 
                file.charAt(length-1) <= '9') {
                return true;
            }
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

        StringTokenizer tokens = new StringTokenizer(locations, ",");
        File crlFile = null;
        
        Map newCrlFileMap = new HashMap();
        Map newCrlIssuerDNMap = new HashMap();

        while(tokens.hasMoreTokens()) {
            crlFile = new File(tokens.nextToken().toString().trim());

            if (!crlFile.canRead()) {
                logger.debug("Cannot read: " + crlFile.getAbsolutePath());
                continue;
            }

            if (crlFile.isDirectory()) {
                String[] crlFiles = crlFile.list(getCrlFilter());
                if (crlFiles == null) {
                    logger.debug("Cannot load CRLs from " +
                                 crlFile.getAbsolutePath() + " directory.");
                } else {
                    logger.debug("Loading CRLs from " +
                                 crlFile.getAbsolutePath() + " directory.");
                    for (int i = 0; i < crlFiles.length; i++) {
                        String crlFilename = crlFile.getPath() + 
                            File.separatorChar + crlFiles[i];
                        File crlFilenameFile = new File(crlFilename);
                        if (crlFilenameFile.canRead()) {
                            loadCrl(crlFilename, 
                                    crlFilenameFile.lastModified(),
                                    newCrlFileMap, newCrlIssuerDNMap);
                        } else {
                            logger.debug("Cannot read: " + 
                                         crlFilenameFile.getAbsolutePath());
                        }
                    }
                }
            } else {
                loadCrl(crlFile.getAbsolutePath(), 
                        crlFile.lastModified(),
                        newCrlFileMap, newCrlIssuerDNMap);
            }
        }
        
        this.crlFileMap = newCrlFileMap;
        this.crlIssuerDNMap = newCrlIssuerDNMap;
    }

    /**
     * Method loads a CRL provided a mapping for it is<br>
     * a) Not already in the HashMap
     * b) In the HashMap, but
     *    - mapped to null object
     *    - the CRLEntry has a modified time that is older that latest time
     */
    private void loadCrl(String crlPath, 
                         long latestLastModified, 
                         Map newCrlFileMap,
                         Map newCrlIssuerDNMap) {
        X509CRL crl = null;

        if (this.crlFileMap == null) {
            this.crlFileMap = new HashMap();
        }

        TimestampEntry crlEntry = (TimestampEntry)this.crlFileMap.get(crlPath);
        try {
            if (crlEntry == null) {
                logger.debug("Loading " + crlPath + " CRL.");
                crl = CertUtil.loadCrl(crlPath);
                crlEntry = new TimestampEntry();
                crlEntry.setValue(crl);
                crlEntry.setLastModified(latestLastModified);
                crlEntry.setDescription(crl.getIssuerDN().getName());
            } else if (latestLastModified > crlEntry.getLastModified()) {
                logger.debug("Reloading " + crlPath + " CRL.");
                crl = CertUtil.loadCrl(crlPath);
                crlEntry.setValue(crl);
                crlEntry.setLastModified(latestLastModified);
                crlEntry.setDescription(crl.getIssuerDN().getName());
            } else {
                logger.debug("CRL " + crlPath + " is up-to-date.");
                crl = (X509CRL)crlEntry.getValue();
            }
            newCrlFileMap.put(crlPath, crlEntry);
            newCrlIssuerDNMap.put(crlEntry.getDescription(), crl);
        } catch (Exception e) {
            logger.error("CRL " + crlPath + " failed to load.", e);
        }
    }
    
    public static CertificateRevocationLists 
        getCertificateRevocationLists(String locations) {
        CertificateRevocationLists crl = new CertificateRevocationLists();
        crl.reload(locations);
        return crl;
    }

    public static synchronized 
        CertificateRevocationLists getDefaultCertificateRevocationLists() {
        return getDefault();
    }
    
    public static void 
        setDefaultCertificateRevocationList(CertificateRevocationLists crl) {
        defaultCrl = crl;
    }
    
    public static synchronized CertificateRevocationLists getDefault() {
        if (defaultCrl == null) {
            defaultCrl = new DefaultCertificateRevocationLists();
        }
        defaultCrl.refresh();
        return defaultCrl;
    }

    public String toString() {
        if (this.crlIssuerDNMap == null) {
            return  "crl list is empty";
        } else {
            return this.crlIssuerDNMap.toString();
        }
    }

    private static class DefaultCertificateRevocationLists 
        extends CertificateRevocationLists {

         private final long lifetime;
         private long lastRefresh;

         public DefaultCertificateRevocationLists() {
             lifetime =
                 CoGProperties.getDefault().getCRLCacheLifetime();
         }
 
        public void refresh() {
             long now = System.currentTimeMillis();
             if (lastRefresh + lifetime <= now) {
		 reload(getDefaultCRLLocations());
	     }
	     lastRefresh = now;
        }

        private static synchronized String getDefaultCRLLocations() {
            String caCertLocations = 
                CoGProperties.getDefault().getCaCertLocations();
            
            if (prevCaCertLocations == null || 
                !prevCaCertLocations.equals(caCertLocations)) {
                
                if (caCertLocations == null) { 
                    logger.debug("No CA cert locations specified");
                    prevCaCertLocations = null;
                    defaultCrlLocations = null;
                } else {
                    StringTokenizer tokens = 
                        new StringTokenizer(caCertLocations, ",");
                    File crlFile = null;
                    LinkedList crlDirs = new LinkedList();
                    while(tokens.hasMoreTokens()) {
                        String crlFileName = 
                            tokens.nextToken().toString().trim();
                        crlFile = new File(crlFileName);
                        if (crlFile.isDirectory()) {
                            // all all directories
                        } else if (crlFile.isFile()) {
                            // add parent directory
                            crlFileName = crlFile.getParent();
                        } else {
                            // skip other types
                            continue;
                        }
                        
                        // don't add directories twice
                        if (crlFileName != null && 
                            !crlDirs.contains(crlFileName)) {
                            crlDirs.add(crlFileName);
                        }
                    }
                    
                    ListIterator iterator = crlDirs.listIterator(0);
                    String locations = null;
                    while (iterator.hasNext()) {
                        if (locations == null) {
                            locations = (String)iterator.next();
                        } else {
                            locations = locations + "," 
                                + (String)iterator.next();
                        }
                    }
                    
                    // set defaults
                    prevCaCertLocations = caCertLocations;
                    defaultCrlLocations = locations;
                }
            }

            return defaultCrlLocations;
        }
    }

}
