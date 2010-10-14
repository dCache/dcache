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

import java.util.Vector;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.globus.util.I18n;

/**
 * Class that holds signing policy information. It contains the CA
 * subject DN for which the signing policy is stored, an optional name
 * of the file from which the policy was read in and if available a
 * vector of Pattern that contain the DN pattern. The Pattern should
 * use the grammar described in java.util.Pattern, see 
 * SigningPolicyParser#getPattern().
 *
 * Note: All subject DNs should be in Globus format (with slashes) and in
 * order (that is NOT reversed)
 */
public class SigningPolicy {

    private Vector patterns;
    private String caDN;
    private String fileName;

    /**
     * Creates a signing policy for the given CA DN
     *
     * @param caDN_
     *       Distinguished Name of the CA, in Globus format (with
     *       slashes) and not reversed. See CertUtil#toGlobusID()
     */
    public SigningPolicy(String caDN_) {

        this(caDN_, null);
    }

    /**
     * Creates a signing policy for the given CA DN and vector of
     * policies. The policies are stored as java.util.Pattern, where
     * each Pattern provdes a regexp format of the signing policy. 
     * See SigningPolicyParser#getPattern().
     *
     * @param caDN_
     *       Distinguished Name of the CA, in Globus format (with
     *       slashes) and not reversed. See CertUtil.toGlobusID()
     * @param patterns_
     *       Vector of java.util.Pattern, each representing an allowed
     *       subject DN policy.
     */
    public SigningPolicy(String caDN_, Vector patterns_) {
        this(caDN_, patterns_, null);
    }

    /**
     * Creates a signing policy for the given CA DN and vector of
     * policies. The policies are stored as java.util.Pattern, where
     * each Pattern provdes a regexp format of the signing policy. 
     * See SigningPolicyParser#getPattern(). The optional filename
     * stores the file from which the signing policy was read in.
     *
     * @param caDN_
     *       Distinguished Name of the CA, in Globus format (with
     *       slashes) and not reversed. See CertUtil.toGlobusID()
     * @param patterns_
     *       Vector of java.util.Pattern, each representing an allowed
     *       subject DN policy.
     * @param fileName_
     *       name of the signing policy file.
     */
    public SigningPolicy(String caDN_, Vector patterns_, String fileName_) {

        if ((caDN_ == null) || (caDN_.trim().equals(""))) {
            throw new IllegalArgumentException();
        }
        
        this.caDN = caDN_;
        this.patterns = patterns_;
        this.fileName = fileName_;
    }

    /**
     * Returns the allowed subject DN patterns. 
     *
     * @return Vector of patterns, each representing an allowed
     *        subject DN policy. Can be null or vector of size zero.
     */
    public Vector getPatterns() {
        return patterns;
    }

    /**
     * Returns the CA subject DN
     *
     *@return CA's DN
     */
    public String getCaSubject() {
        return this.caDN;
    }

    /**
     * Returns file name
     *
     * @return name of file from which the signing policy was read. Can
     *        be null.
     */
    public String getFileName() {
        return this.fileName;
    }

    /**
     * Sets file name from which the signing policy was read.
     *
     * @param fileName_
     *        File name.
     */
    public void setFileName(String fileName_) {
        this.fileName = fileName_;
    }

    /**
     * Method to determine if a signing policy is available for a
     * given DN. 
     * 
     * @return If the patterns vector is not null and has atleast one
     * element, true is returned. Else the method returns false.
     */
    public boolean isPolicyAvailable() {
        
        if ((this.patterns == null) || 
            (this.patterns.size() < 1)) {
            return false;
        }

        return true;
    }

    /**
     * Method to determine if the subject DN matches one of the
     * patterns in the signing policy. Returns true if no policy is
     * available, use isPolicyAvailable() to check presence of policy.
     *
     * @param subjectDN
     *        Subject DN to match
     * @return
     *        Returns true of the subject DN matches one of the
     *        patterns in the policy or if no policy is available. Returns
     *        false otherwise.
     */
    public boolean isValidSubject(String subjectDN) {

        if (subjectDN == null) {
            throw new IllegalArgumentException();
        }
        
        // no policy
        if ((this.patterns == null) || 
            (this.patterns.size() < 1)) {
            return true;
        }

        subjectDN = SigningPolicyParser.normalizeDN(subjectDN);

        int size = this.patterns.size();
        for (int i=0; i<size; i++) {
            Pattern pattern = (Pattern)patterns.get(i);
            Matcher matcher = pattern.matcher(subjectDN);
            boolean valid = matcher.matches();
            if (valid) {
                return true;
            }
        }
        
        return false;
    }
}
