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
package org.globus.gsi.test;

import java.util.Vector;

import java.io.InputStream;
import java.io.StringReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

import org.globus.gsi.SigningPolicy;
import org.globus.gsi.SigningPolicyParser;
import org.globus.gsi.SigningPolicyParserException;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class SigningPolicyParserTest extends TestCase {

    public static final String BASE = "org/globus/gsi/test/";
    private String SUCCESS_FILE = "samplePolicy.signing_policy";

    private String SINGLE_ALLOWED_DN = "5aba75cb.signing_policy";

    private String[] TAB_TEST_FILE = 
        new String[] { "afe55e66.signing_policy", "cf4ba8c8.signing_policy", 
                       "49f18420.signing_policy" };
    
    public SigningPolicyParserTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(SigningPolicyParserTest.class);
    }

    public void testPatternMatching() throws Exception { 

        // test getPattern method
        // no wildcards or question marks
        String patternStr = "abcdefgh";
        String patternR = (SigningPolicyParser.getPattern(patternStr))
            .pattern();
        assertTrue("abcdefgh".equals(patternR));

        // first character wildcard and question marks
        String pattern1Str = "*def?gh?";
        Pattern pattern1 = SigningPolicyParser.getPattern(pattern1Str);
        String pattern1R = pattern1.pattern();
        assertTrue((SigningPolicyParser.WILDCARD_PATTERN + "def" + SigningPolicyParser.SINGLE_PATTERN +"gh" + SigningPolicyParser.SINGLE_PATTERN).equals(pattern1R));

        // only wild cards
        String pattern2Str = "abc*def*gh";
        Pattern pattern2 = SigningPolicyParser.getPattern(pattern2Str);
        String pattern2R = pattern2.pattern();
        assertTrue(("abc" + SigningPolicyParser.WILDCARD_PATTERN + "def" + SigningPolicyParser.WILDCARD_PATTERN +"gh").equals(pattern2R));

        // test isValidDN methods
        // Add patern2, wildcards in middle
        Vector allowed = new Vector();
        allowed.add(pattern2);
        SigningPolicy policy = new SigningPolicy("foo", allowed);

        String subject21 = "abc12DEF34defdef56gh";
        assertTrue(policy.isValidSubject(subject21));        

        String subject22 = "123abc12def34defdef56gh";
        assertFalse(policy.isValidSubject(subject22));        

        String subject23 = "abc12def34defdef56gh123";
        assertFalse(policy.isValidSubject(subject23));        

        // wildcard as first and last character
        String pattern3Str = "*abc*def*gh*";
        Pattern pattern3 = SigningPolicyParser.getPattern(pattern3Str);
        allowed.clear();
        allowed.add(pattern3);
        policy = new SigningPolicy("foo", allowed);

        String subject31 = "ABC12def34defdef56gh";
        assertTrue(policy.isValidSubject(subject31));        

        String subject32 = "123abc12def34defdef56gh555";
        assertTrue(policy.isValidSubject(subject32));        

        // use of space and slashes, from old signing policy file
        String pattern4Str = "/C=US/O=Globus/*"; 
        Pattern pattern4 = SigningPolicyParser.getPattern(pattern4Str);
        allowed.clear();
        allowed.add(pattern4);

        policy = new SigningPolicy("foo", allowed);

        String subject41 = "/C=US/O=Globus/CN=Globus Certification Authority";
        assertTrue(policy.isValidSubject(subject41));        
        
        // wildcard as first character, question mark
        String pattern5Str = "*/C=US/O=Globus/CN=foo-?/CN=*"; 
        Pattern pattern5 = SigningPolicyParser.getPattern(pattern5Str);
        allowed.clear();
        allowed.add(pattern5);
        policy = new SigningPolicy("foo", allowed);

        String subject51 = "/C=US/O=Globus/CN=Globus Certification Authority";
        assertFalse(policy.isValidSubject(subject51));        
        String subject52 = "SOME/C=US/O=Globus/CN=foo-1/CN=a12b/CN=test space";
        assertTrue(policy.isValidSubject(subject52));       
        String subject53 = "/C=US/O=Globus/CN=foo-k/CN=";
        assertTrue(policy.isValidSubject(subject53));       
        String subject54 = "/C=US/O=Globus/CN=foo- /CN= ";
        assertTrue(policy.isValidSubject(subject54));
        String subject55 = "/C=US/O=Globus/CN=foo-123/CN=";
        assertFalse(policy.isValidSubject(subject55));

        // multiple question mark with punctuation
        String pattern6Str = "/C=US/O=global/CN=*/CN=user-??";
        Pattern pattern6 = SigningPolicyParser.getPattern(pattern6Str);
        allowed.clear();
        allowed.add(pattern6);
        policy = new SigningPolicy("foo", allowed);

        String subject61 = "/C=US/O=Globus/CN=foo/CN=user-12";
        assertFalse(policy.isValidSubject(subject61));       
        String subject62 = "/C=US/O=Global/CN=foo/CN=user-12";
        assertTrue(policy.isValidSubject(subject62));       
        String subject63 = "/C=US/O=global/CN=foo /CN=bar 1/CN=user-12";
        assertTrue(policy.isValidSubject(subject63));       

        // add multiple patterns and test validity if atleast one matches
        String pattern7Str = "/C=US/O=Globus/CN=*/CN=user-??";
        Pattern pattern7 = SigningPolicyParser.getPattern(pattern7Str);
        allowed.add(pattern7);
        policy = new SigningPolicy("foo", allowed);

        String subject71 = "/C=US/O=Globus/CN=foo /CN=bar 1/CN=user-12";
        assertTrue(policy.isValidSubject(subject71));  
        assertTrue(policy.isValidSubject(subject63));
    }

    // FIXME: ideally should be able to test raw output from the file
    // without the pattern pieces. API today does not allow it
    public void testFileSuccess() throws Exception {

        String name = BASE + SUCCESS_FILE;

	ClassLoader loader = SigningPolicyParserTest.class.getClassLoader();
        InputStream in = loader.getResourceAsStream(name);
        if (in == null) {
            throw new Exception("Unable to load: " + name);
        }

        SigningPolicy policy = SigningPolicyParser.getPolicy(new InputStreamReader(in), "/C=US/O=Globus/CN=Globus Certification Authority");
        assertTrue(policy != null);
        Vector allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 2);
        
        Vector patterns = new Vector(2);
        patterns.add(((Pattern)allowedDN.get(0)).pattern());
        patterns.add(((Pattern)allowedDN.get(1)).pattern());

        // given the getPattern method is already tested, assuming it
        // works here.
        Pattern p1 = SigningPolicyParser.getPattern("/C=us/O=Globus/*");
        assertTrue(patterns.contains(p1.pattern()));
        p1 = SigningPolicyParser.getPattern("/C=US/O=Globus/*");
        assertTrue(patterns.contains(p1.pattern()));
        p1 = SigningPolicyParser.getPattern("/C=us/O=National Computational Science Alliance/*");
        assertFalse(patterns.contains(p1.pattern()));

        in = loader.getResourceAsStream(name);
        if (in == null) {
            throw new Exception("Unable to load: " + name);
        }
        policy = SigningPolicyParser.getPolicy(new InputStreamReader(in), "/C=US/O=National Computational Science Alliance/CN=Globus Certification Authority");
        assertTrue(policy != null);
        allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 1);
        patterns.clear();
        patterns.add(((Pattern)allowedDN.get(0)).pattern());
        p1 = SigningPolicyParser.getPattern("/C=us/O=National Computational Science Alliance/*");
        assertTrue(patterns.contains(p1.pattern()));        

        // test file with single allows DN without double quotes
        name = BASE + SINGLE_ALLOWED_DN;

        in = loader.getResourceAsStream(name);
        if (in == null) {
            throw new Exception("Unable to load: " + name);
        }

        policy = SigningPolicyParser.getPolicy(new InputStreamReader(in), "/C=US/O=National Computational Science Alliance/OU=Certification Authority");

        assertTrue(policy != null);
        allowedDN.clear();
        allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 1);

        patterns = new Vector(1);
        patterns.add(((Pattern)allowedDN.get(0)).pattern());
        
        p1 = SigningPolicyParser.getPattern("/C=US/O=National Computational Science Alliance/*");
        assertTrue(patterns.contains(p1.pattern()));
    }

    public void testFilesWithTab() throws Exception {

        String name = BASE + TAB_TEST_FILE[0];

	ClassLoader loader = SigningPolicyParserTest.class.getClassLoader();
        InputStream in = loader.getResourceAsStream(name);
        if (in == null) {
            throw new Exception("Unable to load: " + name);
        }

        SigningPolicy policy = SigningPolicyParser.getPolicy(new InputStreamReader(in), "/C=CY/O=CyGrid/O=HPCL/CN=CyGridCA");
        assertTrue(policy != null);
        Vector allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 1);

        name = BASE + TAB_TEST_FILE[1];
        in = loader.getResourceAsStream(name);
        if (in == null) {
            throw new Exception("Unable to load: " + name);
        }
        allowedDN.clear();
        policy = SigningPolicyParser.getPolicy(new InputStreamReader(in), "/C=FR/O=CNRS/CN=CNRS");
        assertTrue(policy != null);
        allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 2);
        
        Vector patterns = new Vector(2);
        patterns.add(((Pattern)allowedDN.get(0)).pattern());
        patterns.add(((Pattern)allowedDN.get(1)).pattern());

        // given the getPattern method is already tested, assuming it
        // works here.
        Pattern p1 = SigningPolicyParser
            .getPattern("/C=FR/O=CNRS/CN=CNRS-Projets");
        assertTrue(patterns.contains(p1.pattern()));
        p1 = SigningPolicyParser.getPattern("/C=FR/O=CNRS/CN=CNRS");
        assertTrue(patterns.contains(p1.pattern()));

        name = BASE + TAB_TEST_FILE[2];

        in = loader.getResourceAsStream(name);
        if (in == null) {
            throw new Exception("Unable to load: " + name);
        }

        allowedDN.clear();
        policy = SigningPolicyParser.getPolicy(new InputStreamReader(in), "/C=IT/O=INFN/CN=INFN Certification Authority");
        assertTrue(policy != null);
        allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 2);

        patterns.clear();
        patterns.add(((Pattern)allowedDN.get(0)).pattern());
        patterns.add(((Pattern)allowedDN.get(1)).pattern());

        // given the getPattern method is already tested, assuming it
        // works here.
        p1 = SigningPolicyParser.getPattern("/C=it/O=INFN/*");
        assertTrue(patterns.contains(p1.pattern()));
        p1 = SigningPolicyParser.getPattern("/C=IT/O=INFN/*");
        assertTrue(patterns.contains(p1.pattern()));
    }

    public void testFileFailure() throws Exception {
        
        boolean exception = false;
        try {
            SigningPolicyParser.getPolicy("foo", "bar");
        } catch (SigningPolicyParserException e) {
            System.out.println(e.getMessage());
            if (e.getException() instanceof FileNotFoundException) {
                exception = true;
            }
        }
        assertTrue(exception);
    }

    public void testParsingFailure() throws Exception {

        // not x509
        String error1 = "access_id_CA      notX509         '/C=US/O=Globus/CN=Globus Certification Authority'\n pos_rights        globus        CA:sign\n cond_subjects     globus       '\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"'";
        
        SigningPolicy policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        assertTrue(policy != null);
        assertTrue(!policy.isPolicyAvailable());

        // not globus
        error1 = "access_id_CA      X509         '/C=US/O=Globus/CN=Globus Certification Authority'\n pos_rights        notglobus        CA:sign\n cond_subjects     globus       '\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"'";
        policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        assertTrue(policy != null);
        assertTrue(!policy.isPolicyAvailable());

        // order of rights matter, atleast one positive right implies
        // allowed DN
        error1 = "access_id_CA      X509         '/C=US/O=Globus/CN=Globus Certification Authority'\n pos_rights        globus        CA:sign\n cond_subjects     globus       '\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"' \n neg_rights        notglobus        some:right";
        policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        assertTrue(policy != null);
        Vector allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 2);

        // incorrect start
        error1 = "X509         '/C=US/O=Globus/CN=Globus Certification Authority'\n pos_rights        notglobus        CA:sign\n cond_subjects     globus       \'\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"\'";
        boolean exception = false;
        try {
            policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        } catch (SigningPolicyParserException exp) {
            if ((exp.getMessage().indexOf("File format is incorrect") != -1) &&
                (exp.getMessage().
                indexOf("Expected line to start with access_id") != -1)) {
                exception = true;
            }
        }
        assertTrue(exception);

        // erroneous quote
        error1 = "access_id_CA X509         '/C=US/O=Globus/CN=Globus Certification Authority\n pos_rights        notglobus        CA:sign\n cond_subjects     globus       \'\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"\'";
        exception = false;
        try {
            policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        } catch (SigningPolicyParserException exp) {
            if ((exp.getMessage().indexOf("Line format is incorrect") != -1) &&
                (exp.getMessage().
                 indexOf("CA DN with space should be enclosed in quotes") != -1)) {
                exception = true;
            }
        }
        assertTrue(exception);

        // neg rights rather than restrictions
        error1 = "access_id_CA      X509         '/C=US/O=Globus/CN=Globus Certification Authority'\n pos_rights        globus        CA:sign\n  neg_rights        notglobus        some:right";
        exception = false;
        try {
            policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        } catch (SigningPolicyParserException exp) {
            if ((exp.getMessage().indexOf("File format is incorrect") != -1) &&
                (exp.getMessage().
                 indexOf("neg_rights cannot be used here") != -1)) {
                exception = true;
            }
        }
        assertTrue(exception);

        // first pos_rights is all that matters
        error1 = "access_id_CA X509         '/C=US/O=Globus/CN=Globus Certification Authority'\n pos_rights        globus        CA:sign\n cond_subjects     globus       '\"/C=us/O=Globus/*\"  \"/C=US/O=Globus/*\"' \n cond_subjects     globus       '\"/C=us/O=Globus/*\"'";
        policy = SigningPolicyParser.getPolicy(new StringReader(error1), "/C=US/O=Globus/CN=Globus Certification Authority");
        assertTrue(policy != null);        
        allowedDN = policy.getPatterns();
        assertTrue(allowedDN != null);
        assertTrue(allowedDN.size() == 2);
    }
}
