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

import org.globus.util.I18n;

import java.util.Vector;
import java.util.HashMap;
import java.util.StringTokenizer;

import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Signing policy BCNF grammar as implemented here: (based on C implementation)
 * 
 * eacl ::=  {eacl_entry}
 * eacl_entry ::= {access_identity} pos_rights {restriction}  
 * {pos_rights {restriction}} | {access_identity} neg_rights  
 * access_identity ::= access_identity_type  def_authority  value  \n
 * access_identity_type ::= "access_id_HOST"  |  
 *                          "access_id_USER"  | 
 *                          "access_id_GROUP" |
 *                          "access_id_CA"    | 
 *                          "access_id_APPLICATION" | 
 *                          "access_id_ANYBODY" 
 * pos_rights ::=  "pos_rights" def_authority value 
 *                 {"pos_rights" def_authority value} 
 * neg_rights ::= "neg_rights" def_authority value 
 *                {"neg_rights" def_authority value} 
 * restriction ::= condition_type  def_authority  value  \n
 * condition_type ::= alphanumeric_string 
 * def_authority ::= alphanumeric_string 
 * value ::= alphanumeric_string
 *
 * This class take a signing policy file as input and parses it to
 * extract the policy that is enforced. Only the following policy is
 * enforced:
 * access_id_CA with defining authority as X509 with CA DN as
 * value. Any positive rights following it with globus as defining
 * authority and value CA:sign. Lastly, restriction "cond_subjects"
 * with globus as defining authority and the DNs the CA is authorized
 * to sign. restrictions are assumed to start with cond_. Order of
 * rights matter, so the first occurance of CA:Sign with allowedDNs is
 * used and rest of the policy is ignored.
 *
 * For a given signing policy file, only policy with the particular
 * CA's DN is parsed.
 *
 * subject names may include the following wildcard characters:
 *   *    Matches zero or any number of characters.
 *   ?    Matches any single character.
 *
 * All subject names should be in Globus format, with slashes and
 * should NOT be revered.
 *
 * The allowed DN patterns are returned as a vector of
 * java.util.regexp.Pattern. The BCNF grammar that uses wildcard (*)
 * and single character (?) are replaced with the regexp grammar
 * needed by the Pattern class.
 */
public class SigningPolicyParser {
   
    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     SigningPolicyParser.class.getClassLoader());

    private static Log logger =
        LogFactory.getLog(SigningPolicyParser.class.getName());

    public static String ACCESS_ID_PREFIX = "access_id_";
    public static String ACCESS_ID_CA = "access_id_CA";
    
    public static String DEF_AUTH_X509 = "X509";
    public static String DEF_AUTH_GLOBUS = "globus";
    
    public static String POS_RIGHTS = "pos_rights";
    public static String NEG_RIGHTS = "neg_rights";

    public static String CONDITION_PREFIX = "cond_";
    public static String CONDITION_SUBJECT = "cond_subjects";    

    public static String VALUE_CA_SIGN = "CA:sign";

    public static String SINGLE_CHAR = "?";
    public static String WILDCARD= "*";

    public static String SINGLE_PATTERN = "[\\p{Print}\\p{Blank}]";
    public static String WILDCARD_PATTERN = SINGLE_PATTERN + "*";

    private static final char[] EMAIL_KEYWORD_1 = { 'E', '=' };
    private static final char[] EMAIL_KEYWORD_2 = { 'E', 'm', 'a', 'i', 'l',
                                                    '=' };
    private static final String EMAIL_KEYWORD = "emailAddress=";

    /**
     * Parses the file to extract signing policy defined for CA with
     * the specified DN. If the policy file does not exist, a
     * SigningPolicy object with only CA DN is created. If policy path
     * exists, but no relevant policy exisit, SigningPolicy object with
     * CA DN and file path is created.
     *
     * @param fileName
     *        Name of the signing policy file
     * @param requiredCaDN
     *        The CA subject name for which policy is extracted
     * @return 
     *        SigningPolicy object that contains the information. If
     *        no policy is found, SigningPolicy object with only the
     *        CA DN is returned.
     * @throws SigningPolicyParserException
     *        Any errors with parsing the signing policy file.
     */
    public static SigningPolicy getPolicy(String fileName, 
                                          String requiredCaDN) 
        throws SigningPolicyParserException {
        
        if ((fileName == null) || (fileName.trim().equals(""))) {
            throw new IllegalArgumentException();
        }
        
        logger.debug("Signing policy file name " + fileName + " with CA DN "
                     + requiredCaDN);

        FileReader fileReader = null;
        
        try {
            fileReader = new FileReader(fileName);
        } catch (FileNotFoundException exp) {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (Exception e) {
                }
            }
            throw new SigningPolicyParserException(exp.getMessage(), exp);
        }

        SigningPolicy policy = getPolicy(fileReader, requiredCaDN);
        policy.setFileName(fileName);
        logger.debug("Policy file parsing completed, policy is " + 
                     (policy == null));
        return policy;
    }

    /**
     * Parses input stream to extract signing policy defined for CA with
     * the specified DN.
     *
     * @param reader
     *        Reader to any input stream to get the signing policy information.
     * @param requiredCaDN
     *        The CA subject name for which policy is extracted
     * @return 
     *        SigningPolicy object that contains the information. If
     *        no policy is found, SigningPolicy object with only the
     *        CA DN is returned.
     * @throws SigningPolicyParserException
     *        Any errors with parsing the signing policy.
     */
    public static SigningPolicy getPolicy(Reader reader, 
                                          String requiredCaDN) 
        throws SigningPolicyParserException {

        BufferedReader bufferedReader = new BufferedReader(reader);
        try {
            String line = bufferedReader.readLine();
            
            while (line != null) {
                
                line = line.trim();
                
                // read line until some line that needs to be parsed.
                if (!isValidLine(line)) {
                    line = bufferedReader.readLine();
                    continue;
                }
                
                logger.trace("Line to parse: " + line);
                
                String caDN = null;
                if (line.startsWith(ACCESS_ID_PREFIX)) {
                    
                    logger.trace("Check if it is CA and get the DN " + line);
                    
                    if (line.startsWith(ACCESS_ID_CA)) {
                        caDN = getCA(line.substring(ACCESS_ID_CA.length(), 
                                                    line.length()));
                        logger.trace("CA DN is " + caDN);
                    }
                    
                    boolean usefulEntry = false;
                    if ((caDN != null) && equalsDN(caDN, requiredCaDN)) {
                        usefulEntry = true;
                        logger.trace("CA DN match " + caDN);
                    }
                    
                    Boolean posNegRights = null;                
                    // check for neg or pos rights with restrictions
                    while ((line = bufferedReader.readLine()) != null) {
                        
                        if (!isValidLine(line)) {
                            continue;
                        }
                        
                        line = line.trim();
                        
                        logger.trace("Line is " + line);
                        
                        if (line.startsWith(POS_RIGHTS)) {
                            if (Boolean.FALSE.equals(posNegRights)) {
                                String err = 
                                    i18n.getMessage("invalidPosRights", line);
                                throw new SigningPolicyParserException(err);
                            }
                            posNegRights = Boolean.TRUE;
                            if (usefulEntry) {
                                logger.trace("Parse pos_rights here");
                                int startIndex = POS_RIGHTS.length();
                                int endIndex = line.length();
                                // if it is not CASignRight, then
                                // usefulentry will be false. Otherwise
                                // other restrictions will be useful.
                                usefulEntry = 
                                    isCASignRight(line.
                                                  substring(startIndex,
                                                            endIndex));
                            }
                            continue;
                        }
                        
                        if (line.startsWith(NEG_RIGHTS)) {
                            
                            if (Boolean.TRUE.equals(posNegRights)) {
                                String err = 
                                    i18n.getMessage("invalidNegRights", line);
                                throw new SigningPolicyParserException(err);
                            }
                            posNegRights = Boolean.FALSE;
                            logger.trace("Ignore neg_rights");
                            continue;
                        }
                        
                        if (line.startsWith(CONDITION_PREFIX)) {
                            
                            if (!Boolean.TRUE.equals(posNegRights)) {
                                String err = i18n.
                                    getMessage("invalidRestrictions", line);
                                throw new SigningPolicyParserException(err);
                            }
                            
                            if (usefulEntry) {
                                if (line.startsWith(CONDITION_SUBJECT)) {
                                    logger.trace("Read in subject condition.");
                                    int startIndex = 
                                        CONDITION_SUBJECT.length();
                                    int endIndex = line.length();
                                    Vector allowedDNs = 
                                        getAllowedDNs(line
                                                      .substring(startIndex,
                                                                 endIndex), 
                                                      line);
                                    SigningPolicy policy =
                                        new SigningPolicy(requiredCaDN,
                                                          allowedDNs);
                                    return policy;
                                }
                            }
                            continue;
                        }
                        
                        if (line.startsWith(ACCESS_ID_PREFIX)) {
                            break;
                        }
                        
                        // no valid start with 
                        String err = i18n.getMessage("invalidLine", line);
                        throw new SigningPolicyParserException(err);
                    }
                } else {
                    // entry needs to start with that.
                    String err = i18n.getMessage("invalidAccessId", line);
                    throw new SigningPolicyParserException(err);
                }

            }
        } catch (IOException exp) {
            throw new SigningPolicyParserException("", exp);
        } finally {
            try {
                bufferedReader.close();
            } catch (Exception exp) {
            }
        }
        return new SigningPolicy(requiredCaDN);
    }

    private static boolean isValidLine(String line) 
        throws SigningPolicyParserException {

        line = line.trim();

        // if line is empty or comment character, skip it.
        if (line.equals("") || line.startsWith("#")) {
            return false;
        }
        
        // Validate that there are atleast three tokens on the line
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() < 3) {
            String err = i18n.getMessage("invalidTokens", line);
            throw new SigningPolicyParserException(err);
        }
        
        return true;
    }

    private static Vector getAllowedDNs(String line, String lineForErr) 
        throws SigningPolicyParserException {
        
        line = line.trim();

        int index = findIndex(line);

        if (index == -1) {
            String err = 
                i18n.getMessage("invalidTokens", line);
            throw new SigningPolicyParserException(err);
        }

        String defAuth = line.substring(0, index);

        if (DEF_AUTH_GLOBUS.equals(defAuth)) {

            String value = line.substring(index + 1, line.length());
            value = value.trim();

            int startIndex = 0;
            int endIndex = value.length();
            if (value.charAt(startIndex) == '\'') {
                startIndex++;
                int endOfDNIndex = value.indexOf('\'', startIndex);
                if (endOfDNIndex == -1) {
                    String err = i18n.getMessage("invalidSubjects", 
                                                 lineForErr);
                    throw new SigningPolicyParserException(err);
                }
                endIndex = endOfDNIndex;
            }
            
            value = value.substring(startIndex, endIndex);
            value = value.trim();

            if (value.equals("")) {
                String err = i18n.getMessage("emptySubjects", lineForErr);
                throw new SigningPolicyParserException(err);
            }

            Vector vector = new Vector();

            startIndex = 0;
            endIndex = value.length();
            if (value.indexOf("\"") == -1) {
                vector.add(getPattern(value)); 
            } else {
                while (startIndex < endIndex) {
                    
                    int quot1 = value.indexOf("\"", startIndex);
                    int quot2 = value.indexOf("\"", quot1 + 1);
                    if (quot2 == -1) {
                        String err = i18n.getMessage("unmatchedQuotes", 
                                                     lineForErr);
                        throw new SigningPolicyParserException(err);
                    }
                    String token = value.substring(quot1 + 1, quot2);
                    vector.add(getPattern(token));
                    startIndex = quot2 + 1;
                }
            }

            return vector;
        }
        return null;
    }

    private static boolean isCASignRight(String line) 
        throws SigningPolicyParserException {

        line = line.trim();

        int index =  findIndex(line);

        if (index == -1) {
            String err = 
                i18n.getMessage("invalidTokens", line);
            throw new SigningPolicyParserException(err);
        }
        
        String def_auth = line.substring(0, index);
        if (DEF_AUTH_GLOBUS.equals(def_auth)) {
            line = line.substring(index + 1, line.length());
            line = line.trim();
            // check if it is CA:Sign
            String value = line.substring(0, line.length());
            if (VALUE_CA_SIGN.equals(value)) {
                return true;
            }
        }

        return false;
    }

    private static String getCA(String inputLine) 
        throws SigningPolicyParserException {

        String line = inputLine.trim();
        
        int index = findIndex(line);

        if (index == -1) {
            String err = 
                i18n.getMessage("invalidTokens", line);
            throw new SigningPolicyParserException(err);
        }
        
        String defAuth = line.substring(0, index);
        
        if (DEF_AUTH_X509.equals(defAuth)) {

            line = line.substring(index + 1, line.length());
            line = line.trim();

            String dnString = line.substring(0, line.length());

            String caDN = null;
            // find CA DN
            int caDNLocation = 0;
            if (line.charAt(caDNLocation) == '\'') {
                caDNLocation++;
                int endofDNIndex = line.indexOf('\'', caDNLocation + 1);
                if (endofDNIndex == -1) {
                    String err = i18n.getMessage("invalidCaDN", inputLine);
                    throw new SigningPolicyParserException(err);
                }
                caDN = line.substring(caDNLocation, endofDNIndex);
            } else {
                caDN = line.substring(caDNLocation, line.length() - 1);
            }
            caDN = caDN.trim();
            return caDN;
        }        
        return null;
    }

    /**
     * Method that takes a pattern string as described in the signing
     * policy file with * for zero or many characters and ? for single
     * character, and converts it into java.util.regexp.Pattern
     * object. This requires replacing the wildcard characters with
     * equivalent expression in regexp grammar.
     *
     * @param patternStr
     *        Pattern string as described in the signing policy file
     *        with for zero or many characters and ? for single
     *        character
     * @return
     *        Pattern object with the expression equivalent to patternStr.
     */
    public static Pattern getPattern(String patternStr) {

        if (patternStr == null) {
            throw new IllegalArgumentException();
        }

        int startIndex = 0;
        int endIndex = patternStr.length();
        StringBuffer buffer = new StringBuffer("");
        while (startIndex < endIndex) {
            int star = patternStr.indexOf(WILDCARD, startIndex);
            if (star == -1) {
                star = endIndex;
                String preStr = patternStr.substring(startIndex, star);
                buffer = buffer.append(preStr);
            } else {
                String preStr = patternStr.substring(startIndex, star);
                buffer = buffer.append(preStr).append(WILDCARD_PATTERN);
            }
            startIndex = star + 1;
        }

        patternStr = buffer.toString();
        
        startIndex = 0;
        endIndex = patternStr.length();
        buffer = new StringBuffer("");
        while (startIndex < endIndex) {
            int qMark = patternStr.indexOf(SINGLE_CHAR, startIndex);
            if (qMark == -1) {
                qMark = endIndex;
                String preStr = patternStr.substring(startIndex, qMark);
                buffer = buffer.append(preStr);
            } else {
                String preStr = patternStr.substring(startIndex, qMark);
                buffer = buffer.append(preStr).append(SINGLE_PATTERN);
            }
            startIndex = qMark + 1;
        }
        patternStr = buffer.toString();
        
        logger.debug("String with replaced pattern is " + patternStr);
        
        return Pattern.compile(patternStr, Pattern.CASE_INSENSITIVE);
    }

    // find first space or tab as separator.
    private static int findIndex(String line) {

        int index = -1;

        if (line == null) {
            return index;
        }

        line = line.trim();
        int spaceIndex = line.indexOf(" ");
        int tabIndex = line.indexOf("\t");

        if (spaceIndex != -1) {
            if (tabIndex != -1) {
                if (spaceIndex < tabIndex) {
                    index = spaceIndex;
                } else {
                    index = tabIndex;
                }
            } else {
                index = spaceIndex;
            }
        } else {
            index = tabIndex;
        }
        return index;
    }

    public static boolean equalsDN(String dn1, String dn2)
    {
        if (dn1 == null && dn2 == null) {
            return true;
        }
        if (dn1 == null || dn2 == null) {
            return false;
        }
        return normalizeDN(dn1).equals(normalizeDN(dn2));
    }

    private static boolean keyWordPresent(char[] args, int startIndex,
                                          char[] keyword) {

        if (startIndex + keyword.length > args.length) {
            return false;
        }

        for (int i=0, j=startIndex; i<keyword.length; i++, j++) {
            if (args[j] != keyword[i]) {
                return false;
            }
        }
        return true;
    }

    public static String normalizeDN(String globusID) {

        if (globusID == null) {
            return null;
        }

        char[] globusIdChars = globusID.toCharArray();

        StringBuffer normalizedDN = new StringBuffer(globusIdChars.length);

        for (int i=0; i<globusIdChars.length; i++) {

            if (globusIdChars[i] == '/') {

                normalizedDN.append("/");

                if (keyWordPresent(globusIdChars, i+1, EMAIL_KEYWORD_1)) {
                    normalizedDN.append(EMAIL_KEYWORD);
                    i = i + EMAIL_KEYWORD_1.length;
                } else if (keyWordPresent(globusIdChars, i+1, EMAIL_KEYWORD_2)) {
                    normalizedDN.append(EMAIL_KEYWORD);
                    i = i + EMAIL_KEYWORD_2.length;
                }
            } else {
                normalizedDN.append(globusIdChars[i]);
            }
        }

        return normalizedDN.toString();
    }

}
