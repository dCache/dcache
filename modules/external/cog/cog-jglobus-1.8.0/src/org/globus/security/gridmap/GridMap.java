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
package org.globus.security.gridmap;

import java.util.Map;
import java.util.Vector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;

import org.globus.util.I18n;

import org.globus.util.QuotedStringTokenizer;
import org.globus.util.ConfigUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GridMap implements Serializable {

    private static I18n i18n =
        I18n.getI18n("org.globus.security.gridmap.errors",
                     GridMap.class.getClassLoader());

    private static Log logger =
	LogFactory.getLog(GridMap.class.getName());

    private static final String COMMENT_CHARS = "#";
    // keywords that need to be replaced
    private static final char[] EMAIL_KEYWORD_1 = { 'e', '=' };
    private static final char[] EMAIL_KEYWORD_2 = { 'e', 'm', 'a', 'i', 'l', 
                                                    '=' };
    private static final char[] UID_KEYWORD = { 'u', 'i', 'd', '=' };
    // Length of key words that need to be replaced
    private static final int EMAIL_KEYWORD_1_L = 2;
    private static final int EMAIL_KEYWORD_2_L = 6;
    private static final int UID_KEYWORD_L = 4;
    // Keywords to be replaced with.
    private static final String EMAIL_KEYWORD = "emailaddress=";
    private static final String USERID_KEYWORD = "userid=";

    protected Map map;

    // the file the grim map was loaded from
    private File file;
    // last time the file was modified
    private long lastModified;
    // log or throw exception on bad entries 
    private boolean ignoreErrors = false;

    /**
     * Sets whether errors in the gridmap file
     * should be ignored. Errors are not ignored by default.
     * 
     * @param ignoreErrors if true, errors in the gridmap file
     *        will be ignored (warnings will be logged). If false,
     *        an exception will be raised on errors. 
     */
    public void setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }
    
    /**
     * Returns whether errors in the gridmap file are
     * ignored.
     *
     * @return true if errors in the gridmap file are ignored.
     *         False, otherwise.
     */
    public boolean getIgnoreErrors() {
        return this.ignoreErrors;
    }

    /**
     * Returns the absolute path anme of file used to load gridmap
     * either using the {@link #load(File) load} or {@link
     * #load(String) load} functions. If no file was used, returns
     * null.
     *
     * @return absolute file path, if gridmap was loaded from
     *         file. Null otherwise.
     */
    public String getFileName() {
        
        if (this.file == null) {
            return null;
        }

        return this.file.getAbsolutePath();
    }

    /**
     * Loads grid map definition from a given file. 
     * 
     * @param file the grid map file
     * @exception IOException in case of I/O error or
     *         when there are parsing errors in the file (only when errors 
     *         are not ignored).
     * @return <tt>true</tt> if the file was parsed and loaded successfully.
     *         <tt>False</tt> otherwise.
     */
    public boolean load(String file)
        throws IOException {
        return load(new File(file));
    }

    /**
     * Loads grid map definition from a given file. 
     * 
     * @param file the grid map file
     * @exception IOException in case of I/O error or
     *         when there are parsing errors in the file (only when errors 
     *         are not ignored).
     * @return <tt>true</tt> if the file was parsed and loaded successfully.
     *         <tt>False</tt> otherwise.
     */
    public boolean load(File file)
        throws IOException {
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            this.file = file;
            this.lastModified = file.lastModified();
            return load(in);
        } finally {
            if (in != null) {
                try { in.close(); } catch(Exception e) {}
            }
        }
    }

    /**
     * Reloads the gridmap from a file only if the gridmap
     * was initially loaded using the {@link #load(File) 
     * load} or {@link #load(String) load} functions.
     * The file will only be reloaded if it has changed
     * since the last time. 
     *
     * @exception IOException in case of I/O error or
     *         when there are parsing errors in the file (only when errors 
     *         are not ignored).
     * @return <tt>true</tt> if the file was parsed and refreshed successfully.
     *         <tt>False</tt> otherwise.
     */
    public boolean refresh() 
        throws IOException {
        if (this.file != null &&
            this.file.lastModified() != this.lastModified) {
            return load(this.file);
        } else {
            return true;
        }
    }
    
    /**
     * Loads grid map file definition from a given input stream. The input 
     * stream is not closed in case of an error.
     *
     * @param input the input stream that contains the gridmap 
     *        definitions.
     * @exception IOException in case of I/O error or
     *         when there are parsing errors in the input (only when errors 
     *         are not ignored).
     * @return <tt>true</tt> if the input was parsed successfully.
     *         <tt>False</tt> otherwise.
     */
    public boolean load(InputStream input) 
        throws IOException {
        boolean success = true;

        BufferedReader reader = 
            new BufferedReader(new InputStreamReader(input));

        Map localMap = new HashMap();
        GridMapEntry entry;
        QuotedStringTokenizer tokenizer;
        StringTokenizer idTokenizer;
        String line;
        while( (line = reader.readLine()) != null) {
            line = line.trim();
            if ( (line.length() == 0) ||
                 ( COMMENT_CHARS.indexOf(line.charAt(0)) != -1) ) {
                continue;
            }
            
            tokenizer = new QuotedStringTokenizer(line);

            String globusID = null;

            if (tokenizer.hasMoreTokens()) {
                globusID = tokenizer.nextToken();
            } else {
                if (this.ignoreErrors) {
                    success = false;
                    logger.warn("Globus ID missing: " + line);
                    continue;
                } else {
                    throw new IOException(i18n.getMessage("globusIdErr", 
                                                          line));
                }
            }

            String userIDs = null;
            
            if (tokenizer.hasMoreTokens()) {
                userIDs = tokenizer.nextToken();
            } else {
                if (this.ignoreErrors) {
                    success = false;
                    logger.warn("User ID mapping missing: " + line);
                    continue;
                } else {
                    throw new IOException(i18n.getMessage("userIdErr", line));
                }
            }

            idTokenizer = new StringTokenizer(userIDs, ",");
            String [] ids = new String [ idTokenizer.countTokens() ];
            int i = 0;
            while(idTokenizer.hasMoreTokens()) {
                ids[i++] = idTokenizer.nextToken();
            }
            
            String normalizedDN = normalizeDN(globusID);
            entry = (GridMapEntry)localMap.get(normalizedDN);
            if (entry == null) {
                entry = new GridMapEntry();
                entry.setGlobusID(globusID);
                entry.setUserIDs(ids);
                localMap.put(normalizedDN, entry);
            } else {
                entry.addUserIDs(ids);
            }
        }
        
        this.map = localMap;

        return success;
    }

    /**
     * Returns first local user name mapped to the specified 
     * globusID.
     * 
     * @param globusID globusID
     * @return local user name for the specified globusID.
     *         Null if the globusID is not mapped
     *         to a local user name. 
     */
    public String getUserID(String globusID) {
        String [] ids = getUserIDs(globusID);
        if (ids != null && ids.length > 0) {
            return ids[0];
        } else {
            return null;
        }
    }
    
    /**
     * Returns local user names mapped to the specified
     * globusID.
     *
     * @param globusID globusID
     * @return array of local user names for the specified globusID.
     *         Null if the globusID is not mapped
     *         to any local user name.
     */
    public String[] getUserIDs(String globusID) {
        if (globusID == null) {
            throw new IllegalArgumentException(i18n
                                               .getMessage("globusIdNull"));
        }
        
        if (this.map == null) {
            return null;
        }
            
        GridMapEntry entry = (GridMapEntry)this.map.get(normalizeDN(globusID));
        return (entry == null) ? null : entry.getUserIDs();
    }

    /**
     * Checks if a given globus ID is associated with given
     * local user account.
     *
     * @param globusID globus ID
     * @param userID userID
     * @return true if globus ID is associated with given local
     *         user account, false, otherwise.
     */
    public boolean checkUser(String globusID, String userID) {
        if (globusID == null) {
            throw new IllegalArgumentException(i18n.getMessage("glousIdNull"));
        }
        if (userID == null) {
            throw new IllegalArgumentException(i18n.getMessage("userIdNull"));
        }
        
        if (this.map == null) {
            return false;
        }

        GridMapEntry entry = (GridMapEntry)this.map.get(normalizeDN(globusID));
        return (entry == null) ? false : entry.containsUserID(userID);
    }

    /**
     * Returns globus ID associated with the
     * specified local user name.
     *
     * @param userID local user name
     * @return associated globus ID, null
     *         if there is not any.
     */
    public String getGlobusID(String userID) {
        if (userID == null) {
            throw new IllegalArgumentException(i18n.getMessage("userIdNull"));
        }
        
        if (this.map == null) {
            return null;
        }

        Iterator iter = this.map.entrySet().iterator();
        Map.Entry mapEntry;
        GridMapEntry entry;
        while(iter.hasNext()) {
            mapEntry = (Map.Entry)iter.next();
            entry = (GridMapEntry)mapEntry.getValue();
            if (entry.containsUserID(userID)) {
                return entry.getGlobusID();
            }
        }
        return null;
    }

    /**
     * Returns all globus IDs associated with the
     * specified local user name.
     *
     * @param userID local user name
     * @return associated globus ID, null
     *         if there is not any.
     */
    public String[] getAllGlobusID(String userID) {
        if (userID == null) {
            throw new IllegalArgumentException(i18n.getMessage("userIdNull"));
        }
        
        if (this.map == null) {
            return null;
        }

        Vector v = new Vector();

        Iterator iter = this.map.entrySet().iterator();
        Map.Entry mapEntry;
        GridMapEntry entry;
        while(iter.hasNext()) {
            mapEntry = (Map.Entry)iter.next();
            entry = (GridMapEntry)mapEntry.getValue();
            if (entry.containsUserID(userID)) {
                v.add(entry.getGlobusID());
           }
        }

        // create array of strings and add values back in
        if(v.size() == 0) {
            return null;
        }

        String idS[] = new String[v.size()];
        for(int ctr = 0; ctr < v.size(); ctr++) {
            idS[ctr] = (String) v.elementAt(ctr);
        }

        return idS;
    }

    public void map(String globusID, String userID) {
        if (globusID == null) {
            throw new IllegalArgumentException(i18n
                                               .getMessage("globusIdNull"));
        }
        if (userID == null) {
            throw new IllegalArgumentException(i18n.getMessage("userIdNull"));
        }
        
        if (this.map == null) {
            this.map = new HashMap();
        }
        
        String normalizedDN = normalizeDN(globusID);

        GridMapEntry entry = (GridMapEntry)this.map.get(normalizedDN);
        if (entry == null) {
            entry = new GridMapEntry();
            entry.setGlobusID(globusID);
            entry.setUserIDs(new String [] {userID});
            this.map.put(normalizedDN, entry);
        } else {
            entry.addUserID(userID);
        }
    }

    static class GridMapEntry implements Serializable {
        String globusID;
        String[] userIDs;

        public String getFirstUserID() {
            return userIDs[0];
        }

        public String[] getUserIDs() {
            return userIDs;
        }

        public String getGlobusID() {
            return globusID;
        }

        public void setGlobusID(String globusID) {
            this.globusID = globusID;
        }
        
        public void setUserIDs(String [] userIDs) {
            this.userIDs = userIDs;
        }

        public boolean containsUserID(String userID) {
            if (userID == null) {
                return false;
            }
            for (int i=0;i<userIDs.length;i++) {
                if (userIDs[i].equalsIgnoreCase(userID)) {
                    return true;
                }
            }
            return false;
        }

        public void addUserID(String userID) {
            if (containsUserID(userID)) return;
            String [] ids = new String[ userIDs.length + 1 ];
            System.arraycopy(userIDs, 0, ids, 0, userIDs.length);
            ids[userIDs.length] = userID;
            userIDs = ids;
        }
        
        public void addUserIDs(String [] userIDs) {
            for (int i=0;i<userIDs.length;i++) {
                addUserID(userIDs[i]);
            }
        }

    }

    private static boolean keyWordPresent(char[] args, int startIndex,
                                          char[] keyword, int length) {
        
        if (startIndex + length > args.length) {
            return false;
        }

        int j=startIndex;
        for (int i=0; i<length; i++) {
            if (args[j] != keyword[i]) {
                return false;
            }
            j++;
        }
        return true;
    }

    public static String normalizeDN(String globusID) {

        if (globusID == null) {
            return null;
        }
        
        globusID = globusID.toLowerCase();
        char[] globusIdChars = globusID.toCharArray();
        
        StringBuffer normalizedDN = new StringBuffer();
        
        int i=0;

        while (i<globusIdChars.length) {
            
            if (globusIdChars[i] == '/') {
                
                normalizedDN.append("/");            

                if (keyWordPresent(globusIdChars, i+1, EMAIL_KEYWORD_1, 
                                   EMAIL_KEYWORD_1_L)) {
                    normalizedDN.append(EMAIL_KEYWORD);
                    i = i + EMAIL_KEYWORD_1_L;
                } else if (keyWordPresent(globusIdChars, i+1, EMAIL_KEYWORD_2, 
                                          EMAIL_KEYWORD_2_L)) {
                    normalizedDN.append(EMAIL_KEYWORD);
                    i = i + EMAIL_KEYWORD_2_L;
                } else if (keyWordPresent(globusIdChars, i+1, UID_KEYWORD, 
                                          UID_KEYWORD_L)) {
                    normalizedDN.append(USERID_KEYWORD);
                    i = i + UID_KEYWORD_L;
                } 
                i++;
            } else {
                normalizedDN.append(globusIdChars[i]);
                i++;
            }
        }
        
        return normalizedDN.toString();
    }
    
    /* 
     * Determine the location of the gridmap file based on the standard
     * Globus logic:
     *   1. If the GRIDMAP system property is set, return it.
     *   2. If the user is not root, return $HOME/.gridmap
     *   3. Otherwise, return /etc/grid-security/grid-mapfile
     */
    public static String getDefaultGridMapLocation() {
        String location = System.getProperty("GRIDMAP");
        if (location == null || location.length() == 0) {
            String uid = null;
            try {
                uid = ConfigUtil.getUID();
            } catch (IOException e) {
                // ignore it
            }
            if (uid != null && uid.equals("0")) {
                location = "/etc/grid-security/grid-mapfile";
            } else {
                location = System.getProperty("user.home") + "/.gridmap";
            }
        }
        return location;
    }
}
