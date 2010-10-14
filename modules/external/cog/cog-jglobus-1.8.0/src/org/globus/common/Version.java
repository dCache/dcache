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
package org.globus.common;

/** 
 * This class contains version information of the JGlobus module.
 * The version number is composed as MAJOR.MINOR.PATCH.
 */
public class Version {

    /**
     * COG version. Required to be MAJOR.MINOR.PATCH, with
     * format integer.integer.integer.
     */
    private static String COG_VERSION = "1.8.0";
    
    /** The major release number */
    public static final int MAJOR;
    
    /** The minor release number */
    public static final int MINOR;
    
    /** The patchlevel of the current release */
    public static final int PATCH;
    
    static {

        int firstDot = COG_VERSION.indexOf(".");
        if (firstDot == -1) {
            throw new IllegalArgumentException("COG version required MAJOR."
                                               + "MINOR.PATCH. It is set as "
                                               + COG_VERSION);
        }

        String tmp = COG_VERSION.substring(0, firstDot);
        if (tmp.equals("")) {
            throw new IllegalArgumentException("Number needed after ." +
                                               "COG version required MAJOR"
                                               + ".MINOR.PATCH. It is set "
                                               + "as " + COG_VERSION);
        }
        try {
            MAJOR = Integer.parseInt(tmp);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("COG version numbers are"
                                               + " required to be integers" +
                                               e.getMessage());
        }
        
        int secondDot = COG_VERSION.indexOf(".", firstDot + 1);
        if (secondDot == -1) {
            PATCH = 0;
            tmp = COG_VERSION.substring(firstDot + 1, COG_VERSION.length());
            if (tmp.equals("")) {
                throw new IllegalArgumentException("Number needed after . " +
                                                   "COG version required " +
                                                   "MAJOR.MINOR.PATCH. It "
                                                   + "is set as " + 
                                                   COG_VERSION);
            }
            try {                    
                MINOR = Integer.parseInt(tmp);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("COG version numbers are"
                                                   + " required to be numbers."
                                                   + e.getMessage());
            }
        } else {
            try {
                MINOR = Integer.parseInt(COG_VERSION.substring(firstDot + 1, 
                                                               secondDot));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("COG version numbers are"
                                                   + " required to be numbers."
                                                   + e.getMessage());
            }

            try {
                PATCH = Integer.parseInt(COG_VERSION
                                         .substring(secondDot + 1, 
                                                    COG_VERSION.length()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("COG version numbers are"
                                                   + " required to be "
                                                   + "numbers." +
                                                   e.getMessage());
            }
        }
    }

    /** 
     * Returns the current version as string in the form MAJOR.MINOR.PATCH.
     */
    public static String getVersion() {
        return getMajor() + "." + getMinor() + "." + getPatch();
    }
    
    /**
     * Returns the major release number.
     * 
     * @return the major release
     */
    public static int getMajor() {
        return MAJOR;
    }
    
    /**
     * Returns the minor release number.
     * 
     * @return the minor release number
     */
    public static int getMinor() {
        return MINOR;
    }
    
    /** 
     * Returns the patch level.
     * 
     * @return the patch level
     */
    public static int getPatch() {
        return PATCH;
    }
    
    /** 
     * Returns the version for the Java CoG Kit as a readble string.
     * 
     * @param args 
     */
    public static void main(String [] args) {
        System.out.println("Java CoG version: " + getVersion());
    }
    
}
