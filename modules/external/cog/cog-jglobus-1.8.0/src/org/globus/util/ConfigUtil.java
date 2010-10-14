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
package org.globus.util;

import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ConfigUtil {

    public static final int UNDEFINED_OS = -1;
    public static final int WINDOWS_OS   = 0;
    public static final int UNIX_OS      = 1;
    public static final int MAC_OS       = 2;
    public static final int OTHER_OS     = 3;
  
    private static int osType = UNDEFINED_OS;

    private static final String PROXY_NAME = "x509up_u";

    private static final String SOLARIS_ID_EXEC =
        "/usr/xpg4/bin/id";
        
    public static String globus_dir = null;
    
    static {
        globus_dir = System.getProperty("user.home") + 
            File.separator + 
            ".globus" +
            File.separator;
    }
    
    /** Returns default PKCS11 library name */
    public static String discoverPKCS11LibName() {
        return "dspkcs"; // use the ibutton library as the default for now
    }
    
    /** Returns default location of user cert file */
    public static String discoverUserCertLocation() {
        String location = null;
        location = globus_dir + "usercert.pem";
        return location;
    }
    
    /** Returns default location of user key file */
    public static String discoverUserKeyLocation() {
        String location = null;
        location = globus_dir + "userkey.pem";
        return location;
    }
    
    /**
     * Tries to discover user proxy location.
     * If a UID system property is set, and running on a Unix machine it
     * returns /tmp/x509up_u${UID}. If any other machine then Unix, it returns
     * ${tempdir}/x509up_u${UID}, where tempdir is a platform-specific
     * temporary directory as indicated by the java.io.tmpdir system property.
     * If a UID system property is not set, the username will be used instead
     * of the UID. That is, it returns ${tempdir}/x509up_u_${username}
     */
    public static String discoverProxyLocation() {

        String dir = null;

        if (getOS() == UNIX_OS) {
            dir = "/tmp/";
        } else {
            String tmpDir = System.getProperty("java.io.tmpdir");
            dir = (tmpDir == null) ? globus_dir : tmpDir;
        }
        
        String uid = System.getProperty("UID");

        if (uid != null) {
            return getLocation(dir, PROXY_NAME + uid);
        } else if (getOS() == UNIX_OS) {
            try {
                return getLocation(dir, PROXY_NAME + getUID());
            } catch (IOException e) {
            }
        }
        
        /* If all else fails use username */
        String suffix = System.getProperty("user.name");
        if (suffix != null) {
            suffix = suffix.toLowerCase();
        } else {
            suffix = "nousername";
        }

        return getLocation(dir, PROXY_NAME + "_" + suffix);
    }

    private static String getLocation(String dir, String file) {
        File f = new File(dir, file);
        return f.getAbsolutePath();
    }

    /**
     * Returns the user id. The user id is obtained by executing 'id -u'
     * external program. 
     * <BR><BR><B>Note: </B><I>
     * Under some circumstances, this function executes an external program; 
     * thus, its behavior is influenced by environment variables such as the
     * caller's PATH and the environment variables that control dynamic 
     * loading.  Care should be used if calling this function from a program 
     * that will be run as a Unix setuid program, or in any other manner in
     * which the owner of the Unix process does not completely control its
     * runtime environment. 
     * </I>
     *
     * @throws IOException if unable to determine the user id.
     * @return the user id
     */
    public static String getUID() throws IOException {
        String exec = "id";
        String osname = System.getProperty("os.name");
        if (osname != null) {
            osname = osname.toLowerCase();
            if ((osname.indexOf("solaris") != -1) ||
                (osname.indexOf("sunos") != -1)) {
                if ((new File(SOLARIS_ID_EXEC).exists())) {
                    exec = SOLARIS_ID_EXEC;
                }
            } else if (osname.indexOf("windows") != -1) {
                throw new IOException("Unable to determine the user id");
            }
        }

        Runtime runTime = Runtime.getRuntime();
        Process process = null;
        BufferedReader buffInReader = null;
        String s = null;
        StringBuffer output = new StringBuffer();
        int exitValue = -1;

        try {
            process = runTime.exec(exec + " -u");
            buffInReader = new BufferedReader
                ( new InputStreamReader(process.getInputStream()) ); 
            while ((s = buffInReader.readLine()) != null) {
                output.append(s);
            }
            exitValue = process.waitFor();
        } catch (Exception e) {
            throw new IOException("Unable to execute 'id -u'");
        } finally {
            if (buffInReader != null) {
                try { 
                    buffInReader.close();
                } catch (IOException e) {}
            }
            if (process != null) {
                try { 
                    process.getErrorStream().close(); 
                } catch (IOException e) {}
                try { 
                    process.getOutputStream().close(); 
                } catch (IOException e) {}
            }
        }
        if (exitValue != 0) {
            throw new IOException("Unable to perform 'id -u'");
        }
        return output.toString().trim();
    } 

    /**
     * Discovers location of CA certificates directory.
     * First the ${user.home}/.globus/certificates directory is checked.
     * If the directory does not exist, and on a Unix machine, the
     * /etc/grid-security/certificates directory is checked next.
     * If that directory does not exist and GLOBUS_LOCATION 
     * system property is set then the ${GLOBUS_LOCATION}/share/certificates
     * directory is checked. Otherwise, null is returned. 
     * This indicates that the certificates directory could
     * not be found.
     */
    public static String discoverCertDirLocation() {
        String location = null;
    
        location = getDir(globus_dir + "certificates");
        if (location != null) return location;
        
        if (getOS() == UNIX_OS) {
            location = getDir( "/etc/grid-security/certificates");
            if (location != null) return location;
        }

        String suffix = File.separator + "share" + File.separator + 
            "certificates";
            
        location = getDir(System.getProperty("GLOBUS_LOCATION") +
                          suffix);
        if (location != null) return location;
        
        return null;
    }

  
    public static int getOS() {
        if (osType != UNDEFINED_OS) {
            return osType;
        }

        String osname = System.getProperty("os.name");
        if (osname != null) {
            osname = osname.toLowerCase();
            if (osname.indexOf("windows") != -1) {
                osType = WINDOWS_OS;
            } else if ( (osname.indexOf("solaris") != -1) ||
                        (osname.indexOf("sunos") != -1) ||
                        (osname.indexOf("linux") != -1) ||
                        (osname.indexOf("aix") != -1) ||
                        (osname.indexOf("hp-ux") != -1) ||
                        (osname.indexOf("compaq's digital unix") != -1) ||
                        (osname.indexOf("osf1") != -1) ||
                        (osname.indexOf("mac os x") != -1) ||
                        (osname.indexOf("netbsd") != -1) ||
                        (osname.indexOf("freebsd") != -1) ||
                        (osname.indexOf("irix") != -1) ) {
                osType = UNIX_OS;
            } else if (osname.indexOf("mac") != -1) {
                osType = MAC_OS;
            } else {
                osType = OTHER_OS;
            }
        } else {
            osType = OTHER_OS;
        }
    
        return osType;
    }

    private static String getDir(String directory) {
        if (directory == null) return null;
        File f = new File(directory);
        if (f.isDirectory() && f.canRead()) {
            return f.getAbsolutePath();
        } else {
            return null;
        }
    }
    
}
