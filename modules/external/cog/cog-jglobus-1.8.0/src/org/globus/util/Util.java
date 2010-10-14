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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.globus.common.CoGProperties;


public class Util {

    private static final String CHMOD = "chmod";

    private static final String DMSG = 
        "Destroyed by Java Globus Proxy Destroy\r\n";

    /**
     * Attempts to create a new file in an atomic way.
     * If the file already exists, it if first deleted.
     *
     * @param filename the name of file to create.
     * @return the created file.
     * @throws SecurityException if the existing file cannot be deleted.
     * @throws IOException if an I/O error occurred.
     */
    public static File createFile(String filename) 
        throws SecurityException, IOException {
        
        File f = new File(filename);
        if (!f.createNewFile()) {
            if (!destroy(f)) {
                throw new SecurityException(
                                   "Could not destroy existing file");
            }
            if (!f.createNewFile()) {
                throw new SecurityException(
                                   "Failed to atomically create new file");
            }
        }
        return f;
    }

    /**
     * Sets permissions on a given file to be only accessible by the current
     * user.
     *
     * @see #setFilePermissions(String, int)
     */
    public static boolean setOwnerAccessOnly(String file) {
        return setFilePermissions(file, 600);
    }

    /**
     * Sets permissions on a given file. The permissions
     * are set using the <i>chmod</i> command and will only
     * work on Linux/Unix machines. <i>Chmod</i> command must be in the path.
     * <BR><BR><B>Note: </B><I>
     * This function executes an external program; thus, its behavior is
     * influenced by environment variables such as the caller's PATH and the
     * environment variables that control dynamic loading.  Care should be
     * used if calling this function from a program that will be run as a 
     * Unix setuid program, or in any other manner in which the owner of the
     * Unix process does not completely control its runtime environment. 
     * </I>
     * 
     * @param file the file to set the permissions of.
     * @param mode the Unix style permissions.
     * @return true, if change was successful, otherwise false.
     *       It can return false, in many instances, e.g. when file
     *       does not exits, when chmod is not found, or other error
     *       occurs.
     */
    public static boolean setFilePermissions(String file, int mode) {
        // since this will not work on Windows 
        if (ConfigUtil.getOS() == ConfigUtil.WINDOWS_OS) {
            return false;
        }

        Runtime runtime = Runtime.getRuntime();
        String [] cmd = new String[] { CHMOD, 
                                       String.valueOf(mode), 
                                       file };
        Process process = null;
        try {
            process = runtime.exec(cmd, null);
            return (process.waitFor() == 0) ? true : false;
        } catch(Exception e) {
            return false;
        } finally {
            if (process != null) {
                try { 
                    process.getErrorStream().close(); 
                } catch (IOException e) {}
                try {
                    process.getInputStream().close(); 
                } catch (IOException e) {}
                try { 
                    process.getOutputStream().close(); 
                } catch (IOException e) {}
            }
        }
    }
    
    /**
     * Overwrites the contents of the file with a random
     * string and then deletes the file.
     *
     * @param file file to remove
     */
    public static boolean destroy(String file) {
        return destroy(new File(file));
    }
    
    /**
     * Overwrites the contents of the file with a random
     * string and then deletes the file.
     *
     * @param file file to remove
     */
    public static boolean destroy(File file) {
        if (!file.exists()) return false;
        
        RandomAccessFile f = null;
        long size = file.length();
        try {
            f = new RandomAccessFile(file, "rw");
            long rec = size/DMSG.length();
            int left = (int)(size - rec*DMSG.length());
            while(rec != 0) {
                f.write(DMSG.getBytes(), 0, DMSG.length());
                rec--;
            }
            if (left > 0) {
                f.write(DMSG.getBytes(), 0, left);
            }
        } catch(Exception e) {
            return false;
        } finally {
            try {
                if (f != null) f.close();
            } catch(Exception e) {}
        }
        
        return file.delete();
    }
    
    /**
     * Displays a prompt and then reads in the input from System.in.
     *
     * @param prompt  the prompt to be displayed
     * @return <code>String</code> the input read in (entered after the prompt)
     */
    public static String getInput(String prompt) {
        System.out.print(prompt);
        
        try {
            BufferedReader in = 
                new BufferedReader(new InputStreamReader(System.in));
            return in.readLine();
        } catch(IOException e) {
            return null;
        }
    }

    /**
     * Displays a prompt and then reads in private input from System.in.
     * Characters typed by the user are replaced with a space on the screen.
     *
     * @param prompt  the prompt to be displayed
     * @return <code>String</code> the input read in (entered after the prompt)
     */
    public static String getPrivateInput(String prompt) {
        System.out.print(prompt);

        PrivateInputThread privateInput = new PrivateInputThread();
        BufferedReader in = 
            new BufferedReader(new InputStreamReader(System.in));

        privateInput.start();
        try {
            return in.readLine();
        } catch(Exception e) {
            return null;
        } finally {
            privateInput.kill();
        }
    }

    /**
     * A helper thread to mask private user input.
     */
    private static class PrivateInputThread extends Thread {
        private volatile boolean stopThread = false;

        public void kill() {
            this.stopThread = true;
        }

        public void run() {
            while(!this.stopThread) {
                System.out.print("\b ");

                try {
                    sleep(1);
                } catch(InterruptedException e) { }
            }
        }
    }

    /**
     * Quotifies a specified string.
     * The entire string is encompassed by double quotes and each
     * " is replaced with \", \ is replaced with \\.
     *
     * @param str the string to quotify
     * @return quotified and escaped string
     */
    public static String quote(String str) {
        int len = str.length();
        StringBuffer buf = new StringBuffer(len+2);
        buf.append("\"");
        char c;
        for (int i=0;i<len;i++) {
            c = str.charAt(i);
            if (c == '"' || c == '\\') {
                buf.append("\\");
            }
            buf.append(c);
        }
        buf.append("\"");
        return buf.toString();
    }
    
    /**
     * Dequotifies a specified string.
     * The quotes are removed and each \" is replaced with " and
     * each \\ is replaced with \.
     *
     * @param str the string to dequotify.
     * @return unquotified string.
     */
    public static String unquote(String str)
        throws Exception {
        int len = str.length();
        StringBuffer buf = new StringBuffer(len);
        boolean inQuotes = false;
        char c;
        int i = 0;
        
        if (str.charAt(i) == '"') {
            inQuotes = true;
            i++;
        }
        while(i < len) {
            c = str.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    inQuotes = false;
                } else if (c == '\\') {
                    buf.append(str.charAt(++i));
                } else {
                    buf.append(c);
                }
            } else {
                if (c == '\r') {
                    if (str.charAt(i++) != '\n') {
                        throw new Exception("Malformed string.");
                    }
                } else if (c == '"') {
                    inQuotes = true;
                    i++;
                } else {
                    buf.append(c);
                }
            }
            i++;
        }
        return buf.toString();
    }

    /**
     * The function decodes URL-encoded strings into a regular string.
     * This function is mostly copied from the Java source code.
     * To convert to a <code>String</code>, each character is examined in turn:
     * <ul>
     * <li>The remaining characters are represented by 3-character
     * strings which begin with the percent sign,
     * "<code>%<i>xy</i></code>", where <i>xy</i> is the two-digit
     * hexadecimal representation of the lower 8-bits of the character.
     * </ul>
     */
    public static String decode(String s) {
        StringBuffer sb = new StringBuffer();
        for(int i=0; i<s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '%':
                try {
                    sb.append((char)Integer.parseInt(
                                                     s.substring(i+1,i+3),16));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException();
                }
                i += 2;
                break;
            default:
                sb.append(c);
                break;
            }
        }
        // Undo conversion to external encoding
        String result = sb.toString();
        try {
            byte[] inputBytes = result.getBytes("8859_1");
            result = new String(inputBytes);
        } catch (UnsupportedEncodingException e) {
            // The system should always have 8859_1
        }
        return result;
    }    


    /**
     * Generates string representation of given time specified
     * as long. The format is: [days][,][h][,][min][,][sec]
     *
     */
    public static String formatTimeSec(long time) {

        StringBuffer str = new StringBuffer();
    
        if (time < 60) {
            str.append(time + " sec");
            return str.toString();
        } 
    
        int days = (int) time / 86400;
    
        if (days != 0) {
            str.append(days + " days");
            time -= days * 86400;
        }
    
        int hours = (int) time / 3600;
    
        if (hours != 0) {
            if (str.length() != 0) str.append(", ");
            str.append(hours + " h");
            time -= hours * 3600;
        }
    
        int mins  = (int) time / 60;
    
        if (mins != 0) {
            if (str.length() != 0) str.append(", ");
            str.append(mins + " min");
            time -= mins * 60;
        }
    
        int sec   = (int) time;
    
        if (sec != 0) {
            if (str.length() != 0) str.append(", ");
            str.append(sec + " sec");
        }
    
        return str.toString();
    }
    
    /**
     * Returns the ip address of the local machine. 
     * If the 'ip' system property is defined it is returned,
     * otherwise, the local ip address is lookup using the
     * <code>InetAddress</code> class. In case the lookup
     * fails, the address 127.0.0.1 is returned.
     *
     * @return local ip address
     */
    public static String getLocalHostAddress() {
        String ipAddr = CoGProperties.getDefault().getIPAddress();
        if (ipAddr == null) {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "127.0.0.1";
            }
        } else {
            return ipAddr;
        }
    }
    
}
