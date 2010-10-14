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
package org.globus.ftp;

import org.globus.ftp.exception.FTPException;
import java.util.StringTokenizer;

/**
 * Represents the properties of a remote file
 * such as size, name, modification date and time, etc.
 * Can represent a regular file as well as a directory
 * or a soft link.
 */
public class FileInfo {
  
    public static final byte UNKNOWN_TYPE   = 0;
    public static final byte FILE_TYPE      = 1;
    public static final byte DIRECTORY_TYPE = 2;
    public static final byte SOFTLINK_TYPE  = 3;
    public static final byte DEVICE_TYPE  = 4;
    
    public static final String UNKNOWN_STRING = "?";  
    public static final int UNKNOWN_NUMBER = -1;  
    
    private long size = UNKNOWN_NUMBER;
    private String name = UNKNOWN_STRING;
    private String date = UNKNOWN_STRING;
    private String time = UNKNOWN_STRING;
    private byte fileType;
    private int mode = 0;
    
    /**
     * Used internally by the FTPClient.
     */
    public FileInfo() {
    }
    
    /**
     * Parses the file information from one line of response to
     * the FTP LIST command. Note: There is no commonly accepted
     * standard for the format of LIST response. 
     * This parsing method only accepts 
     * the most common Unix file listing formats:
     * System V or Berkeley (BSD) 'ls -l'
     *
     * @see #parseUnixListReply(String reply) 
     * @param unixListReply a single line from ls -l command
     */
    public FileInfo(String unixListReply) 
        throws FTPException {
        parseUnixListReply(unixListReply);
    }
    
    /**
     * Given a line of reply received as the result of "LIST" command,
     * this method will set all the attributes(name,size,time,date and file type)
     * of the named file. This method requires the reply to be in 
     * FTP server format, corresponding to either Unix System V or 
     * Berkeley (BSD) output of 'ls -l'. For example,
     * <pre>drwxr-xr-x   2      guest  other  1536  Jan 31 15:15  run.bat</pre>
     *  or
     * <pre>-rw-rw-r--   1      globus    117579 Nov 29 13:24 AdGriP.pdf</pre>
     * If the entry corresponds to a device file, only the file type 
     * will be set and the other parameters will be set to UNKNOWN.
     * 
     * @param     reply reply of FTP server for "dir" command.
     * @exception FTPException if unable to parse the reply
     */
    //protected void parseUnixListReply(String reply) 
    public void parseUnixListReply(String reply) 
        throws FTPException {
        if (reply == null) return;
        
        StringTokenizer tokens = new StringTokenizer(reply);
        String token, previousToken;
        
        int numTokens = tokens.countTokens();
        
        if (numTokens < 8) {
            throw new FTPException(FTPException.UNSPECIFIED,
                           "Invalid number of tokens in the list reply [" + 
                                   reply + "]");
        }
        
        token = tokens.nextToken();
        
        // permissions
        switch( token.charAt(0) ) {
        case 'd':
            setFileType(DIRECTORY_TYPE); break;
        case '-':
            setFileType(FILE_TYPE); break;
        case 'l':
            setFileType(SOFTLINK_TYPE); break;
        case 'c':
        case 'b':
            // do not try to parse device entries;
            // they aren't important anyway
            setFileType(DEVICE_TYPE); 
            return;
        default:
            setFileType(UNKNOWN_TYPE);
        }
        
        try {
            for(int i=1;i<=9;i++) {
                if (token.charAt(i) != '-') {
                    mode += 1 << (9 - i);
                }
            }
        } catch (IndexOutOfBoundsException e) {
            throw new FTPException(FTPException.UNSPECIFIED,
                                   "Could not parse access permission bits");
        }

        
        // ??? can ignore
        tokens.nextToken();
        
        // next token is the owner
        tokens.nextToken();
        
        // In ls from System V, next token is the group
        // In ls from Berkeley (BSD), group token is missing
        previousToken = tokens.nextToken();
        
        // size
        token = tokens.nextToken();
        
        /*
         * if the group is missing this will try to parse the date field
         * as an integer and will fail. if so, then the previous field is the size field
         * and the current token is part of the date. 
         */
        try {
            setSize( Long.parseLong(token) );
            token = null;
        } catch(NumberFormatException e) {
            // this might mean that the group is missing
            // and this token is part of date.
            try {
                setSize( Long.parseLong(previousToken) );
            } catch(NumberFormatException ee) {
                throw new FTPException(FTPException.UNSPECIFIED,
                                       "Invalid size number in the ftp reply [" + 
                                       previousToken + ", " + token + "]");
            }
        }
        
        // date - two fields together
        if (token == null) {
            token = tokens.nextToken();
        }
        String month = token;
        setDate(token + " " + tokens.nextToken());
        
        //next token is either date or time
        token = tokens.nextToken();
        this.setTime(token);
        
        // this is to handle spaces in the filenames
        // as well filenames with dates in them
        int ps = reply.indexOf(month);
        if (ps == -1) {
            // this should never happen
            throw new FTPException(FTPException.UNSPECIFIED,
                                   "Could not find date token");
        } else {
            ps = reply.indexOf(this.time, ps+month.length());
            if (ps == -1) {
                // this should never happen
                throw new FTPException(FTPException.UNSPECIFIED,
                                       "Could not find time token");
            } else {
                this.setName(reply.substring(1+ps+this.time.length()));
            }
        }
    }
    
    // --------------------------------
    
    /**
     * Sets the file size.
     *
     * @param size size of the file
     */
    public void setSize(long size) {
        this.size = size;
    }
    
    /**
     * Sets the file name.
     *
     * @param name name of the file.
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Sets the file date.
     * 
     * @param date date of the file.
     */
    public void setDate(String date) {
        this.date = date;
    }
    
    /**
     * Sets modification time of the file.
     * 
     * @param time time of the file.
     */
    public void setTime(String time) {
        this.time = time;
    }
    
    /**
     * Sets the file type.
     *
     * @param type one of the file types, 
     *             e.g. FILE_TYPE, DIRECTORY_TYPE
     *             
     */
    public void setFileType(byte type) {
        this.fileType = type;
    }
    
    // ---------------------------------
    
    /**
     * Returns size of the file.
     *
     * @return size of the file in bytes
     */
    public long getSize() {
        return size;
    }
    
    /**
     * Returns name of the file.
     * 
     * @return name of the file.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns date of the file.
     * 
     * @return date of the file.
     */
    public String getDate() {
        return date;
    }
    
    /**
     * Returns modification time of the file.
     * 
     * @return time of the file.
     */
    public String getTime() {
        return time;
    }
    
    /**
     * Tests if this file is a file.
     *
     * @return true if this represents a file,
     *         otherwise, false.
     */
    public boolean isFile() {
        return (fileType == FILE_TYPE);
    }
    
    /**
     * Tests if this file is a directory.
     * 
     * @return true if this reprensets a directory,
     *         otherwise, false.
     */
    public boolean isDirectory() {
        return (fileType == DIRECTORY_TYPE);
    }
    
    /**
     * Tests if this file is a softlink.
     * 
     * @return true if this reprensets a softlink,
     *         otherwise, false.
     */
    public boolean isSoftLink() {
        return (fileType == SOFTLINK_TYPE);
    }
    
    /**
     *  Tests if this file is a device.
     */
    
    public boolean isDevice() {
        return (fileType == DEVICE_TYPE);
    }

    public int getMode() {
      return mode;
    }

    public String getModeAsString() {
      StringBuffer modeStr = new StringBuffer();
      for(int j=2;j>=0;j--) {
          int oct = 0;
          for(int i=2;i>=0;i--) {
              if ((mode & (1 << j*3+i)) != 0) {
                  oct += (int)Math.pow(2,i);
              }
          }
          modeStr.append(String.valueOf(oct));
      }
      return modeStr.toString();
    }

    public boolean userCanRead() {
      return ((mode & (1 << 8)) != 0);
    }

    public boolean userCanWrite() {
      return ((mode & (1 << 7)) != 0);
    }

    public boolean userCanExecute() {
      return ((mode & (1 << 6)) != 0);
    }

    public boolean groupCanRead() {
      return ((mode & (1 << 5)) != 0);
    }

    public boolean groupCanWrite() {
      return ((mode & (1 << 4)) != 0);
    }

    public boolean groupCanExecute() {
      return ((mode & (1 << 3)) != 0);
    }

    public boolean allCanRead() {
      return ((mode & (1 << 2)) != 0);
    }

    public boolean allCanWrite() {
      return ((mode & (1 << 1)) != 0);
    }

    public boolean allCanExecute() {
      return ((mode & (1 << 0)) != 0);
    }
  
    // --------------------------------
    
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("FileInfo: ");
        buf.append(getName() + " ");
        buf.append(getSize() + " ");
        buf.append(getDate() + " ");
        buf.append(getTime() + " ");
        
        switch( fileType  ) {
        case DIRECTORY_TYPE:
            buf.append("directory");
            break;
        case FILE_TYPE:
            buf.append("file");
            break;
        case SOFTLINK_TYPE:
            buf.append("softlink");
            break;
        default:
            buf.append("unknown type");
        }
        buf.append(" "+getModeAsString());
        
        return buf.toString();
    }
    
}




