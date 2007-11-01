//package diskCacheV111.services.authorization.gplazmalite.storageauthzdbService;
package gplazma.gplazmalite.storageauthzdbService;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 *
 *  @author Abhishek Singh Rana inspired by KAuthFile's author (anonymous?)
 */

public class DCacheSRMauthzRecordsService {

    private static final String STORAGE_AUTHZ_FILENAME="storage-authzdb";
    private static final String PWD_RECORD_MARKER="passwd ";
    private static final String AUTH_RECORD_MARKER="authorize ";
    private static final String FILE_VERSION_MARKER="version ";
    private static final String VERSION_TO_GENERATE="2.1";

    private double fileVersion;
    private HashMap auth_records = new HashMap();
    private HashMap pwd_records = new HashMap();
    private static double fileVersion_static;
    private static HashMap auth_records_static;
    private static HashMap pwd_records_static;
    private static long prev_refresh_time=0;

    /*
    public DCacheSRMauthzRecordsService(String filename)
    throws IOException {
        FileReader fr = new FileReader(filename);
        BufferedReader reader = new BufferedReader(fr);
        read(reader);
        reader.close();
    }
    */

    public DCacheSRMauthzRecordsService(String filename)
    throws IOException {

     try {
        String fileSeparator = System.getProperty("file.separator"); 
        String[] testFilenamePath = filename.split(fileSeparator);
        String testFilename = testFilenamePath[testFilenamePath.length-1];
        //System.out.println("testFilename after splitting from file separator is: " + testFilename);
         if (!testFilename.equals(STORAGE_AUTHZ_FILENAME)) {
           System.out.println("Storage Authorization Db filename " + testFilename + " is not as expected.");
		    System.out.println("WARNING: Possible security violation.");
         }
     } catch(SecurityException se) {
        System.err.println("Exception in testing filename: " +se);
     }

     read(filename);
    }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead();
      if(!readable) System.out.println("WARNING: Could not read storage-authzdb file " + filename + ". Will try to use cached copy.");
      if(readable && config.lastModified() > prev_refresh_time) {
        FileReader fr = new FileReader(config);
        BufferedReader reader = new BufferedReader(fr);
        try {
          read(reader);
        } finally {
          reader.close();
        }
        prev_refresh_time = current_time;
        fileVersion_static = fileVersion;
        auth_records_static = auth_records;
        pwd_records_static = pwd_records;
      } else {
        fileVersion = fileVersion_static;
        auth_records = auth_records_static;
        pwd_records = pwd_records_static;
      }
  }

    private void read(BufferedReader reader)
    throws IOException {
        boolean eof = false;
        String line;
        while((line = reader.readLine()) != null) {
            line = line.trim();
            if(line.startsWith(AUTH_RECORD_MARKER)) {
                line = line.substring(AUTH_RECORD_MARKER.length());
                StorageAuthorizationRecord rec = readNextAuthorizationRecord(line,reader);
                if(rec != null) {
                    auth_records.put(rec.Username,rec);
                }
                else {
                    while( (line = reader.readLine()) != null ) {
                        line=line.trim();
                        if(line.equals("")) {
                            break;
                        }
                    }
                }
            }
            else if( line.startsWith(PWD_RECORD_MARKER)) {
                line = line.substring(PWD_RECORD_MARKER.length());
                StoragePasswordRecord rec = readNextPasswordRecord(line);
                if(rec != null) {
                    pwd_records.put(rec.Username,rec);
                }
            }
            else if(line.startsWith(FILE_VERSION_MARKER)) {
                line = line.substring(FILE_VERSION_MARKER.length());
                line = line.trim();
                fileVersion = Double.parseDouble(line);
            }
        }
    }
                                                                                                                                                                                                     
    private StorageAuthorizationRecord readNextAuthorizationRecord(String line, BufferedReader reader)
    throws IOException {
        line = line.trim();
        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( (ntokens < 5 || ntokens > 6) && (fileVersion >= 2.1 && (ntokens < 6 || ntokens > 7) ) ) {
            return null;
        }
        boolean readOnly = false;
        String user = t.nextToken();
        if(fileVersion >= 2.1) {
            String readOnlyToken = t.nextToken();
            if( readOnlyToken.equals("read-only") ) {
                readOnly = true;
            }
        }
        int uid = Integer.parseInt(t.nextToken());
        int gid = Integer.parseInt(t.nextToken());
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = root;
        if( ( ntokens == 6 && fileVersion < 2.1) || (fileVersion >= 2.1 &&  ntokens == 7 ) ) {
            fsroot = t.nextToken();
        }
        StorageAuthorizationRecord rec =  new StorageAuthorizationRecord(user,readOnly,uid,gid,home,root,fsroot);
        if (rec.isValid()) {
            return rec;
        }
        return null;
    }

    private StoragePasswordRecord readNextPasswordRecord(String line) {
        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( (ntokens < 6 || ntokens > 7) &&
        (fileVersion >= 2.1 && (ntokens < 7 || ntokens > 8) ) ) {
            return null;
        }
        boolean readOnly = false;
        String username = t.nextToken();
        String passwd = t.nextToken();
        if(fileVersion >= 2.1) {
            if( t.nextToken().equals("read-only") ) {
                readOnly = true;
            }
        }
        int uid = Integer.parseInt(t.nextToken());
        int gid = Integer.parseInt(t.nextToken());
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = root;
        if( ntokens == 8 ) {
            fsroot = t.nextToken();
        }
        StoragePasswordRecord rec =  new StoragePasswordRecord(username,passwd,readOnly,uid,gid,home,root,fsroot);
        if (rec.isValid()) {
            return rec;
        }
        return null;
    }

    public StorageAuthorizationRecord getStorageUserRecord(String username) {
        return (StorageAuthorizationRecord)auth_records.get(username);
    }

    public StoragePasswordRecord getStoragePasswordRecord(String username) {
        return (StoragePasswordRecord)pwd_records.get(username);
    }
}

