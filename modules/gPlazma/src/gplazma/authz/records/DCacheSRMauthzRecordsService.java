//package diskCacheV111.services.authorization.authz.records;
package gplazma.authz.records;

import org.apache.log4j.*;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.lang.reflect.Method;

/**
 *
 *  @author Abhishek Singh Rana inspired by KAuthFile's author (anonymous?)
 */

public class DCacheSRMauthzRecordsService {
    static Logger log = Logger.getLogger(DCacheSRMauthzRecordsService.class.getSimpleName());

    private static final String STORAGE_AUTHZ_FILENAME="storage-authzdb";
    private static final String PWD_RECORD_MARKER="passwd ";
    private static final String AUTH_RECORD_MARKER="authorize ";
    private static final String DYNAMIC_RECORD_MARKER="dynamic";
    private static final String FILE_VERSION_MARKER="version ";

    private double fileVersion;
    private LinkedHashMap auth_records = null;
    private LinkedHashMap pwd_records = null;
    private LinkedHashMap dynamic_records = null;
    private static double fileVersion_static;
    private static LinkedHashMap auth_records_static;
    private static LinkedHashMap pwd_records_static;
    private static LinkedHashMap dynamic_records_static;
    private static long prev_refresh_time=0;
    static final String dynamic_mapper = "gplazma.authz.records.DynamicMappingMethods";
    static final Map<String, Method> dynamic_methods = new HashMap<String, Method>();

    static {
        try {
          Class DynamicMapper = Class.forName(dynamic_mapper);
          Method[] methods = DynamicMapper.getMethods();
          for (Method meth : methods) {
             dynamic_methods.put(meth.getName(), meth);
          }
        } catch (ClassNotFoundException cnfe) {
          log.error("ClassNotFoundException for DynamicMapper " + dynamic_mapper);
        }
    }

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
         if (!testFilename.equals(STORAGE_AUTHZ_FILENAME)) {
           log.warn("Storage Authorization Db filename " + testFilename + " is not as expected.");
		   log.warn("WARNING: Possible security violation.");
         }
     } catch(SecurityException se) {
        log.error("Exception in testing filename: " +se);
     }
     log.debug("DCacheSRMauthzRecordsService reading " + filename);
     read(filename);
    }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead() || prev_refresh_time==0;
      if(!readable) log.error("WARNING: Could not read storage-authzdb file " + filename + ". Will use cached copy.");
      if(readable && config.lastModified() >= prev_refresh_time) {
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
        dynamic_records_static = dynamic_records;
      } else {
        fileVersion = fileVersion_static;
        auth_records = auth_records_static;
        pwd_records = pwd_records_static;
        dynamic_records = dynamic_records_static;
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
                gPlazmaAuthorizationRecord rec = readNextAuthorizationRecord(line,reader);
                if(rec != null) {
                  if(auth_records==null) auth_records = new LinkedHashMap();
                  auth_records.put(rec.getUsername(),rec);
                } else {
                   log.warn("WARNING: could not parse storage-authzdb line into authorization record: " + line);
                }
            }
            else if(line.startsWith(DYNAMIC_RECORD_MARKER)) {
                String dynkey =  new StringTokenizer(line).nextToken();
                line = line.substring(dynkey.length()).trim();
                DynamicAuthorizationRecord rec = readNextDynamicRecord(dynkey, line);
                if(rec != null) {
                  if(dynamic_records==null) dynamic_records = new LinkedHashMap();
                  dynamic_records.put(rec.getUsername(), rec);
                } else {
                   log.warn("WARNING: could not parse storage-authzdb line into dynamic record: " + line);
                }
            }
            else if(line.startsWith(PWD_RECORD_MARKER)) {
                line = line.substring(PWD_RECORD_MARKER.length());
                PasswordRecord rec = readNextPasswordRecord(line);
                if(rec != null) {
                  if(pwd_records==null) pwd_records = new LinkedHashMap();
                  pwd_records.put(rec.getUsername(), rec);
                } else {
                   log.warn("WARNING: could not parse storage-authzdb line into password record: " + line);
                }
            }
            else if(line.startsWith(FILE_VERSION_MARKER)) {
                line = line.substring(FILE_VERSION_MARKER.length());
                line = line.trim();
                fileVersion = Double.parseDouble(line);
            }
        }
    }
                                                                                                                                                                                                     
    private gPlazmaAuthorizationRecord readNextAuthorizationRecord(String line, BufferedReader reader)
    throws IOException {
        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( ntokens < 7) return null;
        String user = t.nextToken();
        boolean readOnly = (t.nextToken().equals("read-only")) ? true : false;
        int priority = (fileVersion >= 2.2 && ntokens > 7) ? Integer.parseInt(t.nextToken()) : 0;
        String uid_str = t.nextToken();
        int uid = Integer.parseInt(uid_str);
        //allow gids to be coma separated list
        StringTokenizer st1 = new StringTokenizer(t.nextToken(),",");
        int[] gids = new int[st1.countTokens()];
        for(int i =0; st1.hasMoreTokens(); ++i) {
           gids[i]= Integer.parseInt(st1.nextToken());
        }
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = t.nextToken();
        gPlazmaAuthorizationRecord rec =  new gPlazmaAuthorizationRecord(user,readOnly,priority,uid,gids,home,root,fsroot);
        if (rec.isValid()) {
            return rec;
        }
        return null;
    }


  private DynamicAuthorizationRecord readNextDynamicRecord(String dynkey, String line)
    throws IOException {
        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( ntokens < 7) return null;
        String user = t.nextToken();
        String readonly = t.nextToken();
        String priority_str = (fileVersion >= 2.2 && ntokens > 7) ? t.nextToken() : "0";
        String uid_str = t.nextToken();
        String gid_str = t.nextToken();
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = t.nextToken();
        DynamicAuthorizationRecord rec =  new DynamicAuthorizationRecord(dynkey, user,readonly,priority_str,uid_str,gid_str,home,root,fsroot);
        if (rec.isValid()) {
            return rec;
        }
        return null;
    }

    private PasswordRecord readNextPasswordRecord(String line) {
        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( ntokens < 7) {
            return null;
        }
        //if (fileVersion == 2.2 && ntokens < 9) {
        //    return null;
        //}
        String username = t.nextToken();
        String passwd = t.nextToken();
        boolean readOnly = (t.nextToken().equals("read-only")) ? true : false;
        int priority = (fileVersion >= 2.2  && ntokens > 7) ? Integer.parseInt(t.nextToken()) : 0;
        int uid = Integer.parseInt(t.nextToken());
        //allow gids to be coma separated list
        StringTokenizer st1 = new StringTokenizer(t.nextToken(),",");
        int[] gids = new int[st1.countTokens()];
        for(int i =0; st1.hasMoreTokens(); ++i) {
           gids[i]= Integer.parseInt(st1.nextToken());
        }
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = t.nextToken();
        PasswordRecord rec =  new PasswordRecord(username,passwd,readOnly,priority,uid,gids,home,root,fsroot);
        if (rec.isValid()) {
            return rec;
        }
        return null;
    }

    public PasswordRecord getStoragePasswordRecord(String username) {
      return (PasswordRecord)pwd_records.get(username);
    }

    public gPlazmaAuthorizationRecord getStorageUserRecord(String username) {
      gPlazmaAuthorizationRecord record;

      if(auth_records==null) return null;
      record = (gPlazmaAuthorizationRecord) auth_records.get(username);
      if(record==null) {
        record = getDynamicAuthorizationRecord(username);
      }

      return record;
    }

    public DynamicAuthorizationRecord getDynamicAuthorizationRecord(String dynkey) {
      if(dynamic_records==null) return null;
      return (DynamicAuthorizationRecord) dynamic_records.get(dynkey);
    }

    public static String getDynamicMapper() {
      return dynamic_mapper;
    }

    public static Map getDynamicMethods() {
      return dynamic_methods;
    }


  
}

