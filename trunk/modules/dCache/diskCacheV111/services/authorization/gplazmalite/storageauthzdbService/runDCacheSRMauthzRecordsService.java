//package diskCacheV111.services.authorization.gplazmalite.storageauthzdbService;
package gplazma.gplazmalite.storageauthzdbService;

import java.util.*;
import java.io.*;
import java.lang.*;

public class runDCacheSRMauthzRecordsService {

 private static String storageAuthzConfPath;
 private static String user_name;
 private static DCacheSRMauthzRecordsService dcacheSrmRecords;


 public static void main(String[] args) {
                                                                                                                                                                                                     
      if(args.length != 2) {
         System.out.println("Usage: runDCacheSRMauthzRecordsService <path to storage-authzdb> <Username>");
         return;
      }

      storageAuthzConfPath = (args[0]);       
      user_name = (args[1]);

      try {
            dcacheSrmRecords = new DCacheSRMauthzRecordsService(storageAuthzConfPath);
      }
      catch(java.io.IOException ioe) {
            System.err.println("Exception in DCacheSRMauthzRecordsService instantiation:" + ioe);
      }


      try {
         StorageAuthorizationRecord authRecord = dcacheSrmRecords.getStorageUserRecord(user_name);
         if (authRecord != null) {
           //return authRecord;
           String username_got = authRecord.Username;
           boolean readonly = authRecord.ReadOnly;
           int priority = authRecord.priority;
           int uid = authRecord.UID;
           int gid = authRecord.GID;
           String home = authRecord.Home;
           String root = authRecord.Root;
           String fsroot = authRecord.FsRoot;

           System.out.println("Records retrieved for Username - " + user_name + " - follow...");
           System.out.println(" Username:        " + username_got);
           System.out.println(" Flag (readonly): " + readonly);
           System.out.println(" priority:        " + uid);
           System.out.println(" UID:             " + uid);
           System.out.println(" GID:             " + gid);
           System.out.println(" Home Path:       " + home);
           System.out.println(" Root Path:       " + root);
           System.out.println(" FS Root Path:    " + fsroot);
         } else {
           System.out.println("Username " + user_name + " is not found in storage-authzdb authorization records");
         }
      } catch(Exception e) {
         System.err.println("Exception:" + e);
      }

 }
}
