//package diskCacheV111.services.authorization.authz.records;
package gplazma.authz.records;

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
         gPlazmaAuthorizationRecord authRecord = dcacheSrmRecords.getStorageUserRecord(user_name);
         if (authRecord != null) {
           //return authRecord;
           String username_got = authRecord.getUsername();
           boolean readonly = authRecord.isReadOnly();
           int priority = authRecord.getPriority();
           int uid = authRecord.getUID();
           int[] gids = authRecord.getGIDs();
           String home = authRecord.getHome();
           String root = authRecord.getRoot();
           String fsroot = authRecord.getFsRoot();

           System.out.println("Records retrieved for Username - " + user_name + " - follow...");
           System.out.println(" Username:        " + username_got);
           System.out.println(" Flag (readonly): " + readonly);
           System.out.println(" priority:        " + priority);
           System.out.println(" UID:             " + uid);
           System.out.println(" GIDs:            " + gids);
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
