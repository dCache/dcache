package org.dcache.auth;

import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.util.NameRolePair;

import java.util.*;


public class RecordConvert {
/*
    public UserAuthRecord getAuthRecord(String username, String subjectDN, String role) throws AuthorizationServiceException {

    DCacheSRMauthzRecordsService storageRecordsServ;

    try {
      storageRecordsServ = new DCacheSRMauthzRecordsService(storageAuthzPath);
    } catch(Exception ase) {
      esay("Exception in reading storage-authzdb configuration file: ");
      esay(storageAuthzPath + " " + ase);
      throw new AuthorizationServiceException(ase.toString());
    }

    StorageAuthorizationRecord authRecord = storageRecordsServ.getStorageUserRecord(username);

    if (authRecord == null) {
      esay("A null record was received from the storage authorization service.");
      return null;
    }

    if(authRecord instanceof DynamicAuthorizationRecord) {
      DynamicAuthorizationRecord dynrecord = (DynamicAuthorizationRecord) authRecord;
      dynrecord.subjectDN = subjectDN;
      dynrecord.role = role;
      authRecord = getDynamicRecord(username, dynrecord);
    }

    String  user=authRecord.Username; if(user==null) {
      String denied = DENIED_MESSAGE + ": received null username " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    //Integer uid = localId.getUID(); if(uid==null) {
    int uid = authRecord.UID; if(uid==-1) {
      String denied = DENIED_MESSAGE + ": uid not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    //Integer gid = localId.getGID(); if(gid==null) {
    int[] gids = authRecord.GIDs; if(gids[0]==-1) {
      String denied = DENIED_MESSAGE + ": gids not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		//String home = localId.getRelativeHomePath(); if(home==null) {
    String home = authRecord.Home; if(home==null) {
      String denied = DENIED_MESSAGE + ": relative home path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		//String root = localId.getRootPath(); if(root==null) {
    String root = authRecord.Root; if(root==null) {
      String denied = DENIED_MESSAGE + ": root path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    String fsroot = authRecord.FsRoot; //if(root==null) {
    int priority = authRecord.priority;

    boolean readonlyflag = authRecord.ReadOnly;

    debug("Plugin now forming user authorization records...");
    HashSet principals = new HashSet();

    UserAuthRecord authRecordtoReturn = new UserAuthRecord(user, subjectDN, role, readonlyflag, priority, uid, gids, home, root, fsroot, principals);
    if (authRecordtoReturn.isValid()) {
      debug("User authorization record has been formed and is valid.");
    }

    return authRecordtoReturn;
  }
*/

    /** Extract values from AuthenticationMessage and write in AuthorizationRecord
        * @return A filled-in AuthorizationRecord.
        */
        public static AuthorizationRecord gPlazmaToAuthorizationRecord(Map<NameRolePair, gPlazmaAuthorizationRecord> user_auths) {

          AuthorizationRecord authrec = new AuthorizationRecord();
          gPlazmaAuthorizationRecord gauthrec;
          Iterator<NameRolePair> nameRolesIter = user_auths.keySet().iterator();
          NameRolePair firstNameAndRole = nameRolesIter.hasNext() ? nameRolesIter.next() : null;
          authrec.setName(firstNameAndRole==null ? null : firstNameAndRole.getName());
          for( NameRolePair nameAndRole : user_auths.keySet()) {
            gauthrec = user_auths.get(nameAndRole);
            if(gauthrec!=null) {
                authrec.setIdentity(gauthrec.getUsername());
                authrec.setUid(gauthrec.getUID());
                authrec.setPriority(gauthrec.getPriority());
                authrec.setHome(gauthrec.getHome());
                authrec.setRoot(gauthrec.getRoot());
                authrec.setReadOnly(gauthrec.isReadOnly());
                break;
            }
          }

        List<GroupList> grplistcoll = new LinkedList<GroupList>();
        //Set<gPlazmaAuthorizationRecord> gauth_records = new LinkedHashSet<gPlazmaAuthorizationRecord>(user_auths.values());
        for( NameRolePair nameAndRole : user_auths.keySet()) {
            GroupList grplist = new GroupList();
            List<Group> grpcoll = new LinkedList<Group>();
            gauthrec = user_auths.get(nameAndRole);
            if(gauthrec==null) {
                continue;
            }
            int[] GIDs = gauthrec.getGIDs();
            for (int gid : GIDs) {
                Group grp = new Group();
                grp.setGroupList(grplist);
                grp.setGid(gid);
                grpcoll.add(grp);
            }
            grplist.setGroups(grpcoll);
            grplist.setAttribute(nameAndRole.getRole());
            grplist.setAuthRecord(authrec);
            grplistcoll.add(grplist);
          }
          authrec.setGroupLists(grplistcoll);
          authrec.setId();

          return authrec;
        }


}
