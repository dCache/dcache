package diskCacheV111.vehicles;

import diskCacheV111.util.UserAuthBase;
import diskCacheV111.util.UserAuthRecord;

import java.util.Collection;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Iterator;


public class AuthenticationMessage extends Message {

  long authRequestID;
  UserAuthBase user_auth;
  LinkedList<UserAuthRecord> user_auths;
  boolean checked_uniformity=false;

  {
    user_auths = null;
  }

  public AuthenticationMessage() {
    super();
    this.user_auth = null;
  }

  public AuthenticationMessage(UserAuthBase user_auth) {
    super();
    this.user_auth = user_auth;
  }

  public AuthenticationMessage(long authRequestID) {
    super();
    this.user_auth = null;
    this.authRequestID = authRequestID;
  }

  public AuthenticationMessage(UserAuthBase user_auth, long authRequestID) {
    super();
    this.user_auth = user_auth;
    this.authRequestID = authRequestID;
  }

  public AuthenticationMessage(LinkedList <UserAuthRecord> user_auths) {
    super();
    
    if(user_auths == null)  {
        throw new NullPointerException("user_auths == null");
    }
    
    user_auth = user_auths.isEmpty() ? null: user_auths.getFirst();
    this.user_auths = user_auths;
  }

  public AuthenticationMessage(LinkedList <UserAuthRecord> user_auths, long authRequestID) {
    this(user_auths);
    this.authRequestID = authRequestID;
  }

  public long getAuthRequestID() {
    return authRequestID;
  }

  public UserAuthBase getUserAuthBase() {
    return user_auth;
  }

  public LinkedList <UserAuthRecord> getUserAuthRecords() {
    if (user_auths==null) {
      user_auths = new LinkedList <UserAuthRecord> ();
      if(user_auth!=null) user_auths.add(
       new UserAuthRecord(user_auth.Username,
			                    user_auth.ReadOnly,
                          user_auth.UID,
                          new int[]{user_auth.GID},
                          user_auth.Home,
                          user_auth.Root,
                          user_auth.FsRoot,
                          new HashSet()));
    }

    if(!checked_uniformity) {
      int[] GIDs = checkUniformity(user_auths);
      checked_uniformity=true;

      if(GIDs!=null && GIDs.length > 1) {
        UserAuthRecord rec = user_auths.getFirst();
        user_auths = new LinkedList <UserAuthRecord> ();
        user_auths.add
        (new UserAuthRecord(rec.Username,
			                    rec.ReadOnly,
                          rec.UID,
                          GIDs,
                          rec.Home,
                          rec.Root,
                          rec.FsRoot,
                          new HashSet()));
      }
    }

    return user_auths;
  }

  public UserAuthBase getNextUserAuthRecord() {

    user_auth = getUserAuthRecords().isEmpty() ? null : getUserAuthRecords().getFirst();

    if(user_auth==null) return null;

    UserAuthRecord rec = (UserAuthRecord) user_auth;

    if(rec.GIDs != null && rec.currentGIDindex < rec.GIDs.length) {
      rec.GID = rec.GIDs[rec.currentGIDindex++];
    } else {
      getUserAuthRecords().removeFirst();
      return getNextUserAuthRecord();
    }

    return user_auth;
  }

  public int[] checkUniformity(LinkedList <UserAuthRecord> user_auths) {
    int[] GIDs;
    UserAuthRecord rec;
    int last_uid;
    String last_root;

    if(user_auths==null) return null;

    Iterator <UserAuthRecord> authsIter = user_auths.iterator();

    if(!authsIter.hasNext()) return null;

    rec = authsIter.next();
    last_uid = rec.UID;
    last_root = rec.Root;

    int i=0;
    while (authsIter.hasNext()) {
      rec = authsIter.next();
      if(rec.UID != last_uid || !rec.Root.equals(last_root)) return null;
      i++;
    }

    // Write into array
    GIDs = new int[i+1];
    authsIter = user_auths.iterator();
    i=0;
    while (authsIter.hasNext()) {
      rec = authsIter.next();
      for(int j=0; j<rec.GIDs.length; j++) {
        GIDs[i++] = rec.GIDs[j];
      }
    }

    return GIDs;
  }

  public void setCheckedUniformity(boolean boolval) {
    checked_uniformity = boolval;
  }

}
