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

    return user_auths;
  }

}
