package diskCacheV111.vehicles;

import diskCacheV111.util.UserAuthBase;


public class AuthenticationMessage extends Message {

  long authRequestID;
  UserAuthBase user_auth;

  public AuthenticationMessage() {
    super();
    this.user_auth = null;
    this.authRequestID = 0;
  }

  public AuthenticationMessage(UserAuthBase user_auth) {
    super();
    this.user_auth = user_auth;
    this.authRequestID = 0;
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

  public long getAuthRequestID() {
    return authRequestID;
  }

  public UserAuthBase getUserAuthBase() {
    return user_auth;
  }

}
