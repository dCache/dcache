package diskCacheV111.vehicles;

import java.util.Map;
import java.util.LinkedHashMap;

import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.util.NameRolePair;

public class AuthenticationMessage extends Message {
  private static final long serialVersionUID = 2742509423862943223L;
  long authRequestID;
  Map<NameRolePair, gPlazmaAuthorizationRecord> user_auths;    

    public AuthenticationMessage() {
    super();
  }

  public AuthenticationMessage(Map<NameRolePair, gPlazmaAuthorizationRecord> user_auths, long authRequestID) {
    this();
    this.authRequestID = authRequestID;
    this.user_auths=user_auths;
  }

  public long getAuthRequestID() {
    return authRequestID;
  }

  public Map<NameRolePair, gPlazmaAuthorizationRecord> getgPlazmaAuthzMap() {
    if (user_auths==null) {
        user_auths = new LinkedHashMap<NameRolePair, gPlazmaAuthorizationRecord>();
  }

    return user_auths;
    }

}
