package diskCacheV111.vehicles;

import java.util.Map;

import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.util.NameRolePair;

public class X509AuthenticationMessage extends AuthenticationMessage {
  private static final long serialVersionUID = 5725336468955225808L;
  X509Info x509info=null;

  public X509AuthenticationMessage() {
    super();
  }

  public X509AuthenticationMessage(Map<NameRolePair, gPlazmaAuthorizationRecord> user_auths, X509Info x509info) {
    super(user_auths, x509info.getId());
    this.x509info = x509info;
  }

}
