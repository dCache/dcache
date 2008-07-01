package diskCacheV111.vehicles;

import org.dcache.auth.UserAuthBase;
import org.dcache.auth.UserAuthRecord;

import java.util.Collection;
import java.util.LinkedList;

public class X509AuthenticationMessage extends AuthenticationMessage {

  X509Info x509info=null;

  public X509AuthenticationMessage() {
    super();
  }

  public X509AuthenticationMessage(X509Info x509info) {
    super(x509info.getId());
    this.x509info = x509info;
  }

  public X509AuthenticationMessage(UserAuthBase user_auth, X509Info x509info) {
    super(user_auth, x509info.getId());
    this.x509info = x509info;
  }

  public X509AuthenticationMessage(LinkedList<UserAuthRecord> user_auths, X509Info x509info) {
    super(user_auths, x509info.getId());
    this.x509info = x509info;
  }

}
