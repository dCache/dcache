package diskCacheV111.vehicles;

import java.util.LinkedList;

import gplazma.authz.records.gPlazmaAuthorizationRecord;

public class X509AuthenticationMessage extends AuthenticationMessage {

  X509Info x509info=null;

  public X509AuthenticationMessage() {
    super();
  }

  //public X509AuthenticationMessage(X509Info x509info) {
  //  super(x509info.getId());
  //  this.x509info = x509info;
  //}

  //public X509AuthenticationMessage(UserAuthBase user_auth, X509Info x509info) {
  //  super(user_auth, x509info.getId());
  //  this.x509info = x509info;
  //}

  public X509AuthenticationMessage(LinkedList<gPlazmaAuthorizationRecord> gauthlist, X509Info x509info) {
    super(gauthlist, x509info.getId());
    this.x509info = x509info;
  }

}
