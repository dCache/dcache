// $Id: DNAuthenticationMessage.java,v 1.3.2.1 2006-07-26 18:42:00 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.3  2006/07/25 15:35:15  tdh
// Added classes to indicate a cell message is a request for authentication.
//
// Revision 1.1  2006/07/12 20:00:14  tdh
// Message to indicate authentication is by DN and role.
//

package diskCacheV111.vehicles;

import diskCacheV111.util.UserAuthBase;

public class DNAuthenticationMessage extends AuthenticationMessage {

  DNInfo dnInfo=null;

  public DNAuthenticationMessage() {
    super();
  }

  public DNAuthenticationMessage(DNInfo fqanInfo) {
    super(fqanInfo.getId());
    this.dnInfo = fqanInfo;
  }

  public DNAuthenticationMessage(UserAuthBase user_auth, DNInfo dnInfo) {
    super(user_auth, dnInfo.getId());
    this.dnInfo = dnInfo;
  }

}
