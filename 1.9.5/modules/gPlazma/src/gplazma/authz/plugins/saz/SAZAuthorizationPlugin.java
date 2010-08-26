package gplazma.authz.plugins.saz;

//import org.dcache.auth.UserAuthRecord;
import org.apache.log4j.*;

import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import fnal.vox.security.Base64;
import fnal.vox.security.ReadWriteSocket;
import gplazma.authz.AuthorizationException;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.plugins.AuthorizationPlugin;
import gplazma.authz.plugins.LoggingPlugin;

/**
 *
 * @author Ted Hesselroth
 */

public class SAZAuthorizationPlugin extends LoggingPlugin {

  private long authRequestID=0;
  private ReadWriteSocket rwSocket;
  static Logger log = Logger.getLogger(SAZAuthorizationPlugin.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);

  public SAZAuthorizationPlugin(long authRequestID) throws AuthorizationException {
    super(authRequestID);
    getLogger().info("saz plugin now loaded");
  }

  public void setLogLevel	(String level) {
    log.setLevel(Level.toLevel(level));
  }

  public gPlazmaAuthorizationRecord authorize(X509Certificate[] certs, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationException {

    gPlazmaAuthorizationRecord record=null;
    //log.setLevel(Level.DEBUG);

    rwSocket = new ReadWriteSocket(socket);
    String messageFromServer=null;
    //int clientIdx = X509CertUtil.findClientCert(myValidatedChain);

    try {

      for(int i=0;i<certs.length;i++){
        String temp="-----BEGIN CERTIFICATE-----\n";
        byte derEncodedByte[]=certs[i].getEncoded();
        temp=temp+ Base64.encodeBytes(derEncodedByte);
        temp=temp+"\n-----END CERTIFICATE-----\n";
        rwSocket.sendMessage(temp);
        messageFromServer=rwSocket.recvMessage();
			  log.debug("From server " + messageFromServer);
        if(!messageFromServer.equals("more")){
          throw new Exception("Failed to send cert chain to the server");
        }
      }

      rwSocket.sendMessage("done");
      messageFromServer=rwSocket.recvMessage();
      log.debug("From server "+messageFromServer);
      if(messageFromServer.equals("no")){
        rwSocket.sendMessage("more");
        messageFromServer=rwSocket.recvMessage();
        log.debug("The Reason "+messageFromServer);
      } else {
      if(messageFromServer.equals("yes")) {
        rwSocket.sendMessage("more");
        messageFromServer=rwSocket.recvMessage();
        log.debug("The Resource "+messageFromServer);
      }
        record = new gPlazmaAuthorizationRecord();
      }
      } catch (Exception e) {
        log.error("Exception in finding SAZ Authorization : " + messageFromServer + "\n" + e);
        rwSocket.closeAll();
        throw new AuthorizationException(messageFromServer + "\n" + e.toString());
      }

    rwSocket.closeAll();

    return record;
  }

  public gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationException {
    return null;
  }

    /**
    * A method that communicates with LRAServer or SAZServer to get back the data from LRADB or SAZDB.
    * @param certs The chain of certificates.
    */
    public void sendRequest(X509Certificate[] certs) throws Exception{


    }



}
