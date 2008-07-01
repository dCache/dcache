package diskCacheV111.services.authorization;

import org.dcache.auth.UserAuthRecord;
import org.apache.log4j.*;

import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import fnal.vox.security.Base64;
import fnal.vox.security.ReadWriteSocket;

/**
 *
 * @author Ted Hesselroth
 */

public class SAZAuthorizationPlugin extends AuthorizationServicePlugin {

  private String serviceUrl;
  private long authRequestID=0;
  private ReadWriteSocket rwSocket;
  static Logger log = Logger.getLogger(SAZAuthorizationPlugin.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);

  public SAZAuthorizationPlugin(String serviceUrl, long authRequestID)
	throws AuthorizationServiceException {
		this.serviceUrl = serviceUrl;
    this.authRequestID=authRequestID;
    if(((Logger)log).getAppender("SAZAuthorizationPlugin")==null) {
      Enumeration appenders = log.getParent().getAllAppenders();
      while(appenders.hasMoreElements()) {
        Appender apnd = (Appender) appenders.nextElement();
        if(apnd instanceof ConsoleAppender)
          apnd.setLayout(loglayout);
      }
    }
    log.debug("SAZAuthorizationPlugin: authRequestID " + authRequestID + " Plugin now loaded: saz-authorization");
	}

	public SAZAuthorizationPlugin(String serviceUrl)
	throws AuthorizationServiceException {
		log.debug("SAZAuthorizationPlugin: now loaded: saz-authorization Plugin");
		this.serviceUrl = serviceUrl;
	}

	public SAZAuthorizationPlugin()
	throws AuthorizationServiceException {
		log.debug("SAZAuthorizationPlugin: now loaded: saz-authorization Plugin");
	}

  public void setLogLevel	(String level) {
    log.setLevel(Level.toLevel(level));
  }

  private void debug(String s) {
    log.debug("SAZAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void say(String s) {
    log.info("SAZAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void warn(String s) {
    log.warn("SAZAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void esay(String s) {
    log.error("SAZAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  public UserAuthRecord authorize(X509Certificate[] certs, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

    UserAuthRecord record=null;
    //log.setLevel(Level.DEBUG);

    rwSocket = new ReadWriteSocket(socket);
    String messageFromServer=null;
    //int clientIdx = CertUtil.findClientCert(myValidatedChain);

    try {

      for(int i=0;i<certs.length;i++){
        String temp="-----BEGIN CERTIFICATE-----\n";
        byte derEncodedByte[]=certs[i].getEncoded();
        temp=temp+ Base64.encodeBytes(derEncodedByte);
        temp=temp+"\n-----END CERTIFICATE-----\n";
        rwSocket.sendMessage(temp);
        messageFromServer=rwSocket.recvMessage();
			  debug("From server " + messageFromServer);
        if(!messageFromServer.equals("more")){
          throw new Exception("Failed to send cert chain to the server");
        }
      }

      rwSocket.sendMessage("done");
      messageFromServer=rwSocket.recvMessage();
      debug("From server "+messageFromServer);
      if(messageFromServer.equals("no")){
        rwSocket.sendMessage("more");
        messageFromServer=rwSocket.recvMessage();
        debug("The Reason "+messageFromServer);
      } else {
      if(messageFromServer.equals("yes")) {
        rwSocket.sendMessage("more");
        messageFromServer=rwSocket.recvMessage();
        debug("The Resource "+messageFromServer);
      }
        record = new UserAuthRecord();
      }
      } catch (Exception e) {
        esay("Exception in finding SAZ Authorization : " + messageFromServer + "\n" + e);
        rwSocket.closeAll();
        throw new AuthorizationServiceException (messageFromServer + "\n" + e.toString());
      }

    rwSocket.closeAll();

    return record;
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {
    return null;
  }

    /**
    * A method that communicates with LRAServer or SAZServer to get back the data from LRADB or SAZDB.
    * @param certs The chain of certificates.
    */
    public void sendRequest(X509Certificate[] certs) throws Exception{


    }



}
