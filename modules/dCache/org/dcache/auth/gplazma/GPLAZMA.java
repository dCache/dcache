// $Id: GPLAZMA.java,v 1.25 2007-08-03 15:46:02 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.24  2007/04/17 21:47:33  tdh
// Fixed forwarding of log level to AuthorizationController.
//
// Revision 1.23  2007/03/27 19:20:28  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.22  2007/03/16 22:36:17  tdh
// Propagate requested username.
//
// Revision 1.21  2007/03/16 21:59:49  tdh
// Added ability to request username.
// Config files are read only if they have changed.
//
// Revision 1.20  2007/01/04 17:45:29  tdh
// Turned on delegation and added timing lines in tester part of code.
//
// Revision 1.19  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationController.
//
// Revision 1.18  2006/12/15 15:54:43  tdh
// Redid indentations.
//
// Revision 1.17  2006/11/29 19:10:46  tdh
// Added debug, warn log levels and ac command to set loglevel. Added lines to set loglevel in AuthorizationController.
//
// Revision 1.16  2006/11/28 21:11:18  tdh
// Removed hard-code of logging level. Added log-level flag and warn output function.
//
// Revision 1.15  2006/08/24 21:12:13  tdh
// Added priority entry to storage-authdb line and associated field in UserAuthBase.
//
// Revision 1.14  2006/08/23 16:43:39  tdh
// Removed concurrent backport dependence. Create with classname.
//
// Revision 1.13  2006/08/01 19:43:32  tdh
// When authorizing by context, print the role as extracted in the authorization process.
//
// Revision 1.12  2006/07/25 15:04:39  tdh
// Merged DN/Role authentification. Added propagation/logging of authRequestID. Made domain name anonymous.
//
// Revision 1.11.2.1  2006/07/12 19:28:37  tdh
// Added method for receiving authentication by DN message.
//
// Revision 1.11  2006/07/06 14:21:29  tdh
// Fixed ampersand typo.
//
// Revision 1.10  2006/07/06 13:53:32  tdh
// Fixed non-compiling development lines.
//
// Revision 1.9  2006/07/05 16:26:26  tdh
// Undo start of DN work to make a new branch.
//
// Revision 1.8  2006/07/05 16:23:25  tdh
// Starting code to authorize using DN rather than context.
//
// Revision 1.7  2006/07/04 22:23:37  timur
// Use Credential Id to reffer to the remote credential in delegation step, reformated some classes
//
// Revision 1.6  2006/07/03 21:13:41  tdh
// Fixed large log output when using dcache.kpwd method of authorization.
//
// Revision 1.5  2006/07/03 19:56:51  tdh
// Added code to throw and/or catch AuthenticationServiceExceptions from GPLAZMA cell.
//
// Revision 1.4  2006/06/29 20:18:07  tdh
// Added code to run as a testing cell. Added threads to kill authentication attempt after a timeout.
//
// Revision 1.3  2006/06/12 21:55:14  tdh
// Changed package name of concurrent utilities to use backport and compile with java 1.4.
//
// Revision 1.2  2006/06/09 15:46:21  tdh
// Merged (added) to main trunk.
//
// Revision 1.1.2.4  2006/06/06 15:18:46  tdh
// Cleaned up code and added javadoc comments.
//
// Revision 1.1.2.3  2006/06/06 14:23:39  tdh
// Added timeout to delegation socket.
//
// Revision 1.1.2.2  2006/06/02 21:58:07  tdh
// Added thread pool using java.util.concurrent.
//
// Revision 1.1.2.1  2006/05/25 21:21:34  tdh
// New cell to handle authentification and return of user's root path and related info.
//
//

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

package org.dcache.auth.gplazma;

import org.dcache.auth.AuthorizationRecord;
import diskCacheV111.vehicles.*;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import gplazma.authz.AuthorizationException;
import gplazma.authz.AuthorizationController;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.vehicles.AuthorizationMessage;
import org.dcache.vehicles.gPlazmaDelegationInfo;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.ietf.jgss.*;
import org.apache.log4j.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.*;

import java.security.cert.X509Certificate;

/**GPLAZMA Cell.<br/>
 * This Cell make callouts on behalf of other cells to a GUMS server for authenfication and Storage Element information.
 * @see gplazma.authz.AuthorizationController
 **/
public class GPLAZMA extends CellAdapter {

  static Logger log = Logger.getLogger(GPLAZMA.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %C{1} ";

  /** Location of gss files **/
  private String service_key           = "/etc/grid-security/hostkey.pem";
  private String service_cert          = "/etc/grid-security/hostcert.pem";
  private String service_trusted_certs = "/etc/grid-security/certificates";

  /** Policy file to specify the behavior of GPLAZMA **/
  protected String gplazmaPolicyFilePath = "/opt/d-cache/config/dcachesrm-gplazma.policy";

  /** Class for persisting and retrieving AuthorizationRecords **/
  AuthRecordPersistenceManager authzPersistenceManager= null;

  /** Arguments specified in .batch file **/
  private Args _opt;

  /** Specifies logging level **/
  private Level loglevel = Level.ERROR;

  /** Username returned by GUMS will be used **/
  public static final String GLOBUS_URL_COPY_DEFAULT_USER = ":globus-mapping:";

  /** Whether to drop the email attribute from the subject DN extracted from the user's certificate chain **/
  private boolean omitEmail=false;

  /** Thread pool to handle multiple simultaneous authentication requests. **/
  //private final ExecutorService authpool;
  private ThreadPoolTimedExecutor authpool;

  /** Number of simultaneous requests to be handled. **/
  public static int THREAD_COUNT = 10;

  /** Starts a timing thread for each executing request and cancels it upon timeout. **/
  ScheduledExecutorService delaychecker;

  /** Elapsed time in seconds after which an authentication request is canceled.
   *  Includes both the time on the queue and the time for actual request processing. **/
  public static int DELAY_CANCEL_TIME = 15;

  /** Cancel time in milliseconds **/
  private static long toolong = 1000*DELAY_CANCEL_TIME;

  /** Reads input parametes from batch file and initializes thread pools. **/
  public GPLAZMA( String name , String args )  throws Exception {

    super( name, GPLAZMA.class.getSimpleName(), args , false ) ;

    _opt = getArgs() ;

    try{

      /**
       *  USAGE :
       *              -gplazma-authorization-module-policy=GPLAZMA_POLICY_FILE
       *              -log-level=LOG_LEVEL
       *              -num-simultaneous-requests=THREAD_COUNT
       *              -request-timeout=DELAY_CANCEL_TIME
       *
       */

      gplazmaPolicyFilePath = setParam("gplazma-authorization-module-policy", gplazmaPolicyFilePath); //todo: use Opts instead.
      String level = _opt.getOpt("log-level") ;
      if(level==null || level.length()==0) {
        if((getNucleus().getPrintoutLevel() & CellNucleus.PRINT_CELL ) > 0 )
          ac_set_LogLevel_$_1(new Args(new String[]{"INFO"}));
      } else {
        ac_set_LogLevel_$_1(new Args(new String[]{level}));
      }

      setLogLayout(0);

      authzPersistenceManager =
          new AuthRecordPersistenceManager(
              _opt.getOpt("jdbcUrl"),
              _opt.getOpt("jdbcDriver"),
              _opt.getOpt("dbUser"),
              _opt.getOpt("dbPass"));

      THREAD_COUNT = setParam("num-simultaneous-requests", THREAD_COUNT);
      DELAY_CANCEL_TIME = setParam("request-timeout", DELAY_CANCEL_TIME);

      say(this.toString() + " starting with policy file " + gplazmaPolicyFilePath);

      authpool =
        new ThreadPoolTimedExecutor(  THREAD_COUNT,
              THREAD_COUNT,
              60,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue());

      delaychecker = Executors.newScheduledThreadPool(THREAD_COUNT);

      say(this.toString() + " started");

    } catch( Exception iae ){
      log.error(this.toString() + " couldn't start due to " + iae);
      start() ;
      kill() ;
      throw iae ;
    }

    //Make the cell name well-known
    getNucleus().export();
    start() ;

    say(" Constructor finished" ) ;
  }

  /**
   * Sets the logging level.
  */
  public String hh_set_LogLevel = "<loglevel>" ;
  public String fh_set_LogLevel =
    " set LogLevel <loglevel>\n"+
    "        Sets the log level. Choices are DEBUG, INFO, WARN, ERROR.\n"+
    "\n";
  public String ac_set_LogLevel_$_1( Args args ) {
    String newlevel  = (args.argv(0)).toUpperCase();
      if( newlevel.equals("DEBUG") ||
          newlevel.equals("INFO")  ||
          newlevel.equals("WARN")  ||
          newlevel.equals("ERROR") ||
          newlevel.equals("FATAL")  )
        log.setLevel(Level.toLevel(newlevel.toUpperCase()));
      else
        return "Log level not set. Allowed values are DEBUG, INFO, WARN, ERROR.";

    return "Log level set to " + log.getLevel();
  }

  
  public final String hh_get_mapping = "\"<DN>\" [\"FQAN1\",...,\"FQANn\"]";
  public String ac_get_mapping_$_1_2 (Args args) throws AuthorizationException {
      
      
      String principal = args.argv(0);
      /*
       * returns null if argv(1) does not exist
       */
      String roleArg = args.argv(1);
      List<String> roles ;
      if( roleArg != null ) {          
          String[] roleList = roleArg.split(",");
          roles = Arrays.asList(roleList) ;                   
      }else{
          roles = new ArrayList<String>(0);
      }
      
      List<gPlazmaAuthorizationRecord> mappedRecords =  authorize(principal, roles, null, 0);
      StringBuilder sb = new StringBuilder();
      for( gPlazmaAuthorizationRecord record : mappedRecords) {
          sb.append("mapped as: ").append(record.getUsername()).append(" ").
          append(record.getUID()).append(" ").append(record.getGIDs()).append(" ").append(record.getRoot());
      }
      
      return sb.toString();
  }


    private void setLogLayout(long authRequestID) {
        /*
        Appender gplazma_apnd = log.getAppender("GPLAZMA");
        if(gplazma_apnd==null) {
            Enumeration appenders = log.getParent().getAllAppenders();
            while(appenders.hasMoreElements()) {
                Appender apnd = (Appender) appenders.nextElement();
                if(apnd instanceof ConsoleAppender) {
                    if (authRequestID != 0) {
                        String authRequestID_str = AuthorizationController.getFormattedAuthRequestID(authRequestID);
                        apnd.setLayout(new PatternLayout(logpattern + "authRequestID " + authRequestID_str + " %m%n"));
                    } else {
                        apnd.setLayout(new PatternLayout(logpattern + "%m%n"));
                    }
                }
            }
        } else {
            if(gplazma_apnd instanceof ConsoleAppender) {
                if (authRequestID != 0) {
                    String authRequestID_str = AuthorizationController.getFormattedAuthRequestID(authRequestID);
                    gplazma_apnd.setLayout(new PatternLayout(logpattern + "authRequestID " + authRequestID_str + "%m%n"));
                } else {
                    gplazma_apnd.setLayout(new PatternLayout(logpattern + "%m%n"));
                }
            }
        }
        */
        String authRequestID_str = AuthorizationController.getFormattedAuthRequestID(authRequestID);
        PatternLayout loglayout = new PatternLayout(logpattern + authRequestID_str + " %m%n");
        boolean found_console_appender=false;
        Enumeration appenders = log.getParent().getAllAppenders();
        while(appenders.hasMoreElements()) {
            Appender apnd = (Appender) appenders.nextElement();
            if(apnd instanceof ConsoleAppender) {
                found_console_appender = true;
                apnd.setLayout(loglayout);
                ((ConsoleAppender)apnd).setThreshold(Level.DEBUG);
            }
        }
        if(!found_console_appender) {
            ConsoleAppender apnd = new ConsoleAppender(loglayout);
            apnd.setThreshold(Level.DEBUG);
            log.getParent().addAppender(apnd);
        }
    }

  
  /**
   * is called if user types 'info'
   */
  public void getInfo( PrintWriter pw ){
    super.getInfo(pw);
    pw.println("GPLAZMA");
  }

  /**
   * This method is called from finalize() in the CellAdapter
   * super Class to clean the actions made from this Cell.
   * It stops the Thread created.
   */
  public void cleanUp(){

    log.debug(" Clean up called ... " ) ;
    synchronized( this ){
      notifyAll() ;
    }

    authpool.shutdownNow();
    delaychecker.shutdownNow();

    log.debug( "Cleanup done" ) ;
  }

  /**
   * This method is invoked when a message arrives to this Cell.
   * The message is placed on the queue of the thread pool.
   * The sender of the message should block, waiting for the response.
   * @param msg CellMessage
   */
  public synchronized void messageArrived( CellMessage msg ) {

    if(msg.getMessageObject() instanceof DNInfo) {
      AuthFQANRunner arunner = new AuthFQANRunner(msg);
      FutureTimedTask authtask = new FutureTimedTask(arunner, null, System.currentTimeMillis());
      authpool.execute(authtask);
    }

    if(msg.getMessageObject() instanceof diskCacheV111.vehicles.X509Info) {
      AuthX509Runner arunner = new AuthX509Runner(msg);
      FutureTimedTask authtask = new FutureTimedTask(arunner, null, System.currentTimeMillis());
      authpool.execute(authtask);
    }

    if(msg.getMessageObject() instanceof gPlazmaDelegationInfo) {
      AuthRunner arunner = new AuthRunner(msg);
      FutureTimedTask authtask = new FutureTimedTask(arunner, null, System.currentTimeMillis());
      authpool.execute(authtask);
    }
  }

  /**
   * A Runnable class that executes the authentication request.
   * Can skip authentication and return a null message if so commanded.
   */
  public class AuthRunner implements Runnable, Skippable
  {
    public CellMessage msg = null;
    boolean skip_processing=false;

    public AuthRunner(CellMessage msg) {
      this.msg = msg;
    }

    public CellMessage getCellMessage() {
      return msg;
    }

    public void setSkipProcessing(boolean skip) {
      skip_processing = skip;
    }

    /**
     * Creates a socket and returns a message to the calling cell containing the host name and port.
     * Waits for delegation on the socket from the calling cell. Upon successful delegation, makes
     * a callout to authentication server. When the authentication objects is received, forwards it
     * to the calling cell over the delegation socket.
     */
    public void run() {
      if(skip_processing) {
        returnNullMessage();
        return;
      }

      Object msg_obj =  msg.getMessageObject();
      if( ! ( msg_obj instanceof gPlazmaDelegationInfo ) ) {
        log.error("Delegation info is not gPlazmaDelegationInfo" );
        return;
      }

      gPlazmaDelegationInfo deleginfo = (gPlazmaDelegationInfo) msg_obj;

      java.net.ServerSocket ss= null;
      try {
        ss = new java.net.ServerSocket(0,1);
        ss.setSoTimeout(DELAY_CANCEL_TIME*1000);
      }
      catch(IOException ioe) {
        log.error("exception while trying to create a server socket for delegation: " + ioe);
        if(ss!=null) {try {ss.close();} catch(IOException e1) {}}
        return;
      }

      RemoteGsiftpDelegateUserCredentialsMessage cred_request;
      try {
        cred_request =
            new RemoteGsiftpDelegateUserCredentialsMessage(deleginfo.getId(),
                deleginfo.getId(),
                java.net.InetAddress.getLocalHost().getHostName(),
                ss.getLocalPort(),
                deleginfo.getRequestCredentialId());
      } catch(UnknownHostException uhe) {
        log.error("could not find localhost name : " + uhe);
        if(ss!=null) {try {ss.close();} catch(IOException e1) {}}
        return;
      }

      long authRequestID = deleginfo.getId();
      setLogLayout(authRequestID);
      log.debug("sending message requesting delegation " + msg.getUOID());
      msg.revertDirection() ;
      msg.setMessageObject(cred_request) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        log.error("Can't send acl_response for : " + ioe) ;
      }

      java.net.Socket deleg_socket=null;
      try{
        log.debug("waiting for delegation connection");
        //timeout after DELAY_CANCEL_TIME seconds if credentials not delegated
        deleg_socket = ss.accept();
        log.debug("connected");
      } catch ( IOException ioe ){
        log.error("failed to receive delegated server socket");
        if(ss!=null) {try {ss.close();} catch(IOException e1) {}}
        return;
      }

      GSSContext context = null;
      GSIGssSocket gsiSocket=null;
      try {
        context = SslGsiSocketFactory.getServiceContext(service_cert, service_key, service_trusted_certs);
        gsiSocket = new GSIGssSocket(deleg_socket, context);
        gsiSocket.setUseClientMode(false);
        gsiSocket.setAuthorization(
            new org.globus.gsi.gssapi.auth.Authorization() {
              public void authorize(org.ietf.jgss.GSSContext context, String host)
                  throws org.globus.gsi.gssapi.auth.AuthorizationException {
                //we might add some authorization here later
                //but in general we trust that the connection
                //came from a head node and user was authorized
                //already
              }
            });
        gsiSocket.setWrapMode(org.globus.gsi.gssapi.net.GssSocket.SSL_MODE);
        if(deleg_socket!=null) {
          gsiSocket.startHandshake();
          log.debug("delegation succeeded");
        }
      } catch (Throwable t) {
        log.error(t);
        // we do not propogate this exception since some exceptions
        // we catch are not serializable!!!
        //throw new Exception(t.toString());
        if(gsiSocket!=null) {try {gsiSocket.close();} catch(IOException e1) {}}
        return;
      }


       Object writethis;
       log.debug("trying authentication");
       try {
         LinkedList<gPlazmaAuthorizationRecord> gauthlist = authorize(context, deleginfo.getUser(), authRequestID);
         if(gauthlist==null) {
           String errorMsg = "authentication denied";
           log.warn(errorMsg);
           writethis = new AuthorizationException("authRequestID " + authRequestID + errorMsg);
         } else {
           Iterator<gPlazmaAuthorizationRecord> recordsIter = gauthlist.iterator();
           while (recordsIter.hasNext()) {
           gPlazmaAuthorizationRecord rec = recordsIter.next();
           log.debug("mapped to " + rec.getUsername());
           }
           writethis = new AuthenticationMessage(gauthlist, deleginfo.getId());
         }
       } catch (Exception ae) {
         log.warn("Exception: " + ae.getMessage());
         writethis = ae;
       }

      log.debug("sending authentication object");
      if(gsiSocket!=null) {
        try {
          ObjectOutputStream authstrm = new ObjectOutputStream(gsiSocket.getOutputStream());
          authstrm.flush();
          authstrm.writeObject(writethis);
          authstrm.flush();
          log.debug("sent authentication object");
        } catch (IOException ioe) {
          log.error("could not send authorization object");
        }
      } else {
        log.error("context stream not established");
      }

      try {
        if(gsiSocket!=null) gsiSocket.close();
        if(deleg_socket!=null) deleg_socket.close();
        if(ss!=null) ss.close();
      } catch (IOException ioe) {}

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    public void returnNullMessage() {
      long authRequestID = 0;
      Object msg_obj =  msg.getMessageObject();
      if(msg_obj instanceof gPlazmaDelegationInfo) {
        gPlazmaDelegationInfo deleginfo = (gPlazmaDelegationInfo) msg_obj;
        authRequestID = deleginfo.getId();
      }

      msg.revertDirection() ;
      msg.setMessageObject(null);
      try {
        log.error("Timed out, returning null message") ;
        sendMessage(msg);
      } catch ( Exception ioe ){
        log.error("Can't send null message for : " + ioe) ;
      }
    }

  }


  /**
   * A Runnable class that executes the authentication request based on FQAN information.
   * Can skip authentication and return a null message if so commanded.
   */
  public class AuthFQANRunner extends AuthRunner {
    public AuthFQANRunner(CellMessage msg) {
      super(msg);
    }

    public void run() {
      if(skip_processing) {
        returnNullMessage();
        return;
      }

      Object msgobj = msg.getMessageObject();
      log.debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof DNInfo ) ) {
        log.error("message object is not DNInfo");
        return;
      }

      DNInfo dnInfo = (DNInfo) msgobj;
      String subjectDN = dnInfo.getDN();
      List<String> roles = dnInfo.getFQANs();
      long authRequestID = dnInfo.getId();
      setLogLayout(authRequestID);

      log.debug("trying authentication");
      Object writethis;
      try {
         LinkedList <gPlazmaAuthorizationRecord> user_auths = authorize(subjectDN, roles, dnInfo.getUser(), authRequestID);
         if(user_auths==null) {
           log.warn("authentication denied");
         } else {
           Iterator <gPlazmaAuthorizationRecord> recordsIter = user_auths.iterator();
           while (recordsIter.hasNext()) {
             gPlazmaAuthorizationRecord rec = recordsIter.next();
             log.debug("mapped to " + rec.getUsername());
           }
         }
         writethis = new DNAuthenticationMessage(user_auths, dnInfo);
       } catch (Exception ae) {
         log.error("Caught exception in authentication. Forwarding as authentication object");
         writethis = new AuthorizationException(ae.getMessage());
       }

      log.debug("sending message containing authentication");
      msg.revertDirection() ;
      msg.setMessageObject(writethis) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        log.error("Can't send acl_response for : " + ioe) ;
      }

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    public void returnNullMessage() {
      long authRequestID = 0;

      Object msgobj = msg.getMessageObject();
      log.debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof DNInfo ) ) {
        log.error("message object is not DNInfo" );
      } else {
        DNInfo dnInfo = (DNInfo) msgobj;
        authRequestID = dnInfo.getId();
      }

      msg.revertDirection() ;
      msg.setMessageObject(null);
      try {
        log.error("Timed out, returning null message") ;
        sendMessage(msg);
      } catch ( Exception ioe ){
        log.error("Can't send null message for : " + ioe) ;
      }
    }
  }

  /**
   * A Runnable class that executes the authentication request based on FQAN information and x509 certificate chain.
   * Can skip authentication and return a null message if so commanded.
   */
  public class AuthX509Runner extends AuthRunner {
    public AuthX509Runner(CellMessage msg) {
      super(msg);
    }

    public void run() {
      if(skip_processing) {
        returnNullMessage();
        return;
      }

      Object msgobj = msg.getMessageObject();
      log.debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof diskCacheV111.vehicles.X509Info ) ) {
        log.error("message object is not X509Info" );
        return;
      }

      diskCacheV111.vehicles.X509Info x509info = (diskCacheV111.vehicles.X509Info) msgobj;
      X509Certificate[] chain = x509info.getChain();
      long authRequestID = x509info.getId();
      setLogLayout(authRequestID);

      log.debug("trying authentication");
      Object writethis;
      try {
        LinkedList <gPlazmaAuthorizationRecord> gauthlist = authorize(chain, x509info.getUser(), authRequestID);
        if(gauthlist==null) {
          log.warn("authorization denied");
          writethis = new AuthorizationException("authorization denied");
        } else {
          Iterator <gPlazmaAuthorizationRecord> recordsIter = gauthlist.iterator();
          //while (recordsIter.hasNext()) {
          //  gPlazmaAuthorizationRecord rec = recordsIter.next();
          //  log.debug("mapped to " + rec.getUsername());
          //}
          writethis = new X509AuthenticationMessage(gauthlist, x509info);
          AuthenticationMessage authnm = new AuthenticationMessage(gauthlist, authRequestID);
          AuthorizationMessage authzm = new AuthorizationMessage(authnm);
          AuthorizationRecord authrec = authzm.getAuthorizationRecord();
          long id = authrec.getId();

          AuthorizationRecord r=null;
          try {
              do {
                  authrec.setId(id++); // Effectively, a delayed incrementation.
                  r = authzPersistenceManager.find(authrec.getId());
              } while (r!=null && !r.equals(authrec));
          } catch (Exception e) {
              log.error(e.getMessage() + " " + e.getCause());
          }
          if (r==null) {
            authzPersistenceManager.persist(authrec);
          }
        }
      } catch (Exception ae) {
        log.error(ae.getMessage());
        writethis = ae;
      }

      log.debug("sending authentication message");
      msg.revertDirection() ;
      msg.setMessageObject(writethis) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        log.error("Can't send acl_response for : " + ioe) ;
      }

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    public void returnNullMessage() {
      long authRequestID = 0;

      Object msgobj = msg.getMessageObject();
      log.debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof diskCacheV111.vehicles.X509Info ) ) {
        log.error("message object is not X509Info" );
      } else {
        diskCacheV111.vehicles.X509Info X509Info_obj = (diskCacheV111.vehicles.X509Info) msgobj;
        authRequestID = X509Info_obj.getId();
      }

      msg.revertDirection() ;
      msg.setMessageObject(null);
      try {
        log.error("Timed out, returning null message") ;
        sendMessage(msg);
      } catch ( Exception ioe ){
        log.error("Can't send null message for : " + ioe) ;
      }
    }
  }

  /**
   * Allows for skipping the normal thread processsing. Used in the case where a timeout
   * occurs before the thread is even invoked. A null message may still be returned, allowing
   * the calling thread to unblock.
   */
  public interface Skippable {
    public void setSkipProcessing(boolean skip);
  }

  /**
   * Extension of ThreadPoolExecutor to allow tasks to be terminated if a timeout occurs.
   * @see ThreadPoolExecutor
   */
  public class ThreadPoolTimedExecutor extends ThreadPoolExecutor {

    public ThreadPoolTimedExecutor(int corePoolSize,
                          int maximumPoolSize,
                          long keepAliveTime,
                          TimeUnit unit,
                          BlockingQueue workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    /**
     * Start a second, timed task which will terminate the task if it has not finished.
     * If the timeout has already been reached, allows the task to run in abbreviated form.
     * The abbreviated form allows the task to immediately unblock other processes which may be waiting on it.
     * @param t
     * @param r
     */
    public void beforeExecute(Thread t, Runnable r) {
      if(r instanceof TimedFuture) {
        TimedFuture timedtask = (TimedFuture) r;
        long now = System.currentTimeMillis();
        long then = timedtask.getCreateTime();
        long timeleft = toolong - (now - then);

        if(timeleft < 0) {
          timedtask.abbreviateTask(true);
        } else {
          TaskCanceller timerrunner = new TaskCanceller(timedtask);
          ScheduledFuture timer = delaychecker.schedule(timerrunner, timeleft, TimeUnit.MILLISECONDS);
          timedtask.setTimer(timer);
        }
      }
      super.beforeExecute(t, r);
    }

    /**
     * Terminate the timer task for the case where the task finished before the timeout.
     * @param r
     * @param t
     */
    public void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      if(r instanceof TimedFuture) {
        TimedFuture timedtask = (TimedFuture) r;
        timedtask.cancelTimer();
      }
    }

  }

  /**
   * Extends FutureTask to allow tasks to be cancelled due to timeout.
   * @see FutureTask
   */
  public class FutureTimedTask extends FutureTask implements TimedFuture {

    private Callable callable;
    private Runnable runnable;
    private Future timer;
    private long createtime;

    public FutureTimedTask(Callable callable, long createtime) {
      super(callable);
      this.callable = callable;
      this.createtime = createtime;
    }

    FutureTimedTask(Runnable runnable, Object result, long createtime) {
      super(runnable, result);
      this.runnable = runnable;
      this.createtime = createtime;
    }
    public Callable getCallable() {
      return callable;
    }

    public Runnable getRunnable() {
      return runnable;
    }

    /**
     * Task which will terminate this task upon timeout.
     * @param timer
     */
    public void setTimer(Future timer) {
      this.timer = timer;
    }

    public Future getTimer() {
      return timer;
    }

    /**
     * When this task finishes on time, its timer should be terminated.
     */
    public void cancelTimer() {
      if(timer!=null) timer.cancel(true);
    }

    public long getCreateTime() {
      return createtime;
    }

    /**
     * When a task times out on the queue before it is even executed,
     * it may need to run in an abbreviated form to unblock processes
     * which may be waiting on it.
     * @param abbreviate
     */
    public void abbreviateTask(boolean abbreviate) {
      if(callable instanceof Skippable) {
        ((Skippable) callable).setSkipProcessing(abbreviate);
      }
      if(runnable instanceof Skippable) {
        ((Skippable) runnable).setSkipProcessing(abbreviate);
      }
    }

  }

  /**
   * Interface allowing for tasks to be timed.
   * @see FutureTimedTask
   */
  public interface TimedFuture extends Future {
    public long getCreateTime();
    public void setTimer(Future timer);
    public void cancelTimer();
    public void abbreviateTask(boolean abbreviate);
  }

  /**
   * Runnable which cancels a task which has timed out.
   * Will generall have no effect on tasks which are blocking
   * on IO (unless NIO is being used). In that case, the specifice
   * IO sockets themselves will need to have their own timeouts.
   */
  public class TaskCanceller implements Runnable {
    private Future task;

    public TaskCanceller(Future task) {
      this.task = task;
    }

    public Future getFuture() {
      return task;
    }

    public void run () {
      task.cancel(true);
    }

  }

  /**
   * Makes an authorization callout based on the credentials delegated to the service context.
   * @param subjectDN
   * @param desiredUserName
   * @param authRequestID
   * @return
   */
  public  LinkedList <gPlazmaAuthorizationRecord>  authorize(String subjectDN, List<String> roles, String desiredUserName, long authRequestID) throws AuthorizationException {

      AuthorizationController authServ;

      LinkedList <gPlazmaAuthorizationRecord> _user_auths;
      String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
      log.debug("to authorize");

      try {
        authServ = new AuthorizationController(gplazmaPolicyFilePath, authRequestID);
        authServ.setLogLevel(log.getLevel());
      }
      catch( Exception e ) {
        throw new AuthorizationException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
      }

      if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
        log.debug("special case , user is " + _user);
        try {
          _user_auths = authServ.authorize(subjectDN, roles, null, null, null);
        } catch ( AuthorizationException ase ) {
          throw ase;
        } catch ( Exception e ) {
          throw new AuthorizationException("exception: " + e);
        }

        if(_user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      } else {
        try {
          _user_auths = authServ.authorize(subjectDN, roles, _user, null, null);
        } catch ( AuthorizationException ase ) {
          throw ase;
        }
        if(_user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      }

    Iterator <gPlazmaAuthorizationRecord> recordsIter = _user_auths.iterator();
    while (recordsIter.hasNext()) {
      gPlazmaAuthorizationRecord rec = recordsIter.next();
      String GIDS_str = Arrays.toString(rec.getGIDs());
      log.warn("authorized " +  rec.getUsername() + " " + rec.getUID() + " " + GIDS_str + " " + rec.getRoot() + " for " + subjectDN);
    }

    return _user_auths;
  }

  /**
   * Makes an authorization callout based on the credentials delegated to the service context.
   * @param chain
   * @param desiredUserName
   * @param authRequestID
   * @return
   */
  public LinkedList <gPlazmaAuthorizationRecord> authorize(X509Certificate[] chain, String desiredUserName, long authRequestID) throws AuthorizationException {

    AuthorizationController authServ;

    LinkedList <gPlazmaAuthorizationRecord>  _user_auths;
    String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
    log.debug("to authorize using X509Certificate chain");

    try {
      authServ = new AuthorizationController(gplazmaPolicyFilePath, authRequestID);
      authServ.setLogLevel(log.getLevel());
    }
    catch( Exception e ) {
      throw new AuthorizationException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
    }

    if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
      log.debug("special case , user is " + _user);
      try {
        _user_auths = authServ.authorize(chain, null, null, null);
      } catch ( AuthorizationException ase ) {
        throw new AuthorizationException("caught exception " + ase.getMessage());
      } catch ( Exception e ) {
        throw new AuthorizationException("caught exception " + e.getMessage());
      }

      if(_user_auths.isEmpty()) {
        log.error("could not authorize: Permission denied");
        return null;
      }
    } else {
      try {
        _user_auths = authServ.authorize(chain, _user, null, null);
      } catch ( AuthorizationException ase ) {
        throw ase;
      }
      if(_user_auths.isEmpty()) {
        log.error("could not authorize: Permission denied");
        return null;
      }
    }

    String subjectDN;
    try {
      subjectDN = X509CertUtil.getSubjectFromX509Chain(chain, omitEmail);
      if(omitEmail) log.warn("Removed email field from DN: " + subjectDN);
    } catch (Exception e) {
      throw new AuthorizationException("\nException thrown by " + this.getClass().getName() + ": " + e.getMessage());
    }
    Iterator <gPlazmaAuthorizationRecord> recordsIter = _user_auths.iterator();
    while (recordsIter.hasNext()) {
      gPlazmaAuthorizationRecord rec = recordsIter.next();
      String GIDS_str = Arrays.toString(rec.getGIDs());
      log.warn("authorized " +  rec.getUsername() + " " + rec.getUID() + " " + GIDS_str + " " + rec.getRoot() + " for " + subjectDN);
    }
    return _user_auths;
  }

  /**
   * Makes an authorization callout based on the credentials delegated to the service context.
   * @param serviceContext
   * @param desiredUserName
   * @param authRequestID
   * @return
   */
  public LinkedList <gPlazmaAuthorizationRecord> authorize(GSSContext serviceContext, String desiredUserName, long authRequestID) throws AuthorizationException {

      GSSName GSSIdentity = null;
      AuthorizationController authServ;

      LinkedList <gPlazmaAuthorizationRecord> _user_auths;
      String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
      log.debug("to authorize");

      try {
        GSSIdentity = serviceContext.getSrcName();
      } catch( Exception e ) {
        throw new AuthorizationException("Authorization service context name not defined: " + e);
      }

      try {
        authServ = new AuthorizationController(gplazmaPolicyFilePath, authRequestID);
        authServ.setLogLevel(log.getLevel());
      }
      catch( Exception e ) {
        throw new AuthorizationException("Authorization service failed to initialize: " + e);
      }

      if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
        log.debug("special case , user is " + _user);
        try {
          _user_auths = authServ.authorize(serviceContext, null, null, null);
        } //catch ( GSSException gsse ) {
          //throw new AuthorizationException("authRequestID " + authRequestID + " could not determine dn: " + gsse);
        //}
          catch ( AuthorizationException ase ) {
          throw ase;
        } catch ( Exception e ) {
          throw new AuthorizationException("exception: " + e);
        }

        if(_user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      } else {
        try {
          _user_auths = authServ.authorize(serviceContext, _user, null, null);
        } catch ( AuthorizationException ase ) {
          throw ase;
        }
        if(_user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      }



    Iterator <gPlazmaAuthorizationRecord> recordsIter = _user_auths.iterator();
    while (recordsIter.hasNext()) {
      gPlazmaAuthorizationRecord rec = recordsIter.next();
      String GIDS_str = Arrays.toString(rec.getGIDs());
      log.warn("authorized " +  rec.getUsername() + " " + rec.getUID() + " " + GIDS_str + " " + rec.getRoot() + " for " + GSSIdentity);
    }

    return _user_auths;
  }

  public synchronized void Message( String msg1, String msg2 ){
    log.debug("Message received");
  }

  /** Set a parameter according to option specified in .batch config file **/
  private String setParam(String name, String target) {
    if(target==null) target = "";
    String option = _opt.getOpt(name) ;
    if((option != null) && (option.length()>0)) target = option;
    say("Using " + name + " : " + target);
    return target;
  }

  /** Set a parameter according to option specified in .batch config file **/
  private int setParam(String name, int target) {
    String option = _opt.getOpt(name) ;
    if( ( option != null ) && ( option.length() > 0 ) ) {
      try{ target = Integer.parseInt(option); } catch(NumberFormatException e) {}
    }
    say("Using " + name + " : " + target);
    return target;
  }

  /** Set a parameter according to option specified in .batch config file **/
  private long setParam(String name, long target) {
    String option = _opt.getOpt(name) ;
    if( ( option != null ) && ( option.length() > 0 ) ) {
      try{ target = Integer.parseInt(option); } catch(NumberFormatException e) {}
    }
    say("Using " + name + " : " + target);
    return target;
  }


}
