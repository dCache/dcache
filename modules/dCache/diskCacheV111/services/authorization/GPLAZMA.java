// $Id: GPLAZMA.java,v 1.25 2007-08-03 15:46:02 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.24  2007/04/17 21:47:33  tdh
// Fixed forwarding of log level to AuthorizationService.
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
// Moved some functions from GPLAZMA to AuthorizationService.
//
// Revision 1.18  2006/12/15 15:54:43  tdh
// Redid indentations.
//
// Revision 1.17  2006/11/29 19:10:46  tdh
// Added debug, warn log levels and ac command to set loglevel. Added lines to set loglevel in AuthorizationService.
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

package diskCacheV111.services.authorization;

import org.dcache.auth.UserAuthBase;
import org.dcache.auth.UserAuthRecord;
import diskCacheV111.vehicles.*;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.ietf.jgss.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Collection;
import java.util.Arrays;
import java.util.List;

import org.bouncycastle.jce.provider.X509CertificateObject;
import java.security.cert.X509Certificate;

/**GPLAZMA Cell.<br/>
 * This Cell make callouts on behalf of other cells to a GUMS server for authenfication and Storage Element information.
 * @see diskCacheV111.services.authorization.AuthorizationService
 **/
public class GPLAZMA extends CellAdapter implements Runnable {

  /** Location of gss files **/
  private String service_key           = "/etc/grid-security/hostkey.pem";
  private String service_cert          = "/etc/grid-security/hostcert.pem";
  private String service_trusted_certs = "/etc/grid-security/certificates";

  /** Policy file to specify the behavior of GPLAZMA **/
  protected String gplazmaPolicyFilePath = "/opt/d-cache/config/dcachesrm-gplazma.policy";

  /** Arguments specified in .batch file **/
  private Args _opt;

  /** Specifies logging level **/
  private int loglevel = DEBUG_ERROR;


  /**
   *  debug levels, bit mask
   */

  static private final int DEBUG_NONE    = 0;
  static private final int DEBUG_ERROR   = 1;
  static private final int DEBUG_WARN    = 2;
  static private final int DEBUG_INFO    = 4;
  static private final int DEBUG_DEBUG   = 8;

  /** Username returned by GUMS will be used **/
  public static final String GLOBUS_URL_COPY_DEFAULT_USER = ":globus-mapping:";

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

  /** Number of authorizations tested **/
  private long numtests=0;

  /** Average time needed for a test authorization **/
  private float AveTime=0;

  /** Context for testing. If "-proxy-certificate" is set in batch file, will use proxy.
   *  Otherwise, will use host certificates.
   * **/
  GSSContext test_context=null;

  /**
   * Distinguished name to use for testing. If set, will ignore test_context.
   */
  String test_dn=null;

  /**
   * Fully-qualified attribute name to use for testing. If set, will ignore test_context.
   */
  String test_role=null;

  private boolean did_reset=false;

  /** Reads input parametes from batch file and initializes thread pools. **/
  public GPLAZMA( String name , String args )  throws Exception {

    super( name, GPLAZMA.class.getSimpleName(), args , false ) ;

    //useInterpreter( true ) ;
    //addCommandListener(this);

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
      THREAD_COUNT = setParam("num-simultaneous-requests", THREAD_COUNT);
      DELAY_CANCEL_TIME = setParam("request-timeout", DELAY_CANCEL_TIME);

      say(this.toString() + " starting with policy file " + gplazmaPolicyFilePath);

      //authpool = Executors.newFixedThreadPool(THREAD_COUNT);
      authpool =
        new ThreadPoolTimedExecutor(  THREAD_COUNT,
              THREAD_COUNT,
              60,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue());

      delaychecker = Executors.newScheduledThreadPool(THREAD_COUNT);

      say(this.toString() + " started");

      if(setParam("run-gplazma-tests", "false").equals("true")) run_test_authorizations();

    } catch( Exception iae ){
      esay(this.toString() + " couldn't start due to " + iae);
      esay(iae);
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
   * valid values NONE, ERROR, INFO, WARN, DEBUG
   * default ERROR
   * @param levelString
   * @return bitmask of log level or default if string not in valid values of NULL
   */
  private static int stringToLogLevel(String levelString) {

	  int debugMask = DEBUG_ERROR;


	  if( levelString != null ) {
		  String level = levelString.toUpperCase();

		  if( level.equals("DEBUG") ) {
			  debugMask = DEBUG_DEBUG | DEBUG_INFO | DEBUG_WARN |  DEBUG_ERROR;
		  } else if (level.equals("INFO") ) {
			  debugMask = DEBUG_INFO | DEBUG_WARN |  DEBUG_ERROR;
		  } else if (level.equals("WARN") ) {
			  debugMask = DEBUG_WARN |  DEBUG_ERROR;
		  } else if (level.equals("ERROR") ) {
			  debugMask = DEBUG_ERROR;
		  } else if (level.equals("NONE") ) {
			  debugMask = DEBUG_NONE;
		  }
	  }

	  return debugMask;

  }

  /**
   *
   * @param levelMask
   * @return NULL if no valid mask
   */
  private static String logLevelToString( int levelMask ) {

	  String levelString = null;

	  if( (levelMask & DEBUG_DEBUG) > 0 ) {
		  levelString = "ERROR | WARN | INFO | DEBUG";
	  }else if ( (levelMask & DEBUG_INFO) > 0 ) {
		  levelString = "ERROR | WARN | INFO";
	  }else if ( (levelMask & DEBUG_WARN) > 0 ) {
		  levelString = "ERROR | WARN";
	  }else if ( (levelMask & DEBUG_ERROR) > 0 ) {
		  levelString = "ERROR";
	  }else if ( levelMask == DEBUG_NONE  ) {
		  levelString = "NONE";
	  }

	  return levelString;
  }

  /**
   *
   * @param levelMask
   * @return NULL if no valid mask
   */
  private static String logLevelToShortString( int levelMask ) {

	  String levelString = null;

	  if( (levelMask & DEBUG_DEBUG) > 0 ) {
		  levelString = "DEBUG";
	  }else if ( (levelMask & DEBUG_INFO) > 0 ) {
		  levelString = "INFO";
	  }else if ( (levelMask & DEBUG_WARN) > 0 ) {
		  levelString = "WARN";
	  }else if ( (levelMask & DEBUG_ERROR) > 0 ) {
		  levelString = "ERROR";
	  }else if ( levelMask == DEBUG_NONE  ) {
		  levelString = "NONE";
	  }

	  return levelString;
  }


   /** Print to System.err if debug requested **/
   public void debug( String message ){
      if( loglevel >= DEBUG_DEBUG ) {
        super.esay(message);
      }
   }

   /** Print to System.err if info requested **/
   public void say( String message ){
      if( loglevel  >=  DEBUG_INFO ) {
        super.esay(message);
      }
   }

   /** Print to System.err if warning requested **/
   public void warn( String message ){
      if(  loglevel >=  DEBUG_WARN ) {
        super.esay(message);
      }
   }

   /** Print to System.err **/
   public void esay( String message ){

      if( loglevel >=  DEBUG_ERROR) {
    	  super.esay(message);
      }
   }

   /** Print to System.err **/
   public void esay( Throwable t ){

	 if( loglevel >=  DEBUG_ERROR) {
		 super.esay(t);
	 }
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
          newlevel.equals("ERROR")  )
        loglevel = stringToLogLevel(newlevel);
      else
        return "Log level not set. Allowed values are DEBUG, INFO, WARN, ERROR.";

    return "LogLevel set to " + logLevelToString(loglevel);
  }

  
  public final String hh_get_mapping = "\"<DN>\" [\"FQAN1\",...,\"FQANn\"]";
  public String ac_get_mapping_$_1_2 (Args args) throws AuthorizationServiceException {
      
      
      String principal = args.argv(0);
      /*
       * returns null if argv(1) does not exist
       */
      String roleArg = args.argv(1);
      Collection<String> roles ;
      if( roleArg != null ) {          
          String[] roleList = roleArg.split(",");
          roles = Arrays.asList(roleList) ;                   
      }else{
          roles = new ArrayList<String>(0);
      }
      
      List<UserAuthRecord> mappedRecords =  authorize(principal, roles, null, 0);
      StringBuilder sb = new StringBuilder();
      for( UserAuthRecord record : mappedRecords) {
          sb.append("mapped as: ").append(record.Username).append(" ").
          append(record.UID).append(" ").append(record.GID).append(" ").append(record.Root);
      }
      
      return sb.toString();
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

    debug(" Clean up called ... " ) ;
    synchronized( this ){
      notifyAll() ;
    }

    authpool.shutdownNow();
    delaychecker.shutdownNow();

    debug( " Done" ) ;
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

    if(msg.getMessageObject() instanceof X509Info) {
      AuthX509Runner arunner = new AuthX509Runner(msg);
      FutureTimedTask authtask = new FutureTimedTask(arunner, null, System.currentTimeMillis());
      authpool.execute(authtask);
    }

    if(msg.getMessageObject() instanceof RemoteGsiftpTransferProtocolInfo) {
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
      //try{Thread.sleep(500);} catch(Exception e){}
      //skip_processing=true;
      if(skip_processing) {
        returnNullMessage();
        return;
      }

      ProtocolInfo protocol = (ProtocolInfo) msg.getMessageObject();
      //say("delegation protocol= " + protocol.getProtocol());
      if( ! ( protocol instanceof RemoteGsiftpTransferProtocolInfo ) ) {
        esay("delegation protocol info is not RemoteGsiftpransferProtocolInfo" );
        return;
      }

      RemoteGsiftpTransferProtocolInfo remoteGsi = (RemoteGsiftpTransferProtocolInfo) protocol;

      java.net.ServerSocket ss= null;
      try {
        ss = new java.net.ServerSocket(0,1);
        ss.setSoTimeout(DELAY_CANCEL_TIME*1000);
      }
      catch(IOException ioe) {
        esay("exception while trying to create a server socket for delegation: " + ioe);
        if(ss!=null) {try {ss.close();} catch(IOException e1) {}}
        return;
      }

      RemoteGsiftpDelegateUserCredentialsMessage cred_request;
      try {
        cred_request =
            new RemoteGsiftpDelegateUserCredentialsMessage(remoteGsi.getId(),
                remoteGsi.getId(),
                java.net.InetAddress.getLocalHost().getHostName(),
                ss.getLocalPort(),
                remoteGsi.getRequestCredentialId());
      } catch(UnknownHostException uhe) {
        esay("could not find localhost name : " + uhe);
        if(ss!=null) {try {ss.close();} catch(IOException e1) {}}
        return;
      }

      long authRequestID = remoteGsi.getId();
      debug("authRequestID " + authRequestID + " sending message requesting delegation " + msg.getUOID());
      msg.revertDirection() ;
      msg.setMessageObject(cred_request) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        esay("authRequestID " + authRequestID + " Can't send acl_response for : " + ioe) ;
        esay(ioe) ;
      }

      java.net.Socket deleg_socket=null;
      try{
        debug("authRequestID " + authRequestID + " waiting for delegation connection");
        //timeout after DELAY_CANCEL_TIME seconds if credentials not delegated
        deleg_socket = ss.accept();
        debug("authRequestID " + authRequestID + " connected");
      } catch ( IOException ioe ){
        esay("authRequestID " + authRequestID + " failed to receive delegated server socket");
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
          debug("authRequestID " + authRequestID + " delegation succeeded");
        }
      } catch (Throwable t) {
        esay(t);
        // we do not propogate this exception since some exceptions
        // we catch are not serializable!!!
        //throw new Exception(t.toString());
        if(gsiSocket!=null) {try {gsiSocket.close();} catch(IOException e1) {}}
        return;
      }


       Object writethis;
       debug("authRequestID " + authRequestID + " trying authentication");
       try {
         LinkedList<UserAuthRecord> user_auths = authorize(context, remoteGsi.getUser(), authRequestID);
         if(user_auths==null) {
           String errorMsg = "authRequestID " + authRequestID + " authentication denied";
           warn(errorMsg);
           writethis = new AuthorizationServiceException(errorMsg);
         } else {
           Iterator<UserAuthRecord> recordsIter = user_auths.iterator();
           while (recordsIter.hasNext()) {
           UserAuthRecord rec = recordsIter.next();
           debug("authRequestID " + authRequestID + " mapped to " + rec.Username);
           }
           writethis = new AuthenticationMessage(user_auths, remoteGsi.getId());
         }
       } catch (Exception ae) {
         warn("authRequestID " + authRequestID + " Exception: " + ae.getMessage());
         writethis = ae;
       }

      debug("authRequestID " + authRequestID + " sending authentication object");
      if(gsiSocket!=null) {
        try {
          ObjectOutputStream authstrm = new ObjectOutputStream(gsiSocket.getOutputStream());
          authstrm.flush();
          authstrm.writeObject(writethis);
          authstrm.flush();
          debug("authRequestID " + authRequestID + " sent authentication object");
        } catch (IOException ioe) {
          esay("530 authRequestID " + authRequestID + " could not send authorization object");
        }
      } else {
        esay("530 authRequestID " + authRequestID + " context stream not established");
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
      ProtocolInfo protocol = (ProtocolInfo) msg.getMessageObject();
      if(protocol instanceof RemoteGsiftpTransferProtocolInfo) {
        RemoteGsiftpTransferProtocolInfo remoteGsi = (RemoteGsiftpTransferProtocolInfo) protocol;
        authRequestID = remoteGsi.getId();
      }

      msg.revertDirection() ;
      msg.setMessageObject(null);
      try {
        esay("authRequestID " + authRequestID + " Timed out, returning null message") ;
        sendMessage(msg);
      } catch ( Exception ioe ){
        esay("authRequestID " + authRequestID + " Can't send null message for : " + ioe) ;
        esay(ioe) ;
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
      debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof DNInfo ) ) {
        esay("message object is not DNInfo" );
        return;
      }

      DNInfo dnInfo = (DNInfo) msgobj;
      String subjectDN = dnInfo.getDN();
      Collection<String> roles = dnInfo.getFQANs();
      long authRequestID = dnInfo.getId();
      String authRequestID_str = AuthorizationService.getFormattedAuthRequestID(authRequestID);

      debug("authRequestID " + authRequestID_str + " trying authentication");
      Object writethis;
      try {
         LinkedList <UserAuthRecord> user_auths = authorize(subjectDN, roles, dnInfo.getUser(), authRequestID);
         //LinkedList <UserAuthRecord> user_auths = authorize(subjectDN, role, dnInfo.getUser(), authRequestID);
         if(user_auths==null) {
           warn("authRequestID " + authRequestID_str + " authentication denied");
         } else {
           Iterator <UserAuthRecord> recordsIter = user_auths.iterator();
           while (recordsIter.hasNext()) {
             UserAuthRecord rec = recordsIter.next();
             debug("authRequestID " + authRequestID_str + " mapped to " + rec.Username);
           }
         }
         writethis = new DNAuthenticationMessage(user_auths, dnInfo);
       } catch (Exception ae) {
         esay("authRequestID " + authRequestID_str + " Caught exception in authentication. Forwarding as authentication object");
         writethis = new AuthorizationServiceException("authRequestID " + authRequestID_str + " " + ae.getMessage());
       }

      debug("authRequestID " + authRequestID_str + " sending message containing authentication");
      msg.revertDirection() ;
      msg.setMessageObject(writethis) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        esay("authRequestID " + authRequestID_str + " Can't send acl_response for : " + ioe) ;
        esay(ioe) ;
      }

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    public void returnNullMessage() {
      long authRequestID = 0;

      Object msgobj = msg.getMessageObject();
      debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof DNInfo ) ) {
        esay("message object is not DNInfo" );
      } else {
        DNInfo dnInfo = (DNInfo) msgobj;
        authRequestID = dnInfo.getId();
      }

      msg.revertDirection() ;
      msg.setMessageObject(null);
      try {
        esay("authRequestID " + authRequestID + " Timed out, returning null message") ;
        sendMessage(msg);
      } catch ( Exception ioe ){
        esay("authRequestID " + authRequestID + " Can't send null message for : " + ioe) ;
        esay(ioe) ;
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
      debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof X509Info ) ) {
        esay("message object is not X509Info" );
        return;
      }

      X509Info x509info = (X509Info) msgobj;
      X509Certificate[] chain = x509info.getChain();
      long authRequestID = x509info.getId();
      String authRequestID_str = AuthorizationService.getFormattedAuthRequestID(authRequestID);

      debug("authRequestID " + authRequestID_str + " trying authentication");
      Object writethis;
      try {
        LinkedList <UserAuthRecord> user_auths = authorize(chain, x509info.getUser(), authRequestID);
        if(user_auths==null) {
          warn("authRequestID " + authRequestID_str + " authorization denied");
          writethis = new AuthorizationServiceException("authRequestID " + authRequestID_str + " authorization denied");
        } else {
          Iterator <UserAuthRecord> recordsIter = user_auths.iterator();
          while (recordsIter.hasNext()) {
            UserAuthRecord rec = recordsIter.next();
            debug("authRequestID " + authRequestID_str + " mapped to " + rec.Username);
          }
          writethis = new X509AuthenticationMessage(user_auths, x509info);
        }
      } catch (Exception ae) {
        esay("authRequestID " + authRequestID_str + " " + ae.getMessage());
        writethis = ae;
      }

      debug("authRequestID " + authRequestID_str + " sending authentication message");
      msg.revertDirection() ;
      msg.setMessageObject(writethis) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        esay("authRequestID " + authRequestID_str + " Can't send acl_response for : " + ioe) ;
        esay(ioe) ;
      }

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    public void returnNullMessage() {
      long authRequestID = 0;

      Object msgobj = msg.getMessageObject();
      debug("message object = " + msgobj.getClass());
      if( ! ( msgobj instanceof X509Info ) ) {
        esay("message object is not X509Info" );
      } else {
        X509Info X509Info_obj = (X509Info) msgobj;
        authRequestID = X509Info_obj.getId();
      }

      msg.revertDirection() ;
      msg.setMessageObject(null);
      try {
        esay("authRequestID " + authRequestID + " Timed out, returning null message") ;
        sendMessage(msg);
      } catch ( Exception ioe ){
        esay("authRequestID " + authRequestID + " Can't send null message for : " + ioe) ;
        esay(ioe) ;
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
  public  LinkedList <UserAuthRecord>  authorize(String subjectDN, Collection<String> roles, String desiredUserName, long authRequestID) throws AuthorizationServiceException {

      AuthorizationService authServ;

      LinkedList <UserAuthRecord> _user_auths;
      String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
      debug("authRequestID " + authRequestID + " to authorize");

      try {
        authServ = new AuthorizationService(gplazmaPolicyFilePath, authRequestID, this);
        authServ.setLogLevel(logLevelToShortString(loglevel));
      }
      catch( Exception e ) {
        throw new AuthorizationServiceException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
      }

      if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
        debug("authRequestID " + authRequestID + " special case , user is " + _user);
        try {
          _user_auths = authServ.authorize(subjectDN, roles, null, null, null);
        } catch ( AuthorizationServiceException ase ) {
          throw ase;
        } catch ( Exception e ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " exception: " + e);
        }

        if(_user_auths == null) {
          esay("530 authRequestID " + authRequestID + " could not authorize: Permission denied");
          return null;
        }
      } else {
        try {
          _user_auths = authServ.authorize(subjectDN, roles, _user, null, null);
        } catch ( AuthorizationServiceException ase ) {
          throw ase;
        }
        if(_user_auths == null) {
          esay("530 authRequestID " + authRequestID + " could not authorize: Permission denied");
          return null;
        }
      }

    Iterator <UserAuthRecord> recordsIter = _user_auths.iterator();
    while (recordsIter.hasNext()) {
      UserAuthRecord rec = recordsIter.next();
      warn("authRequestID " + authRequestID + " authorized " +  rec.Username + " " + rec.UID + " " + rec.GID + " " + rec.Root + " for " + subjectDN);
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
  public LinkedList <UserAuthRecord> authorize(X509Certificate[] chain, String desiredUserName, long authRequestID) throws AuthorizationServiceException {

    AuthorizationService authServ;

    LinkedList <UserAuthRecord>  _user_auths;
    String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
    debug("authRequestID " + authRequestID + " to authorize using X509Certificate chain");

    try {
      authServ = new AuthorizationService(gplazmaPolicyFilePath, authRequestID, this);
      authServ.setLogLevel(logLevelToShortString(loglevel));
    }
    catch( Exception e ) {
      throw new AuthorizationServiceException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
    }

    if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
      debug("authRequestID " + authRequestID + " special case , user is " + _user);
      try {
        _user_auths = authServ.authorize(chain, null, null, null);
      } catch ( AuthorizationServiceException ase ) {
        throw new AuthorizationServiceException("authRequestID " + authRequestID + " caught exception " + ase.getMessage());
      } catch ( Exception e ) {
        throw new AuthorizationServiceException("authRequestID " + authRequestID + " caught exception " + e.getMessage());
      }

      if(_user_auths.isEmpty()) {
        esay("530 authRequestID " + authRequestID + " could not authorize: Permission denied");
        return null;
      }
    } else {
      try {
        _user_auths = authServ.authorize(chain, _user, null, null);
      } catch ( AuthorizationServiceException ase ) {
        throw ase;
      }
      if(_user_auths.isEmpty()) {
        esay("530 authRequestID " + authRequestID + " could not authorize: Permission denied");
        return null;
      }
    }

    String subjectDN;
    try {
      subjectDN = authServ.getSubjectFromX509Chain(chain);
    } catch (Exception e) {
      throw new  AuthorizationServiceException("\nException thrown by " + this.getClass().getName() + ": " + e.getMessage());
    }
    Iterator <UserAuthRecord> recordsIter = _user_auths.iterator();
    while (recordsIter.hasNext()) {
      UserAuthRecord rec = recordsIter.next();
      String GIDS_str = Arrays.toString(rec.GIDs);
      warn("authRequestID " + authRequestID + " authorized " +  rec.Username + " " + rec.UID + " " + GIDS_str + " " + rec.Root + " for " + subjectDN);
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
  public LinkedList <UserAuthRecord> authorize(GSSContext serviceContext, String desiredUserName, long authRequestID) throws AuthorizationServiceException {

      GSSName GSSIdentity = null;
      AuthorizationService authServ;

      LinkedList <UserAuthRecord> _user_auths;
      String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
      debug("authRequestID " + authRequestID + " to authorize");

      try {
        GSSIdentity = serviceContext.getSrcName();
      } catch( Exception e ) {
        throw new AuthorizationServiceException("authRequestID " + authRequestID + " Authorization service context name not defined: " + e);
      }

      try {
        authServ = new AuthorizationService(gplazmaPolicyFilePath, authRequestID, this);
        authServ.setLogLevel(logLevelToShortString(loglevel));
      }
      catch( Exception e ) {
        throw new AuthorizationServiceException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
      }

      if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
        debug("authRequestID " + authRequestID + " special case , user is " + _user);
        try {
          _user_auths = authServ.authorize(serviceContext, null, null, null);
        } //catch ( GSSException gsse ) {
          //throw new AuthorizationServiceException("authRequestID " + authRequestID + " could not determine dn: " + gsse);
        //}
          catch ( AuthorizationServiceException ase ) {
          throw ase;
        } catch ( Exception e ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " exception: " + e);
        }

        if(_user_auths == null) {
          esay("530 authRequestID " + authRequestID + " could not authorize: Permission denied");
          return null;
        }
      } else {
        try {
          _user_auths = authServ.authorize(serviceContext, _user, null, null);
        } catch ( AuthorizationServiceException ase ) {
          throw ase;
        }
        if(_user_auths == null) {
          esay("530 authRequestID " + authRequestID + " could not authorize: Permission denied");
          return null;
        }
      }



    Iterator <UserAuthRecord> recordsIter = _user_auths.iterator();
    while (recordsIter.hasNext()) {
      UserAuthRecord rec = recordsIter.next();
      warn("authRequestID " + authRequestID + " authorized " +  rec.Username + " " + rec.UID + " " + rec.GID + " " + rec.Root + " for " + GSSIdentity);
    }

    return _user_auths;
  }

  public synchronized void Message( String msg1, String msg2 ){
    debug("Message received");
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

  /**
   * For testing, run several threads which make repeated authorization requests to another GPLAZMA cell.
   **/
  public void run_test_authorizations(){

    say("Start testing");

    test_dn = setParam("test-fqan", "").replace('*', ' ');
    test_role = setParam("test-role", "").replace('*', ' ');

    if(test_dn.equals("")) {
      String test_cert = setParam("proxy-certificate", "host-proxy");
      try {
        if(test_cert.equals("host-proxy"))
          test_context = AuthorizationService.getServiceContext();
        else
          test_context = AuthorizationService.getUserContext(test_cert);
      } catch (GSSException gce) {
        esay(gce);
        esay("could not load host globus credentials " + gce.toString());
      }
    }

    int TEST_THREADS = setParam("max-simultaneous-tests", 10);
    //ExecutorService testerpool = Executors.newFixedThreadPool(TEST_THREADS);
    ArrayBlockingQueue testqueue = new ArrayBlockingQueue(TEST_THREADS+1);
    ExecutorService testerpool =
        new ThreadPoolExecutor(  TEST_THREADS,
            TEST_THREADS,
            60,
            TimeUnit.MILLISECONDS,
            testqueue);

    int MAXTRIES=setParam("max-total-tests", 1100);
    int delay=setParam("delay", 0);
    int numtries=0;
    //while(numtries<Integer.MAX_VALUE) {
    while(numtries<MAXTRIES) {
      AuthTester testrunner = new AuthTester();
      // Block on the testqueue to throttle the requests
      try {
        //say("testqueue size is " + testqueue.size());
        testqueue.put(testrunner);
        testqueue.remove(testrunner);
      } catch (Exception e) {
        e.printStackTrace();
      }
      testerpool.execute(testrunner);
      if(delay>0) try{Thread.sleep(delay);} catch(Exception e){}
      //FutureTask testtack = new FutureTask(testrunner, null);
      //try {
      //  testerpool.submit(testtack).get();
      //} catch (Exception e) {
      //  e.printStackTrace();
      //}
      numtries++;
    }
  }

  /**
   * Allows several test threads to make authentication requests.
   */
  public class AuthTester implements Runnable {

    public void run() {
      UserAuthBase PwdRecord;

      long starttime = System.currentTimeMillis();
      if(test_dn.equals(""))
        PwdRecord = authenticate(test_context);
      else
        PwdRecord = authenticate(test_dn, test_role);
      long finishtime = System.currentTimeMillis();

      if(PwdRecord!=null) averageTime(finishtime-starttime);
    }
  }


  public synchronized float averageTime(long thistime) {
    if(!did_reset && numtests==100) {
      numtests=0;
      AveTime=0;
      did_reset=true;
    }
    AveTime = ((AveTime*numtests) + thistime)/++numtests;
    say("Average authentication time after "  + numtests + " tests is " +  (int) AveTime);
    return AveTime;
  }

  /**
   * For testing, makes authentication requests to another instance of the GPLAZMA cell.
   */
  public UserAuthBase authenticate(GSSContext test_context) {
    AuthorizationService authServ = null;
    UserAuthRecord authRecord = null;
    //say("Running test");
    try {
      authServ = new AuthorizationService(gplazmaPolicyFilePath);
      authServ.setLogLevel(logLevelToShortString(loglevel));
      //boolean delegate_to_gplazma = setParam("delegate-to-gplazma", "false").equalsIgnoreCase("true");
      authServ.setDelegateToGplazma(false);
      authRecord =
          (UserAuthRecord) authServ.authenticate(
              test_context,
              new CellPath("gPlazma"),
        this).getUserAuthBase();

    }  catch(Exception e) {
      e.printStackTrace();
    }

    return authRecord;
  }

  /**
   * For testing, makes authentication requests to another instance of the GPLAZMA cell.
   */
  public UserAuthBase authenticate(String test_dn, String test_role) {
    AuthorizationService authServ = null;
    UserAuthRecord authRecord = null;
    //say("Running test");
    try {
      authServ = new AuthorizationService(gplazmaPolicyFilePath);
      authServ.setLogLevel(logLevelToShortString(loglevel));
      authRecord =
          (UserAuthRecord) authServ.authenticate(
              test_dn, test_role,
              new CellPath("gPlazma"),
        this).getUserAuthBase();

    }  catch(Exception e) {
      e.printStackTrace();
    }

    return authRecord;
  }

  /**
   * Not implemented.
   **/
  public void run(){

  }
}
