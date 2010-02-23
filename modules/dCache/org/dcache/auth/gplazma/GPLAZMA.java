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
import org.dcache.auth.AuthzQueryHelper;
import diskCacheV111.vehicles.*;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import gplazma.authz.AuthorizationException;
import gplazma.authz.AuthorizationController;
import gplazma.authz.util.NameRolePair;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import dmg.cells.nucleus.*;
import dmg.util.Args;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.vehicles.AuthorizationMessage;
import org.dcache.vehicles.gPlazmaDelegationInfo;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.ietf.jgss.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.*;

import java.security.cert.X509Certificate;

/**
 * GPLAZMA Cell.
 *
 * This Cell make callouts on behalf of other cells to a GUMS server
 * for authenfication and Storage Element information.
 * @see gplazma.authz.AuthorizationController
 **/
public class GPLAZMA extends AbstractCell
{
  private static final Logger log = LoggerFactory.getLogger(GPLAZMA.class);

  /** Location of gss files **/
  private static final String SERVICE_KEY =
    "/etc/grid-security/hostkey.pem";
  private static final String SERVICE_CERT =
    "/etc/grid-security/hostcert.pem";
  private static final String SERVICE_TRUSTED_CERTS =
    "/etc/grid-security/certificates";

  /** Username returned by GUMS will be used **/
  public static final String GLOBUS_URL_COPY_DEFAULT_USER = ":globus-mapping:";

  /** Class for persisting and retrieving AuthorizationRecords **/
  protected AuthRecordPersistenceManager authzPersistenceManager;

  /** Thread pool to handle multiple simultaneous authentication
   *  requests. **/
  private ThreadPoolTimedExecutor authpool;

  /** Starts a timing thread for each executing request and cancels it
   *  upon timeout. **/
  private ScheduledExecutorService delaychecker;

  /** Cancel time in milliseconds **/
  private long tooLong;

  @Option(
    name = "gplazma-authorization-module-policy",
    description = "Policy file to specify the behavior of GPLAZMA",
    defaultValue = "/opt/d-cache/config/dcachesrm-gplazma.policy"
  )
  protected String gplazmaPolicyFilePath;

  @Option(
    name = "jdbcUrl",
    required = true
  )
  protected String jdbcUrl;

  @Option(
    name = "jdbcDriver",
    required = true
  )
  protected String jdbcDriver;

  @Option(
    name = "dbUser",
    required = true
  )
  protected String dbUser;

  @Option(
    name = "dbPass",
    required = true
  )
  protected String dbPass;

  @Option(
    name = "num-simultaneous-requests",
    description = "Number of simultaneous requests to be handled",
    defaultValue = "10"
  )
  protected int threadCount;

  @Option(
    name = "request-timeout",
    description = "Elapsed time in seconds after which an authentication request is canceled",
    defaultValue = "180"
  )
  protected int delayCancelTime;

  public GPLAZMA(String name, String args)
    throws InterruptedException, ExecutionException
  {
    super(name, GPLAZMA.class.getSimpleName(), new Args(args));

    doInit();
  }

  protected void init()
    throws IOException
  {
    System.setProperty("dmg.cells.nucleus.send_session", "true");

    authzPersistenceManager =
      new AuthRecordPersistenceManager(jdbcUrl, jdbcDriver, dbUser, dbPass);

    say(this.toString() + " starting with policy file " + gplazmaPolicyFilePath);
    tooLong = 1000 * delayCancelTime;

    authpool =
      new ThreadPoolTimedExecutor(threadCount,
                                  threadCount,
                                  60,
                                  TimeUnit.SECONDS,
                                  new LinkedBlockingQueue());

    delaychecker = Executors.newScheduledThreadPool(threadCount);

    say(this.toString() + " started");
  }

  public final static String hh_get_mapping =
    "\"<DN>\" [\"FQAN1\",...,\"FQANn\"]";
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

      Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths =  authorize(principal, roles, null, 0);
      StringBuilder sb = new StringBuilder();
      for( NameRolePair nameAndRole : user_auths.keySet()) {
          sb.append(nameAndRole.toString()).append(" mapped as: ");
          gPlazmaAuthorizationRecord record = user_auths.get(nameAndRole);
          if(record!=null) {
                sb.append(record.toShortString());
            } else {
                sb.append("null");
            }
          sb.append("\n");
      }

      return sb.toString();
  }


  /**
   * is called if user types 'info'
   */
  @Override
  public void getInfo( PrintWriter pw ){
    super.getInfo(pw);
    pw.println("GPLAZMA");
  }

  /**
   * This method is called from finalize() in the CellAdapter
   * super Class to clean the actions made from this Cell.
   * It stops the Thread created.
   */
  @Override
  public void cleanUp(){

    log.debug(" Clean up called ... " ) ;
    synchronized( this ){
      notifyAll() ;
    }
    if(authpool != null) {
        authpool.shutdownNow();
    }
    if(delaychecker != null) {
        delaychecker.shutdownNow();
    }

    log.debug( "Cleanup done" ) ;
  }

  /**
   * This method is invoked when a message arrives to this Cell.
   * The message is placed on the queue of the thread pool.
   * The sender of the message should block, waiting for the response.
   * @param msg CellMessage
   */
  @Override
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
        ss.setSoTimeout(delayCancelTime*1000);
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
      log.debug("sending message requesting delegation " + msg.getUOID());
      msg.revertDirection() ;
      msg.setMessageObject(cred_request) ;
      try{
        sendMessage(msg) ;
      } catch ( Exception ioe ){
        log.error("Can't send authorization message to " + msg.getSourcePath() + ":  " + ioe.getMessage());
      }

      java.net.Socket deleg_socket=null;
      try{
        log.debug("waiting for delegation connection");
        //timeout after delayCancelTime seconds if credentials not delegated
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
        context = SslGsiSocketFactory.getServiceContext(SERVICE_CERT, SERVICE_KEY, SERVICE_TRUSTED_CERTS);
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
        log.error(t.toString());
        // we do not propogate this exception since some exceptions
        // we catch are not serializable!!!
        //throw new Exception(t.toString());
        if(gsiSocket!=null) {try {gsiSocket.close();} catch(IOException e1) {}}
        return;
      }


        Object writethis;
        log.debug("gPlazma trying authorization");
        try {
            Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authorize(context, deleginfo.getUser(), authRequestID);
            writethis = new AuthenticationMessage(user_auths, deleginfo.getId());
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

    @Override
    public void run() {
      CDC.clearMessageContext();
      CDC.setMessageContext(msg);

      if(skip_processing) {
        returnNullMessage();
        return;
      }

      Object msgobj = msg.getMessageObject();
      if( ! ( msgobj instanceof DNInfo ) ) {
        log.error("message object is not DNInfo");
        return;
      }

      DNInfo dnInfo = (DNInfo) msgobj;
      String subjectDN = dnInfo.getDN();
      List<String> roles = dnInfo.getFQANs();
      long authRequestID = dnInfo.getId();

      log.debug("gPlazma trying authorization");
      Object writethis;
        try {
            Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authorize(subjectDN, roles, dnInfo.getUser(), authRequestID);
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
        log.error("Can't send authorization message to " + msg.getSourcePath() + ":  " + ioe.getMessage());
      }

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    @Override
    public void returnNullMessage() {
      long authRequestID = 0;

      Object msgobj = msg.getMessageObject();
      if( ! ( msgobj instanceof DNInfo ) ) {
        log.error("message object is not DNInfo" );
      } else {
        DNInfo dnInfo = (DNInfo) msgobj;
        authRequestID = dnInfo.getId();
      }

      log.debug("sending authorization message to " + msg.getSourcePath());
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

    @Override
    public void run() {
      //NDC.inherit(parent_stack);
      CDC.clearMessageContext();
      CDC.setMessageContext(msg);
      if(skip_processing) {
        returnNullMessage();
        return;
      }

      Object msgobj = msg.getMessageObject();
      if( ! ( msgobj instanceof diskCacheV111.vehicles.X509Info ) ) {
        log.error("message object is not X509Info" );
        return;
      }

      diskCacheV111.vehicles.X509Info x509info = (diskCacheV111.vehicles.X509Info) msgobj;
      X509Certificate[] chain = x509info.getChain();
      long authRequestID = x509info.getId();

      log.debug("gPlazma trying authorization");
      Object writethis;
      try {
        Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authorize(chain, x509info.getUser(), authRequestID);
          writethis = new X509AuthenticationMessage(user_auths, x509info);
          AuthenticationMessage authnm = new AuthenticationMessage(user_auths, authRequestID);
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
              log.error(e.toString());
          }
          if (r==null) {
            log.debug("auth object not found in database, persisting ");
            authzPersistenceManager.persist(authrec);
          }

      } catch (Exception ae) {
        log.error(ae.toString());
        writethis = ae;
      }

      log.debug("sending authorization message to " + msg.getSourcePath());
      msg.revertDirection() ;
      msg.setMessageObject(writethis) ;
      try{
        sendMessage(msg) ;
        log.debug("message is sent");
      } catch ( Exception ioe ){
        log.error("Can't send authorization message to " + msg.getSourcePath() + ":  " + ioe.getMessage()) ;
      }

    }

    /**
     * When skip_processing has been set true, this method causes a null object to be
     * returned to the calling cell.
     */
    @Override
    public void returnNullMessage() {
      long authRequestID = 0;

      Object msgobj = msg.getMessageObject();
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
    @Override
    public void beforeExecute(Thread t, Runnable r) {
      if(r instanceof TimedFuture) {
        TimedFuture timedtask = (TimedFuture) r;
        long now = System.currentTimeMillis();
        long then = timedtask.getCreateTime();
        long timeleft = tooLong - (now - then);

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
    @Override
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
  public  Map <NameRolePair, gPlazmaAuthorizationRecord>
      authorize(String subjectDN, List<String> roles, String desiredUserName, long authRequestID) throws AuthorizationException {

      AuthorizationController authServ;

      Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths;
      String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
      log.debug("gPlazma to authorize");

      try {
        authServ = new AuthorizationController(gplazmaPolicyFilePath, authRequestID);
      }
      catch( Exception e ) {
        throw new AuthorizationException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
      }

      if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
        log.debug("gPlazma: special case , user is " + _user);
        try {
          user_auths = authServ.authorize(subjectDN, roles, null, null, null, null);
        } catch ( AuthorizationException ase ) {
          throw ase;
        } catch ( Exception e ) {
          throw new AuthorizationException("exception: " + e);
        }

        if(user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      } else {
        try {
          user_auths = authServ.authorize(subjectDN, roles, null, _user, null, null);
        } catch ( AuthorizationException ase ) {
          throw ase;
        }
        if(user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      }

      if(log.isInfoEnabled()) {
          AuthzQueryHelper.logAuthzMessage(user_auths, log);
      }

    return user_auths;
  }

  /**
   * Makes an authorization callout based on the credentials delegated to the service context.
   * @param chain
   * @param desiredUserName
   * @param authRequestID
   * @return
   */
  public Map <NameRolePair, gPlazmaAuthorizationRecord> authorize(X509Certificate[] chain, String desiredUserName, long authRequestID) throws AuthorizationException {

    AuthorizationController authServ;

    Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths;
    String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
    log.debug("gPlazma to authorize using X509Certificate chain");

    try {
      authServ = new AuthorizationController(gplazmaPolicyFilePath, authRequestID);
    }
    catch( Exception e ) {
      throw new AuthorizationException("authRequestID " + authRequestID + " Authorization service failed to initialize: " + e);
    }

    if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
      log.debug("gPlazma: special case , user is " + _user);
      try {
        user_auths = authServ.authorize(chain, null, null, null);
      } catch ( AuthorizationException ase ) {
        throw new AuthorizationException("caught exception: " + ase.getMessage());
      } catch ( Exception e ) {
        throw new AuthorizationException("caught exception: " + e.getMessage());
      }

      if(user_auths.isEmpty()) {
        log.error("could not authorize: Permission denied");
        return null;
      }
    } else {
      try {
        user_auths = authServ.authorize(chain, _user, null, null);
      } catch ( AuthorizationException ase ) {
        throw ase;
      }
      if(user_auths.isEmpty()) {
        log.error("could not authorize: Permission denied");
        return null;
      }
    }

      if(log.isInfoEnabled()) {
          AuthzQueryHelper.logAuthzMessage(user_auths, log);
      }
    return user_auths;
  }

  /**
   * Makes an authorization callout based on the credentials delegated to the service context.
   * @param serviceContext
   * @param desiredUserName
   * @param authRequestID
   * @return
   */
  public Map <NameRolePair, gPlazmaAuthorizationRecord> authorize(GSSContext serviceContext, String desiredUserName, long authRequestID) throws AuthorizationException {

      GSSName GSSIdentity = null;
      AuthorizationController authServ;

      Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths;
      String _user = (desiredUserName==null) ? GLOBUS_URL_COPY_DEFAULT_USER : desiredUserName;
      log.debug("gPlazma to authorize");

      try {
        GSSIdentity = serviceContext.getSrcName();
      } catch( Exception e ) {
        throw new AuthorizationException("Authorization service context name not defined: " + e);
      }

      try {
        authServ = new AuthorizationController(gplazmaPolicyFilePath, authRequestID);
      }
      catch( Exception e ) {
        throw new AuthorizationException("Authorization service failed to initialize: " + e);
      }

      if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
        log.debug("gPlazma: special case , user is " + _user);
        try {
          user_auths = authServ.authorize(serviceContext, null, null, null);
        } //catch ( GSSException gsse ) {
          //throw new AuthorizationException("authRequestID " + authRequestID + " could not determine dn: " + gsse);
        //}
          catch ( AuthorizationException ase ) {
          throw ase;
        } catch ( Exception e ) {
          throw new AuthorizationException("exception: " + e);
        }

        if(user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      } else {
        try {
          user_auths = authServ.authorize(serviceContext, _user, null, null);
        } catch ( AuthorizationException ase ) {
          throw ase;
        }
        if(user_auths == null) {
          log.error("could not authorize: Permission denied");
          return null;
        }
      }

      if(log.isInfoEnabled()) {
          AuthzQueryHelper.logAuthzMessage(user_auths, log);
      }

    return user_auths;
  }
}
