package org.dcache.auth.gplazma;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.apache.log4j.*;
import org.dcache.auth.AuthzQueryHelper;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.vehicles.AuthorizationMessage;
import org.globus.gsi.CertUtil;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.Enumeration;
import java.io.PrintWriter;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;

import dmg.util.Args;
import dmg.cells.nucleus.*;
import gplazma.authz.AuthorizationController;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.util.HostUtil;
import diskCacheV111.vehicles.AuthenticationMessage;

/**
 * Created by IntelliJ IDEA.
 * User: tdh
 * Date: Oct 31, 2008
 * Time: 11:38:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class gPlazmaTester extends CellAdapter {

    static Logger log = Logger.getLogger(GPLAZMA.class.getSimpleName());

    /** Location of gss files **/
    private String service_key           = "/etc/grid-security/hostkey.pem";
    private String service_cert          = "/etc/grid-security/hostcert.pem";
    private String service_trusted_certs = "/etc/grid-security/certificates";

    /** Arguments specified in .batch file **/
    private Args _opt;

    /** Specifies logging level **/
    private Level loglevel = Level.ERROR;

    /** Whether to use the gPlazma cell for authorization **/
    protected boolean _use_gplazmaAuthzCell=true;

    /** Cell path for GPLAZMA **/
    protected CellPath gPlazmaCellPath = new CellPath("gPlazma");

    /** Number of authorizations tested **/
    private long numtests=0;

    /** Average time needed for a test authorization **/
    private float AveTime=0;

    /** Context for testing. If "-proxy-certificate" is set in batch file, will use proxy.
     *  Otherwise, will use host certificates.
     * **/
    GSSContext test_context=null;
    X509Certificate[] test_certificate_chain=null;

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
    public gPlazmaTester( String name , String args )  throws Exception {

        super( name , args , false ) ;

        //useInterpreter( true ) ;
        //addCommandListener(this);

        _opt = getArgs() ;

        try{

            /**
             *  USAGE :
             *              -log-level=LOG_LEVEL
             *              -num-simultaneous-requests=THREAD_COUNT
             *              -request-timeout=DELAY_CANCEL_TIME
             *
             */

            System.setProperty("dmg.cells.nucleus.send_session", "true");
            String level = _opt.getOpt("log-level") ;
            if(level==null || level.length()==0) {
              if((getNucleus().getPrintoutLevel() & CellNucleus.PRINT_CELL) ==0)
                ac_set_LogLevel_$_1(new Args(new String[]{"WARN"}));
              else
                ac_set_LogLevel_$_1(new Args(new String[]{"DEBUG"}));
            } else {
              ac_set_LogLevel_$_1(new Args(new String[]{level}));
            }

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
     * For testing, run several threads which make repeated authorization requests to another GPLAZMA cell.
     **/
    public void run_test_authorizations(){

        say("Start testing");

        test_dn = setParam("test-fqan", "").replace('*', ' ');
        test_role = setParam("test-role", "").replace('*', ' ');

        if(test_dn.equals("")) {
            String test_cert = setParam("proxy-certificate", "host-proxy");
            try {
                if(test_cert.equals("host-proxy")) {
                    test_context = HostUtil.getServiceContext();
                    test_certificate_chain = null;//loadCertificates(String file)
                } else {
                    test_context = X509CertUtil.getUserContext(test_cert);
                    test_certificate_chain = CertUtil.loadCertificates(test_cert);
                }
            } catch (IOException ioe) {
                log.error("could not read cert file " + test_cert);
            } catch (GSSException gce) {
                log.error("could not load host globus credentials " + gce.toString());
            } catch (GeneralSecurityException gse) {
                log.error("could not load host globus credentials " + gse.toString());
            }
        }

        int TEST_THREADS = setParam("max-simultaneous-tests", 1);
        //ExecutorService testerpool = Executors.newFixedThreadPool(TEST_THREADS);
        ArrayBlockingQueue testqueue = new ArrayBlockingQueue(TEST_THREADS+1);
        ExecutorService testerpool =
                new ThreadPoolExecutor(  TEST_THREADS,
                        TEST_THREADS,
                        60,
                        TimeUnit.MILLISECONDS,
                        testqueue);

        int MAXTRIES=setParam("max-total-tests", 100);
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
            CDC.createSession();
            AuthorizationRecord authRecord=null;

            if(test_dn.equals(""))
                authRecord = authorize(test_certificate_chain);
            else
                authRecord = authorize(test_dn, test_role);

            if(authRecord!=null) averageTime(authRecord.getUid());

            try { Thread.sleep(1000); } catch (Exception e) {}
        }
    }


    public synchronized float averageTime(int thistime) {
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
     * For testing, makes authentication requests to an instance of the GPLAZMA cell.
     */
    public AuthorizationRecord authorize(GSSContext test_context) {
        AuthorizationRecord authRecord=null;
        if (_use_gplazmaAuthzCell) {
            AuthzQueryHelper authHelper;
            try {
                authHelper = new AuthzQueryHelper(this);
                //authHelper.setDelegateToGplazma(_delegate_to_gplazma);
                log.info("Requesting authorization with context call");
                authRecord =  authHelper.getAuthorization(test_context, new CellPath("gPlazma"), this).getAuthorizationRecord();
            } catch( Exception e ) {
                log.error("authorization through gPlazma cell failed: " + e.getMessage());
                authRecord = null;
            }
        }

        return authRecord;
    }

    /**
     * For testing, makes authentication requests to an instance of the GPLAZMA cell.
     */
    public AuthorizationRecord authorize(X509Certificate[] chain) {
        AuthorizationRecord authRecord=null;
        if (_use_gplazmaAuthzCell) {
            AuthzQueryHelper authHelper;
            try {
                authHelper = new AuthzQueryHelper(this);
                //authHelper.setDelegateToGplazma(_delegate_to_gplazma);
                log.info(CDC.getSession().toString() + " Requesting authorization with certificate chain call");
                AuthenticationMessage authmessage = authHelper.authorize(chain, null, gPlazmaCellPath, this);
                authRecord =  new AuthorizationMessage(authmessage).getAuthorizationRecord();
            } catch( Exception e ) {
                log.error("authorization through gPlazma cell failed: " + e.getMessage());
                authRecord = null;
            }
        }

        return authRecord;
    }

    /**
     * For testing, makes authentication requests to an instance of the GPLAZMA cell.
     */
    public AuthorizationRecord authorize(String test_dn, String test_role) {
        AuthorizationRecord authRecord=null;
        if (_use_gplazmaAuthzCell) {
            AuthzQueryHelper authHelper;
            try {
                authHelper = new AuthzQueryHelper(this);
                //authHelper.setDelegateToGplazma(_delegate_to_gplazma);
                log.info("Requesting authorization with dn and role call");
                //authRecord =  authHelper.getAuthorization(test_dn, test_role, new CellPath("gPlazma"), this).getAuthorizationRecord();
            } catch( Exception e ) {
                log.error("authorization through gPlazma cell failed: " + e.getMessage());
                authRecord = null;
            }
        }

        return authRecord;
    }

    /** Set a parameter according to option specified in .batch config file **/
    private String setParam(String name, String target) {
        if(target==null) target = new String();
        String option = _opt.getOpt(name) ;
        if((option != null) && (option.length()>0)) target = option;
        say("Using " + name + " : " + target);
        return target;
    }

    /** Set a parameter according to option specified in .batch config file **/
    private int setParam(String name, int target) {
        String option = _opt.getOpt(name) ;
        if( ( option != null ) && ( option.length() > 0 ) ) {
            try{ target = Integer.parseInt(option); } catch(Exception e) {}
        }
        say("Using " + name + " : " + target);
        return target;
    }

    /** Set a parameter according to option specified in .batch config file **/
    private long setParam(String name, long target) {
        String option = _opt.getOpt(name) ;
        if( ( option != null ) && ( option.length() > 0 ) ) {
            try{ target = Integer.parseInt(option); } catch(Exception e) {}
        }
        say("Using " + name + " : " + target);
        return target;
    }

    /**
     * is called if user types 'info'
     */
    public void getInfo( PrintWriter pw ){
        super.getInfo(pw);
        pw.println("GPLAZMA");
        //pw.println(" admin_email :  " + _admin_email);
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
            newlevel.equals("FATAL")  ) {
          log.setLevel(Level.toLevel(newlevel.toUpperCase()));
          try {
              sendMessage(new CellMessage(new CellPath("System"), "log4j appender set stdout " + newlevel), true, false);
          } catch (NoRouteToCellException nre) {
              log.warn("Message to change appender log level failed: NoRouteToCell for " + "System@" + getCellDomainName());
          }
        }
        else
          return "Log level not set. Allowed values are DEBUG, INFO, WARN, ERROR.";

      return "Log level set to " + log.getLevel();
    }


}
