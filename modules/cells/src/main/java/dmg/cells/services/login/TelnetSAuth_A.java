package  dmg.cells.services.login ;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.FutureCellMessageAnswerable;
import dmg.protocols.telnet.TelnetServerAuthentication;

import org.dcache.util.Args;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class      TelnetSAuth_A
       implements TelnetServerAuthentication  {

  private static final Logger _log =
      LoggerFactory.getLogger(TelnetSAuth_A.class);

  private CellEndpoint _endpoint;
  private Args         _args ;
  private String       _password ;
  private boolean      _localOk ;
  private String       _acmCell ;
  private static UnixPassword __passwordFile;
  /**
  */
  public TelnetSAuth_A(CellEndpoint endpoint, Args args) throws Exception {
      _endpoint  = endpoint;
      _args      = args ;
      _password  = args.getOpt("passwd") ;
      _password  = _password == null ? "elch" : _password ;
      _localOk   = args.hasOption("localOk") ;
      _acmCell   = args.getOpt("acm") ;

      String pwdFile = args.getOpt( "pswdfile" ) ;
      synchronized (TelnetSAuth_A.class) {
         if( ( __passwordFile == null ) && ( pwdFile != null ) ){
            __passwordFile = new UnixPassword(pwdFile) ;
         }
      }
  }
  //
  // ssh server authetication
  //
   @Override
   public boolean isHostOk( InetAddress host ){
      return _localOk ;
   }
   @Override
   public boolean isUserOk( InetAddress host , String user ){
      return false ;
   }

    private Object askAcm(Object... request) throws Exception
    {
        FutureCellMessageAnswerable reply = new FutureCellMessageAnswerable();
        _endpoint.sendMessage(new CellMessage(new CellPath(_acmCell), request),
                              reply, MoreExecutors.directExecutor(), 4000);

        CellMessage answerMsg;
        try {
            answerMsg = reply.get();
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), Exception.class);
            throw Throwables.propagate(e.getCause());
        }

        Serializable answer = answerMsg.getMessageObject();
        if (answer instanceof Exception) {
            throw (Exception) answer;
        }
        return answer;
    }

   private boolean checkPasswd( String user , String passwd )
           throws Exception {

       Object answer = askAcm("request", "*", "check-password", user, passwd);
       if( ( ! ( answer instanceof Object [] )  ) ||
           (   ((Object[])answer).length < 6    ) ||
           ( ! (((Object[])answer)[5] instanceof Boolean ) ) ) {
           throw new Exception("Wrong formated answer");
       }

       return (Boolean) ((Object[]) answer)[5];
   }

    private boolean checkAcl( String user ,
                             String action ,
                             String className ,
                             String instanceName )
           throws Exception {

       Object answer = askAcm("request", "*", "check-acl", user, action, className, instanceName);

       if( ( ! ( answer instanceof Object [] )  ) ||
           (   ((Object[])answer).length < 8    ) ||
           ( ! (((Object[])answer)[7] instanceof Boolean ) ) ) {
           throw new Exception("Wrong formated answer");
       }

       return (Boolean) ((Object[]) answer)[7];
   }
   @Override
   public boolean isPasswordOk( InetAddress host , String user , String passwd ){
      if( _acmCell != null ){
         try{

             if( ! checkPasswd( user , passwd ) ) {
                 throw new Exception("Not authenticated");
             }
             if( ! checkAcl( user , "exec" , "shell" , "*" ) ) {
                 throw new Exception("Not authorized");
             }
             return true ;
         }catch( Exception e ){
            _log.info( "Exception in TelnetSAuth_A : "+ e ) ;
            return false ;
         }
      }else if( __passwordFile !=  null ){
         return __passwordFile.checkPassword( user , passwd ) ;
      }else{
         return passwd.equals(_password) ;
      }
   }
}

