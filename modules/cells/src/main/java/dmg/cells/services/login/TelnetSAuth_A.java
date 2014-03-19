package  dmg.cells.services.login ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
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

  private final static Logger _log =
      LoggerFactory.getLogger(TelnetSAuth_A.class);

  private CellNucleus  _nucleus ;
  private Args         _args ;
  private String       _password ;
  private boolean      _localOk ;
  private String       _acmCell ;
  private static UnixPassword __passwordFile;
  /**
  */
  public TelnetSAuth_A( CellNucleus nucleus , Args args ) throws Exception {
      _nucleus   = nucleus ;
      _args      = args ;
      _password  = args.getOpt("passwd") ;
      _password  = _password == null ? "elch" : _password ;
      _localOk   = args.hasOption("localOk") ;
      _acmCell   = args.getOpt("acm") ;

      String pwdFile = args.getOpt( "pswdfile" ) ;
      synchronized( this.getClass() ){
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
   private boolean checkPasswd( String user , String passwd )
           throws Exception {

       Object [] request = new Object[5] ;
       request[0] = "request" ;
       request[1] = "*" ;
       request[2] = "check-password" ;
       request[3] = user ;
       request[4] = passwd ;
       CellMessage answerMsg;
       answerMsg = _nucleus.sendAndWait(
                       new CellMessage( new CellPath(_acmCell) ,
                                        request ) ,
                       4000  ) ;


       if( answerMsg == null ) {
           throw new Exception("Timeout from acm");
       }

       Object answer = answerMsg.getMessageObject() ;

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

       Object [] request = new Object[7] ;
       request[0] = "request" ;
       request[1] = "*" ;
       request[2] = "check-acl" ;
       request[3] = user ;
       request[4] = action ;
       request[5] = className  ;
       request[6] = instanceName ;
       CellMessage answerMsg;
       answerMsg = _nucleus.sendAndWait(
                       new CellMessage( new CellPath(_acmCell) ,
                                        request ) ,
                       4000  ) ;


       if( answerMsg == null ) {
           throw new Exception("Timeout from acm");
       }

       Object answer = answerMsg.getMessageObject() ;

       if( answer instanceof Exception ) {
           throw (Exception) answer;
       }

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

