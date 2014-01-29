package dmg.cells.services.login.user  ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.security.digest.Crypt;

import org.dcache.util.Args;
import dmg.util.Authorizable;
import dmg.util.UserPasswords;

import static java.util.Arrays.asList;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       AclCell
       extends     CellAdapter            {

  private final static Logger _log =
      LoggerFactory.getLogger(AclCell.class);

  private String       _cellName ;
  private CellNucleus  _nucleus ;
  private Args         _args ;

  private AclDb            _aclDb;
  private UserRelationable _userDb;
  private UserMetaDb       _userMetaDb;

  private UserPasswords    _sysPassword;
  private UserPasswords    _egPassword;
  private Crypt            _crypt        = new Crypt() ;
  public AclCell( String name , String argString ) throws Throwable {

      super( name , argString , false ) ;

      _cellName  = name ;
      _args      = getArgs() ;


      try{

         if( _args.argc() < 1 ) {
             throw new
                     IllegalArgumentException("Usage : ... <dbPath>");
         }

         File dbBase   = new File( _args.argv(0) ) ;
           _aclDb      = new AclDb(
                              new File( dbBase , "acls" ) ) ;
           _userDb     = new InMemoryUserRelation(
                              new FileUserRelation(
                                  new File( dbBase , "relations" )
                                                   )
                          ) ;
           _userMetaDb = new UserMetaDb(
                              new File( dbBase , "meta" ) ) ;

          UserAdminCommands uac = new UserAdminCommands( _userDb , _aclDb , _userMetaDb ) ;
          addCommandListener( uac ) ;
          setCommandExceptionEnabled( true ) ;
          //
          // read the password file information
          //
          String tmp;
          if( ( tmp = _args.getOpt( "syspassword" ) ) != null ){
             _sysPassword = new UserPasswords( new File( tmp ) ) ;
             _log.info( "using as SystemPasswordfile : "+tmp ) ;
          }
          if( ( tmp = _args.getOpt( "egpassword"  ) ) != null ){
             _egPassword  = new UserPasswords( new File( tmp ) ) ;
             _log.info( "using as EgPasswordfile : "+tmp ) ;
          }
      }catch( Throwable e ){
         _log.warn( "Exception while <init> : "+e, e ) ;
         start() ;
         kill() ;
         throw e ;
      }

      start() ;

  }
  //
  // for now we also serve the password checking request
  //
  @Override
  public void messageArrived( CellMessage msg ){
      Serializable obj     = msg.getMessageObject() ;
      Serializable answer;

      try{
         _log.info( "Message type : "+obj.getClass() ) ;
         if( ( obj instanceof Object []              )  &&
             (  ((Object[])obj).length >= 3          )  &&
             (  ((Object[])obj)[0].equals("request") ) ){

            Object [] request    = (Object[])obj ;
            String user          = request[1] == null ?
                                   "unknown" : (String)request[1] ;
            String command       = (String)request[2] ;

            _log.info( ">"+command+"< request from "+user ) ;
            try{
                switch (command) {
                case "check-password":
                    answer = acl_check_password(request);
                    break;
                case "check-permission":
                    answer = acl_check_permission(request);
                    break;
                case "get-metainfo":
                    answer = acl_get_metainfo(request);
                    break;
                default:
                    throw new Exception("Command not found : " + command);
                }
            }catch( Exception xe ){
               throw new Exception( "Problem : "+xe ) ;
            }
         }else{
             String r = "Illegal message object received from : "+
                         msg.getSourcePath() ;
             _log.warn( r ) ;
             throw new Exception( r ) ;
         }
      }catch(Exception iex ){
         answer = iex ;
      }

      if( answer instanceof Object [] ) {
          ((Object[]) answer)[0] = "response";
      }

      msg.revertDirection() ;
      msg.setMessageObject( answer ) ;
      try{
         sendMessage( msg ) ;
      }catch( Exception ioe ){
         _log.warn( "Can't send acl_response : "+ioe, ioe ) ;
      }
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <requestor>
  //  r[2] : get-metainfo
  //  r[3] : <user>
  //  r[4] : <key>[,<key>[...]]
  //
  //  checks : nothing
  //
  //    response
  //
  //  r[0] : response
  //  r[1] : <requestor>
  //  r[2] : get-metainfo
  //  r[3] : <user>
  //  r[4] : <key>[,<key>[...]]
  //  r[5] : <valueOfKey1>
  //  r[6] : <valueOfKey2>
  //  r[7] : ...
  //
  private Serializable
          acl_get_metainfo( Object [] request )
  {

      if( ( request.length < 5 ) ||
          ( request[3] == null ) ||
          ( request[4] == null ) ) {
          throw new
                  IllegalArgumentException(
                  "Not enough or illegal arguments for 'check-password'");
      }

      String userName = request[3].toString() ;
      UserMetaDictionary dict = _userMetaDb.getDictionary(userName) ;
      if( dict == null ) {
          throw new
                  IllegalArgumentException(
                  "No such user : " + userName);
      }


      List<Object> result = new ArrayList<>(asList(request).subList(0, 5));
      StringTokenizer st = new StringTokenizer( request[4].toString() , "," ) ;
      while( st.hasMoreTokens() ){
         result.add(dict.valueOf(st.nextToken())) ;
      }
      return result.toArray(new Object[result.size()]);
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <anything>
  //  r[2] : check-password
  //  r[3] : <user>
  //  r[4] : <password>[plainText]
  //
  //  checks : nothing
  //
  //    response
  //
  //  r[0] : response
  //  r[1] : <user>
  //  r[2] : check-password
  //  r[3] : <user>
  //  r[4] : <password>[plainText]
  //  r[5] : Boolean(true/false)
  //
  private Serializable
          acl_check_password( Object [] request )
  {

      if( request.length < 5 ) {
          throw new
                  IllegalArgumentException(
                  "Not enough arguments for 'check-password'");
      }

      Object [] response = new Object[6] ;
      System.arraycopy(request, 0, response, 0, 5);
      response[1]     = request[3] ;
      String userName = (String)request[3] ;
      String password = (String)request[4] ;

      response[5] = matchPassword(userName, password);
      return response ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <anything>
  //  r[2] : check-permission
  //  r[3] : <principal>
  //  r[4] : <acl>
  //
  //  checks : nothing
  //
  //    response
  //
  //  r[0] : response
  //  r[1] : <user>
  //  r[2] : check-permission
  //  r[3] : <principal>
  //  r[4] : <acl>
  //  r[5] : Boolean(true/false)
  //
  private Serializable
          acl_check_permission( Object [] request )
  {

      if( request.length < 5 ) {
          throw new
                  IllegalArgumentException(
                  "Not enough arguments for 'check-permission'");
      }

      Object [] response = new Object[6] ;
      System.arraycopy(request, 0, response, 0, 5);
      response[1]     = request[3] ;
      String userName = (String)request[3] ;
      String acl      = (String)request[4] ;

      response[5] = checkPermission(userName, acl);
      return response ;
  }
  private boolean checkPermission( String user , String acl ) {
     if( user.equals("admin") ) {
         return true;
     }

     try{
        if( _aclDb.check( acl , user , _userDb ) ) {
            return true;
        }
     }catch(Exception ee ){}

     try{
        if( _aclDb.check( "super.access" , user , _userDb ) ) {
            return true;
        }
     }catch(Exception ee ){}

     return false ;
  }
  private static final String DUMMY_ADMIN = "5t2Hw7lNqVock"  ;
  private boolean matchPassword( String userName , String password ){

      String pswd;
      updatePassword() ;

      try{
         if( userName.equals("admin" ) ){
            if( ( _sysPassword == null ) ||
                ( ( pswd = _sysPassword.getPassword(userName) ) == null ) ){

               if( ( _egPassword == null ) ||
                   ( ( pswd = _egPassword.getPassword(userName) ) == null ) ){

                   pswd = DUMMY_ADMIN ;
               }

            }
            return _crypt.crypt( pswd , password ).equals(pswd) ;

         }else{
            //
            // the user must have been created.
            //
            UserMetaDictionary dict = _userMetaDb.getDictionary(userName) ;
            if( dict == null ) {
                return false;
            }
            //
            // check for login disabled.
            //
            String dis = dict.valueOf("login") ;
            if( ( dis != null ) && ( dis.equals("no") ) ) {
                return false;
            }

            if( ( _sysPassword == null ) ||
                ( ( pswd = _sysPassword.getPassword(userName) ) == null ) ){

               if( ( _egPassword == null ) ||
                   ( ( pswd = _egPassword.getPassword(userName) ) == null ) ){

                   return false ;
               }

            }
            return _crypt.crypt( pswd , password ).equals(pswd) ;

         }
      }catch( Throwable t ){
         _log.warn( "Found : "+t ) ;
      }
      return false ;
  }
  private void updatePassword(){
     try{
        if( _sysPassword != null ) {
            _sysPassword.update();
        }
     }catch(Exception ee ){
        _log.warn( "Updating failed : "+_sysPassword ) ;
     }
     try{
        if( _egPassword != null ) {
            _egPassword.update();
        }
     }catch(Exception ee ){
        _log.warn( "Updating failed : "+_egPassword ) ;
     }
   }

  /////////////////////////////////////////////////////////////
  //
  //   the interpreter
  //
  private void checkPermission( Args args , String acl ) throws Exception {
     if( ! ( args instanceof Authorizable ) ) {
         throw new
                 AclPermissionException("Command not authorizable");
     }
     String user = ((Authorizable)args).getAuthorizedPrincipal() ;
     if( user.equals("admin") ) {
         return;
     }
     try{
        if( _aclDb.check( "super.access" , user , _userDb ) ) {
            return;
        }
     }catch(Exception ee ){}
     if( ! _aclDb.check(acl,user,_userDb) ) {
         throw new
                 AclPermissionException("Acl >" + acl + "< negative for " + user);
     }
  }
  public String ac_interrupted( Args args )
  {
     return "\n" ;
  }
  public static final String hh_set_passwd =
         "[-user=<userName>] [-old=<oldPasswd>] newPswd verifyPswd";
  public String ac_set_passwd_$_2( Args args )throws Exception {
     if( _egPassword == null ) {
         throw new
                 AclPermissionException("No private password file found");
     }
     if( ! ( args instanceof Authorizable ) ) {
         throw new
                 AclPermissionException("Command not authorizable");
     }
     String pswd1 = args.argv(0) ;
     String pswd2 = args.argv(1) ;
     if( ! pswd1.equals( pswd2 ) ) {
         throw new
                 IllegalArgumentException("pswd1 doesn't match pswd2");
     }

     String auth  = ((Authorizable)args).getAuthorizedPrincipal() ;
     String user  = args.getOpt("user") ;
     user = user == null ? auth : user ;
     String old   = args.getOpt("old");
     String acl   = "user."+user+".setpassword" ;
     String [] record;
     if( ! ( auth.equals("admin" ) || _aclDb.check( acl , auth , _userDb ) ) ){
        if( auth.equals(user) ){
           if( old == null ) {
               throw new
                       IllegalArgumentException("-old=<oldPassword> option missing");
           }
        }else{
           throw new
           AclPermissionException( "Acl >"+acl+"< negative for "+auth ) ;
        }

        if( ( pswd2 = _egPassword.getPassword(user) ) == null  ) {
            throw new
                    IllegalArgumentException("User not found in private passwd file");
        }

        if( ! _crypt.crypt( pswd2 , old ).equals(pswd2) ) {
            throw new
                    IllegalArgumentException("Old password doesn't match");
        }
        record = _egPassword.getRecord(user) ;
        if( record == null ) {
            throw new
                    IllegalArgumentException("User " + user + " doesn't exist");
        }
     }else{
        record = _egPassword.getRecord(user) ;
        if( record == null ){
            record = new String[2] ;
            record[0] = user ;
        }


     }
     record[1] = _crypt.crypt( user.substring(0,2) , pswd1 ) ;
      _egPassword.addRecord( record ) ;
     _egPassword.commit() ;
     return "" ;

  }
}
