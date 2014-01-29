package diskCacheV111.admin ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.security.digest.Crypt;
import dmg.util.AgingHash;
import dmg.util.UserPasswords;

import org.dcache.util.Args;


/**
  * PAM Authentification Cell.
  *
  */



public class PAMAuthentificator  extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(PAMAuthentificator.class);

   private final int request_len  = 5;
   private final int response_len = 6;

   private final CellNucleus _nucleus;
   private final String      _cellName;
   private String      _service;
   private PAM_Auth    _pam;
   private ExecAuth    _execAuth;

   private Args   _args;
   private Date   _started       = new Date() ;

   private int    _requestCount;
   private int    _badRequestCount;
   private int    _failedRequestCount;

   private static final int USER_SERVICE_FILE  = 1 ;
   private static final int USER_SERVICE_NIS   = 2 ;
   private static final int USER_SERVICE_LDAP  = 3 ;
   private static final int USER_SERVICE_CLASS = 4 ;

   private UserPasswords    _sysPassword;
   private UserPasswords    _egPassword;

   private UserMetaDataProvider _userServiceProvider;
   private DirContext           _userServiceNIS;
   private UserPasswords        _userServiceFile;
   private int                  _userServiceType;

   private final Crypt _crypt  = new Crypt() ;


   public PAMAuthentificator( String cellName , String args ) throws Exception {

      super( cellName , PAMAuthentificator.class.getName(), args , false );

      _nucleus  = getNucleus() ;
      _cellName = cellName;
      _args     = getArgs() ;

      useInterpreter( true ) ;

      try {

         // Usage: ... [-service=<login_service>]
         //            [-syspassword=</etc/password>]
         //            [-dcachepassword=<dCachePasswordFile>]
         //            [-users=[file:]<passwordFilePath>|nis:<nisserver>|ldap:<ldapserver>]
         //            [-provider=<nisProviderClass>]
         //            [-external=<binToCheckPam>]
         //            [-usepam]
         //

         _service = _args.getOpt( "service" ) ;
         if( _service == null ){
            _service = "dcache";
            _log.info("'service' not defined. Using '"+_service+"' as default service");
         }


         String tmp = _args.getOpt("external") ;
         if( tmp == null ){

             if( ( tmp = _args.getOpt( "syspassword" ) ) != null ){
                _sysPassword = new UserPasswords( new File( tmp ) ) ;
                _log.info( "using as SystemPasswordfile : "+tmp ) ;
            }
            if( ( tmp = _args.getOpt( "dcachepassword"  ) ) != null ){
                _egPassword  = new UserPasswords( new File( tmp ) ) ;
                _log.info( "using as dCachePassword : "+tmp ) ;
            }

             if( ( tmp = _args.getOpt("usepam") ) != null ){
                _pam = new PAM_Auth( _service );
                _log.info( "using PAM mudule") ;
             }
         }else{
            _execAuth = new ExecAuth(tmp) ;
         }



         if( ( tmp = _args.getOpt( "users"  ) ) != null ){
            _log.info( "using as userService : "+tmp ) ;

            if( tmp.startsWith("nis:") ){

               String provider = _args.getOpt("provider") ;
               provider = provider == null ? "com.sun.jndi.nis.NISCtxFactory" : provider ;
               Hashtable<String, String> env = new Hashtable<>();
               env.put(Context.INITIAL_CONTEXT_FACTORY, provider );
               // String url = tmp.substring(tmp.indexOf(":")+1);
               // url = "nis://nisserv6.desy.de/desy.afs" ;
               env.put(Context.PROVIDER_URL, tmp );
               try{
                   _userServiceNIS = new InitialDirContext(env);
               }catch( NamingException ne ){
                   _log.warn("Can't InitialDirContext(env) "+ne ) ;
                   throw ne ;
               }
               _userServiceType = USER_SERVICE_NIS ;
            }else if( tmp.startsWith("ldap:") ){
               throw new
               IllegalArgumentException("LDap not yet supported" ) ;

            }else if( tmp.startsWith("file:") ){
               _userServiceFile = new UserPasswords( new File( tmp.substring(5) ) ) ;
               _userServiceType = USER_SERVICE_FILE ;
            }else if( tmp.startsWith("class:") ){
               _userServiceProvider = initUserServiceProvider( tmp.substring(6) ) ;
               _userServiceType   = USER_SERVICE_CLASS ;
            }else if( (!tmp.contains(":")) ){
               _userServiceFile = new UserPasswords( new File( tmp ) ) ;
               _userServiceType = USER_SERVICE_FILE ;
            }else {
                throw new
                        IllegalArgumentException("Invalid user service provider : " + tmp);
            }

         }


      }catch(Exception e) {
         start();
         kill();
         throw e;
      }
      export();
      start() ;
   }
   private UserMetaDataProvider initUserServiceProvider( String className )
      throws Exception {

      Class<?>[] argClasses = { CellAdapter.class } ;
      Object[] argObjects = { this } ;

      Class<? extends UserMetaDataProvider> exec = Class.forName(className).asSubclass(UserMetaDataProvider.class);

      Constructor<? extends UserMetaDataProvider> constructor = exec.getConstructor(argClasses) ;

      UserMetaDataProvider provider = constructor.newInstance(argObjects);

      addCommandListener(provider);

      return provider ;

   }
   @Override
   public void getInfo( PrintWriter pw ){
    try{
      pw.println("PAMAuthentificator");
      pw.println("        Request Count : "+_requestCount ) ;
      pw.println("    Bad Request Count : "+_badRequestCount ) ;
      pw.println(" Failed Request Count : "+_failedRequestCount ) ;
      pw.println("           PAM loaded : "+(_pam==null?"Not requested":(""+_pam.pamOk())) ) ;
      pw.println(" System Password File : "+
                 (_sysPassword==null?"<null>":_sysPassword.toString()) ) ;
      pw.println(" dCache Password File : "+
                 (_egPassword==null?"<null>":_egPassword.toString() )) ;
      switch( _userServiceType ){
        case USER_SERVICE_FILE :
           pw.println(" user metadata   File : "+
                      (_userServiceFile==null?"<null>":_userServiceFile.toString()) ) ;
        break ;
        case USER_SERVICE_NIS :
           pw.println(" user metadata   Nis  : "+_userServiceNIS ) ;
        break ;
        case USER_SERVICE_LDAP :
           pw.println(" user metadata   LDAP : "+_userServiceNIS ) ;
        break ;
        case USER_SERVICE_CLASS :
           pw.println(" user metadata   Class  : "+_userServiceProvider.getClass().getName() ) ;
           pw.println(" user metadata   Object : "+_userServiceProvider ) ;
        break ;
      }
    }catch(Exception eee){
       eee.printStackTrace() ;
    }
   }
  private static final String DUMMY_ADMIN = "5t2Hw7lNqVock"  ;
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
   private boolean authenticate( String principal , String password ){
      password = password.trim() ;
      if( _pam != null ){
         return _pam.authenticate( principal , password ) ;
      }else if( _execAuth != null ){
         try{
            String result =
               _execAuth.command( "check "+_service+
                                  " "+principal+
                                  " "+password       ) ;
            return result.equals("true") ;
         }catch(Exception ee ){
            _log.warn(ee.toString(), ee) ;
            return false ;
         }
      }else{
         return false ;
      }
   }
   private boolean checkAccess( String principal , String password ){
      boolean pamOk = false ;
      try{
         pamOk = authenticate( principal , password )  ;
      }catch(Exception ee ){
         _log.warn( "_pam.authorize : "+ee ) ;
      }
      if( ! pamOk ){
         _log.info("pam _log.infos no to <"+principal+"> (switching to local)");
         try{
            return matchPassword( principal , password ) ;
         }catch(Exception ee ){
            _log.warn( "matchPassword : "+ee ) ;
         }
      }
      return pamOk ;
   }
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
                case "get-metainfo":
                    answer = getMetaInfo(request);
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
  private Serializable getMetaInfo( Object [] request )throws Exception {
     return _userServiceType == USER_SERVICE_FILE ?
             acl_get_metainfo( request ) :
             _userServiceType == USER_SERVICE_NIS ?
             acl_get_metainfo_nis( request ) :
             _userServiceType == USER_SERVICE_CLASS ?
             acl_get_metainfo_class( request ) :
             new IllegalArgumentException("Panix "+_userServiceType) ;
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
          throws Exception {

      if( ( request.length < 5 ) ||
          ( request[3] == null ) ||
          ( request[4] == null ) ) {
          throw new
                  IllegalArgumentException(
                  "Not enough or illegal arguments for 'get-metainfo'");
      }

      String userName = request[3].toString() ;

      if( _userServiceFile == null ) {
          throw new
                  IllegalArgumentException("User service not configured");
      }
      _userServiceFile.update() ;
      String [] dict = _userServiceFile.getRecord(userName) ;
      if( dict == null ) {
          throw new
                  IllegalArgumentException(
                  "No such user : " + userName);
      }


      StringTokenizer st = new StringTokenizer( request[4].toString() , "," ) ;
      List<String> result = new ArrayList<>() ;
      while( st.hasMoreTokens() ){
         String key = st.nextToken() ;
          switch (key) {
          case "uid":
              result.add(dict.length > 2 ? dict[2] : null);
              break;
          case "gid":
              result.add(dict.length > 3 ? dict[3] : null);
              break;
          case "home":
              result.add(dict.length > 5 ? dict[5] : null);
              break;
          case "fqn":
              result.add(dict.length > 4 ? dict[4] : null);
              break;
          case "shell":
              result.add(dict.length > 6 ? dict[6] : null);
              break;
          default:
              result.add(null);
              break;
          }
      }
      String [] r = new String[5+result.size()] ;
      for( int i = 0 ; i < 5 ; i++ ) {
          r[i] = (String) request[i];
      }
      for( int i = 5 ; i < r.length ; i++ ) {
          r[i] = result.get(i - 5);
      }

      return r ;
  }
  private Serializable
          acl_get_metainfo_class( Object [] request )
          throws Exception {

      if( ( request.length < 5 ) ||
          ( request[3] == null ) ||
          ( request[4] == null ) ) {
          throw new
                  IllegalArgumentException(
                  "Not enough or illegal arguments for 'get-metainfo'");
      }

      String userName = request[3].toString() ;
      String principal = request[1].toString() ;        //VP

      if( _userServiceProvider == null ) {
          throw new
                  IllegalArgumentException("User service not configured");
      }

      List<String>      attrList = new ArrayList<>() ;
      StringTokenizer st = new StringTokenizer( request[4].toString() , "," ) ;
      while( st.hasMoreTokens() ) {
          attrList.add(st.nextToken());
      }

//VP  Map map = _userServiceProvider.getUserMetaData( userName , attrList ) ;
      Map<String,String> map = _userServiceProvider.getUserMetaData( principal, userName , attrList ) ;

      String [] r = new String[5+attrList.size()] ;
      for( int i = 0 ; i < 5 ; i++ ) {
          r[i] = (String) request[i];
      }
      for( int i = 5 ; i < r.length ; i++ ){
          String t = map.get(attrList.get(i-5));
          r[i] = t == null ? "Unknown" : t ;
      }

      return r ;
  }
  private static final long HASH_REFRESH = 4*3600*1000 ;
  private AgingHash _map = new AgingHash(400) ;
  private class UserRecord {
     private Attributes _userRecord;
     private long       _timestamp;
     private UserRecord( Attributes userRecord ){
        _userRecord = userRecord ;
        _timestamp  = System.currentTimeMillis() ;
     }
  }
  private Serializable
          acl_get_metainfo_nis( Object [] request )
          throws Exception {

  try{
      if( ( request.length < 5 ) ||
          ( request[3] == null ) ||
          ( request[4] == null ) ) {
          throw new
                  IllegalArgumentException(
                  "Not enough or illegal arguments for 'check-password'");
      }

      String userName = request[3].toString() ;

      if( _userServiceNIS == null ) {
          throw new
                  IllegalArgumentException("User 'nis' service not configured");
      }

      UserRecord record = (UserRecord)_map.get(userName);
      Attributes answer;
      if( ( record == null ) ||
          ( ( record._timestamp != 0 ) &&
            ( System.currentTimeMillis() - record._timestamp ) >  HASH_REFRESH ) ){

           answer = _userServiceNIS.getAttributes("system/passwd/"+userName);
           if( answer.size() == 0 ) {
               throw new
                       IllegalArgumentException("No such user : " + userName);
           }

          _map.put( userName , new UserRecord(answer) ) ;

      }else{
          answer = record._userRecord ;
      }


      StringTokenizer st = new StringTokenizer( request[4].toString() , "," ) ;
      List<Object> result = new ArrayList<>();
      while( st.hasMoreTokens() ){
         String key = st.nextToken() ;
          switch (key) {
          case "uid":
              try {
                  result.add(answer.get("uidNumber").get());
              } catch (Exception e) {
                  result.add(null);
              }
              break;
          case "gid":
              try {
                  result.add(answer.get("gidNumber").get());
              } catch (Exception e) {
                  result.add(null);
              }
              break;
          case "home":
              try {
                  result.add(answer.get("homeDirectory").get());
              } catch (Exception e) {
                  result.add(null);
              }
              break;
          case "fqn":
              try {
                  result.add(answer.get("gecos").get());
              } catch (Exception e) {
                  result.add(null);
              }
              break;
          case "shell":
              try {
                  result.add(answer.get("loginShell").get());
              } catch (Exception e) {
                  result.add(null);
              }
              break;
          default:
              result.add(null);
              break;
          }
      }
      String [] r = new String[5+result.size()] ;
      for( int i = 0 ; i < 5 ; i++ ) {
          r[i] = (String) request[i];
      }
      for( int i = 5 ; i < r.length ; i++ ) {
          r[i] = (String) result.get(i - 5);
      }

      return r ;
    }catch(Exception ee ){
        ee.printStackTrace() ;
        throw ee;
    }
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

      response[5] = checkAccess(userName, password);
      return response ;
  }
  //
  //  r[0] : request
  //  r[1] : <requestor>
  //  r[2] : get-metainfo
  //  r[3] : <user>
  //  r[4] : <key>[,<key>[...]]
  //
   public static final String hh_check_meta = "<user>" ;
   public String ac_check_meta_$_1(Args args ) throws Exception {
       Object [] request = new Object[5] ;
       request[0] = "request" ;
       request[1] = "nobody" ;
       request[2] = "get-metainfo" ;
       request[3] = args.argv(0);
       request[4] = "uid,gid,home,shell,fqn";

       Object answer  =  getMetaInfo( request ) ;

       if( answer instanceof Exception ) {
           throw (Exception) answer;
       }
       Object [] a = (Object [])answer ;
       StringBuilder sb = new StringBuilder() ;
       sb.append("  Uid : ").append(a[5]).append("\n") ;
       sb.append("  Gid : ").append(a[6]).append("\n") ;
       sb.append(" Home : ").append(a[7]).append("\n") ;
       sb.append("Shell : ").append(a[8]).append("\n") ;
       sb.append(" Name : ").append(a[9]).append("\n") ;

       return sb.toString();
   }
   public static final String hh_check_auth = "<user> <password>" ;
   public String ac_check_auth_$_2( Args args )
   {
      String user = args.argv(0) ;
      String pwd  = args.argv(1) ;
      boolean result = checkAccess( user , pwd ) ;
      return result ? "Authentication ok for user <"+user+">" :
                      "Authentication failed for user <"+user+">" ;
   }
   public static final String hh_user_map_ls = "# [-t]" ;
   public String ac_user_map_ls( Args args ){
      if( _map == null ) {
          throw new
                  IllegalArgumentException("User map hash not needed");
      }
      Iterator<?>     i  = _map.keysIterator() ;
      StringBuilder sb = new StringBuilder() ;
      while( i.hasNext() ){
         sb.append(i.next()).append("\n");
      }
      return sb.toString() ;
   }
   public static final String hh_user_map_remove = "<userName> # remove user from hash";
   public String ac_user_map_remove_$_1(Args args ){
      if( _map == null ) {
          throw new
                  IllegalArgumentException("User map hash not needed");
      }

      if( _map.remove(args.argv(0)) == null ) {
          throw new
                  IllegalArgumentException("User name not in cache : " + args
                  .argv(0));
      }

      return "";
   }
   public static final String hh_user_map_add = "<userName> <uid> <gid> [<home> [<shell>]]" ;
   public String ac_user_map_add_$_3_5( Args args ){
      String user  = args.argv(0) ;
      String uid   = args.argv(1) ;
      String gid   = args.argv(2) ;
      String home  = args.argc() > 3 ? args.argv(3) : "/dev/home" ;
      String shell = args.argc() > 4 ? args.argv(4) : "/bin/shell" ;

      BasicAttributes attr  = new BasicAttributes() ;
      attr.put( "uidNumber" , uid ) ;
      attr.put( "gidNumber" , gid ) ;
      attr.put( "homeDirectory" , home ) ;
      attr.put( "loginShell" , shell ) ;
      attr.put( "gecos" , user ) ;

      UserRecord ur = new UserRecord(attr) ;
      ur._timestamp  = 0L ;

      _map.put( user , ur ) ;
      return "" ;
   }
   public static final String hh_user_map_reset = "# clear user map hash" ;
   public String ac_user_map_reset( Args args ){
      if( _map == null ) {
          throw new
                  IllegalArgumentException("User map hash not needed");
      }
      _map.clear() ;
      return "" ;
   }

} // End of PAMAuthentificator
