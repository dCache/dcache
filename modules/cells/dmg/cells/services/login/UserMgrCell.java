package dmg.cells.services.login ;

import java.lang.reflect.* ;
import java.io.* ;
import java.util.*;
import dmg.cells.nucleus.*;
import dmg.util.*;
import dmg.util.cdb.* ;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       UserMgrCell
       extends     CellAdapter            {

  private String       _cellName ;
  private CellNucleus  _nucleus ;
  private UserDb       _userDb ;
  private Args         _args ;

  private static final String [] __root_priv = {
     "create-user:user:*" ,
     "create-group:user:*" ,
     "add-user:user:*" ,
     "add-group:user:*" ,
     "remove-user:user:*" ,
     "destroy-user:user:*" ,
     "remove-group:user:*" ,
     "destroy-group:user:*" ,
     "modify-user:user:*" ,
     "modify-group:user:*" ,
     "add-allowed:user:*" ,
     "remove-allowed:user:*" ,
     "add-denied:user:*" ,
     "remove-denied:user:*" ,

  } ;
  /**
  */
  public UserMgrCell( String name , String argString ) throws Exception {

      super( name , argString , false ) ;

      _cellName      = name ;

      _args = getArgs() ;


      try{

         if( _args.argc() < 1 )
           throw new IllegalArgumentException( "Usage : ... <dbPath>" ) ;

         try{
            _userDb = new UserDb( new File( _args.argv(0) ) , false ) ;
         }catch( Exception eee ){
            _userDb = new UserDb( new File( _args.argv(0) ) , true ) ;
            //
            // not really necessary, because 'root' is trusted
            // anyway.
            //
            createRootUser( _userDb ) ;

         }

//         setPrintoutLevel( 0xf ) ;

      }catch( Exception e ){
         start() ;
         kill() ;
         throw e ;
      }

      start() ;

  }
  private void createRootUser( UserDb db )throws Exception {
      UserHandle user = db.createUser( "root" ) ;
      user.open( CdbLockable.WRITE ) ;
         for( int i = 0 ; i < __root_priv.length ; i++ )
            user.addAllowed( __root_priv[i] ) ;
         user.setPassword( "elch" ) ;
      user.close( CdbLockable.COMMIT ) ;
  }
  private static final Class [] __argListDef = {
      dmg.cells.services.login.UserPrivileges.class ,
      java.lang.Object[].class
  } ;
  public void messageArrived( CellMessage msg ){

      Object obj     = msg.getMessageObject() ;
      Object answer  = "PANIX" ;

      try{
         say( "Message : "+obj.getClass() ) ;
         if( ( ! ( obj instanceof Object [] ) ) ||
             (  ((Object[])obj).length < 3 )    ||
             ( !((Object[])obj)[0].equals("request") ) ){
             String r = "Illegal message object received from : "+
                         msg.getSourcePath() ;
             esay( r ) ;
             throw new Exception( r ) ;
         }
         Object [] request    = (Object[])obj ;
         String user          = request[1] == null ?
                                "unknown" : (String)request[1] ;
         String command       = (String)request[2] ;
         UserPrivileges priv  = _userDb.getUserPrivileges( user ) ;
         say( ">"+command+"< request from "+user ) ;
         try{
            command  = createMethodName( command ) ;
            Method m = this.getClass().getDeclaredMethod( command , __argListDef ) ;
            Object [] a = new Object[2] ;
            a[0] = priv ;
            a[1] = request ;
            answer = m.invoke( this , a ) ;
         }catch( InvocationTargetException ite ){
            throw (Exception)ite.getTargetException() ;
         }catch( Exception xe ){
            throw new Exception( "Command not found : "+(String)request[2] ) ;
         }
      }catch(Exception iex ){
         answer = iex ;
      }

      if( answer instanceof Object [] )
        ((Object[])answer)[0] = "response" ;

      msg.revertDirection() ;
      msg.setMessageObject( answer ) ;
      try{
         sendMessage( msg ) ;
      }catch( Exception ioe ){
         esay( "Can't send acl_response : "+ioe ) ;
      }
  }
  private String createMethodName( String com ){
     char c ;
     StringBuffer sb = new StringBuffer() ;
     sb.append( "acl_" ) ;
     for( int i = 0 ; i < com.length() ; i ++ ){
         c = com.charAt(i) ;
         sb.append( c == '-' ? '_' : c ) ;
     }
     return sb.toString() ;
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
  private Object
          acl_check_password( UserPrivileges priv , Object [] request )
          throws Exception {

      if( request.length < 5 )
         throw new
         IllegalArgumentException(
         "Not enough arguments for 'check-password'" ) ;

      Object [] response = new Object[6] ;
      for( int i = 0 ;i < 5; i++ )response[i] =  request[i] ;
      response[1]     = request[3] ;
      String userName = (String)request[3] ;

      UserHandle user = _userDb.getUserByName( userName ) ;
      user.open( CdbLockable.READ ) ;
      String password ;
      try{
         password = user.getPassword() ;
      }catch(Exception e ){
          user.close( CdbLockable.ABORT ) ;
          throw e ;
      }
      user.close( CdbLockable.COMMIT ) ;

      response[5] = Boolean.valueOf( password.equals(request[4]) ) ;
      return response ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <anything>
  //  r[2] : check-acl
  //  r[3] : <user>
  //  r[4] : <action>
  //  r[5] : <class>
  //  r[6] : <instance>
  //
  //  checks : nothing
  //
  //    response
  //
  //  r[0] : response
  //  r[1] : <user>
  //  r[2] : check-acl
  //  r[3] : <user>
  //  r[4] : <action>
  //  r[5] : <class>
  //  r[6] : <instance>
  //  r[7] : Boolean(true/false)
  //
  private Object
          acl_check_acl( UserPrivileges priv , Object [] request )
          throws Exception {

      if( request.length < 7 )
         throw new
         IllegalArgumentException(
         "Not enough arguments for 'check-acl'" ) ;

      Object [] response = new Object[8] ;
      for( int i = 0 ;i < 7; i++ )response[i] =  request[i] ;
      response[1]     = request[3] ;
      String userName = (String)request[3] ;

      UserPrivileges privToCheck = _userDb.getUserPrivileges( userName ) ;
      String p = ""+request[4]+":"+request[5]+":"+request[6] ;
      response[7] = Boolean.valueOf( privToCheck.isAllowed( p ) ) ;
      return response ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <user>
  //  r[2] : create-group
  //  r[3] : <newUser>
  //
  //  checks : create-user:user:*
  //
  private Object
          acl_create_user( UserPrivileges priv , Object [] request )
          throws Exception {

      if( ! priv.isAllowed( "create-user:user:*" ) )
         throw new
         Exception( "Operation not allowed for "+priv.getUserName() ) ;

      if( request.length < 4 )
         throw new
         IllegalArgumentException( "Not enough arguments for 'create-user'" ) ;

      _userDb.createUser( (String)request[3] ) ;

      return request ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <user>
  //  r[2] : get-user-attr
  //  r[3] : <user>
  //
  //  checks : nothing
  //
  private Object
          acl_get_user_attr( UserPrivileges priv , Object [] request )
          throws Exception {


      if( request.length < 4 )
         throw new
         IllegalArgumentException( "Not enough arguments for 'get-user-attr'" ) ;

         UserHandle user = _userDb.getUserByName( request[3].toString() ) ;
         user.open( CdbLockable.READ ) ;
            String eMail = user.getEmail() ;
         user.close( CdbLockable.COMMIT ) ;
         String [] p = new String[2] ;
         p[0] = "e-mail" ;
         p[1] = eMail ;
         Object [] answer = new Object[5] ;
         System.arraycopy( request , 0 , answer , 0 , 4 ) ;
         answer[4]    = new Object[1] ;
         ((Object[])answer[4])[0] = p ;
      return answer ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <user>
  //  r[2] : set-user-attr
  //  r[3] : <user>
  //  r[4] -> array of ( String [2] ( key , value ) )
  //
  //  checks : nothing
  //
  private Object
          acl_set_user_attr( UserPrivileges priv , Object [] request )
          throws Exception {


      if( request.length < 5 )
         throw new
         IllegalArgumentException( "Not enough arguments for 'set-user-attr'" ) ;

      if( ! ( request[4] instanceof Object [] ) )
         throw new
         IllegalArgumentException( "Illegal request format 'set-user-attr'" ) ;
      //
      // does the user exists ?
      //
      UserHandle user = _userDb.getUserByName( request[3].toString() ) ;
      //
      // is the requestor allowed to do the operation ?
      //
      if( ( ! priv.isAllowed( "set-password:user:*" ) ) &&
          ( ! priv.getUserName().equals( request[3].toString() )   )    )
         throw new
         Exception( "Operation not allowed for "+priv.getUserName() ) ;

      Object [] array = (Object[])request[4] ;
      for( int i = 0 ; i < array.length ; i++ ){
          if( array[i] instanceof String [] ){
             String [] pair = (String [])array[i] ;
             if( pair[0].equals("e-mail") ){
                user.open( CdbLockable.WRITE ) ;
                   user.setEmail( pair[1] ) ;
                user.close( CdbLockable.COMMIT ) ;
             }else{
               pair[1] = "" ;
             }
          }else{
              array[i] = null ;
          }
      }
      return request ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <user>
  //  r[2] : create-group
  //  r[3] : <newGroup>
  //
  //  checks : create-group:user:<newGroup>
  //
  private Object
          acl_create_group( UserPrivileges priv , Object [] request )
          throws Exception {

      if( request.length < 4 )
         throw new
         IllegalArgumentException( "Not enough arguments for 'create-group'" ) ;

      String groupName = (String)request[3] ;

      if( ! priv.isAllowed( "create-group:user:"+groupName ) )
         throw new
         Exception( "Operation not allowed for "+priv.getUserName() ) ;


      _userDb.createGroup( groupName ) ;

      return request ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <user>
  //  r[2] : set-password
  //  r[3] : <user>
  //  r[4] : <password/plaintext>
  //
  //  checks : set-password:user:*
  //
  private Object
          acl_set_password( UserPrivileges priv , Object [] request )
          throws Exception {

      if( request.length < 5 )
         throw new
         IllegalArgumentException( "Not enough arguments for 'set-password'" ) ;

      String userName = (String)request[3] ;

      if( ( ! priv.isAllowed( "set-password:user:*" ) ) &&
          ( ! priv.getUserName().equals( userName )   )    )
         throw new
         Exception( "Operation not allowed for "+priv.getUserName() ) ;

         UserHandle user = _userDb.getUserByName( userName ) ;
         user.open( CdbLockable.WRITE ) ;
            user.setPassword( (String)request[4] ) ;
         user.close( CdbLockable.COMMIT ) ;

      return request ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //  r[0] : request
  //  r[1] : <user>
  //  r[2] : add-allowed
  //  r[3] : <user/group>
  //  r[4] : <privilege>
  //
  //  checks : add-allowed:user:*           ; for users
  //  checks : add-allowed:user:<group>[.*] ; for groups
  //
  private Object
          acl_add_allowed( UserPrivileges priv , Object [] request )
          throws Exception {
      if( request.length < 5 )
         throw new
         IllegalArgumentException( "Not enough arguments for 'add-allowed'" ) ;

      String userName = (String)request[3] ;

      UserHandle user = _userDb.getUserByName( userName ) ;
      boolean isGroup ;
      user.open( CdbLockable.READ ) ;
      try{
          isGroup = user.isGroup() ;
      }catch(Exception e ){
          throw e ;
      }finally{
          user.close( CdbLockable.COMMIT ) ;
      }
      String p ;
      if( isGroup ){
         p = "add-allowed:user:"+userName ;
         if( ! priv.isAllowed( p ) ){
            say( ">"+p+"< denied for "+priv.getUserName() ) ;
            throw new
            Exception( "Operation not allowed for "+priv.getUserName() ) ;
         }
      }else{
         p = "add-allowed:user:*";
         if( ! priv.isAllowed( p ) ){
            say( ">"+p+"< denied for "+priv.getUserName() ) ;
            throw new
            Exception( "Operation not allowed for "+priv.getUserName() ) ;
         }
      }
      user.open( CdbLockable.WRITE ) ;
      try{
         user.addAllowed( (String)request[4] ) ;
      }catch(Exception e ){
          user.close( CdbLockable.ABORT ) ;
          throw e ;
      }
      user.close( CdbLockable.COMMIT ) ;
      return request ;
  }
}
