// $Id: UserSecurityCell.java,v 1.4 2006-12-15 10:58:14 tigran Exp $
package dmg.cells.services.login.user  ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.Enumeration;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;

import org.dcache.util.Args;
import dmg.util.Authorizable;
import dmg.util.AuthorizedString;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       UserSecurityCell
       extends     CellAdapter            {

  private final static Logger _log =
      LoggerFactory.getLogger(UserSecurityCell.class);

  private final String       _cellName ;
  private final CellNucleus  _nucleus ;
  private final Args         _args ;

  private AclDb            _aclDb;
  private UserRelationable _userDb;
  private UserMetaDb       _userMetaDb;

  public UserSecurityCell( String name , String argString ) throws Exception {

      super( name , argString , false ) ;

      _cellName  = name ;
      _args      = getArgs() ;
      _nucleus = getNucleus();

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

      }catch( Exception e ){
         _log.warn( "Exception while <init> : "+e, e ) ;
         start() ;
         kill() ;
         throw e ;
      }

      start() ;

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

            _log.info( ">"+command+"< request from "+user );
            //FIXME: refactoring required
            try{
              if( command.equals( "check-password" ) ) {
                  answer = acl_check_password(request);
              } else {
                  throw new Exception("Command not found : " + command);
              }
            }catch( Exception xe ){
               throw new Exception( "Problem : "+xe ) ;
            }
         }else if( obj instanceof AuthorizedString ){
            String command = obj.toString() ;
            String user    = ((Authorizable)obj).getAuthorizedPrincipal() ;
            answer = execAuthorizedString( user , command ) ;

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
  private Serializable execAuthorizedString( String user , String command )
          throws Exception {

       if( ( user == null ) || ( user.length() == 0 ) ) {
           throw new
                   Exception("Not authenticated");
       }

       if (command.trim().isEmpty()) {
           return "";
       }
       try{
          return command( new Args( command + " -auth="+user ) ) ;
       }catch( CommandPanicException cte ){
          throw (Exception)cte.getTargetException() ;
       }catch( CommandThrowableException cte ){
          throw (Exception)cte.getTargetException() ;
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


      response[5] = true;
      return response ;
  }
  private void checkPermission( String user , String acl )
          throws AclPermissionException {
     if( user == null ) {
         throw new
                 AclPermissionException("Not authenticated");
     }

     if( ! user.equals("admin") ){
        if( ! _aclDb.check(acl,user,_userDb) ) {
            throw new
                    AclPermissionException("Acl >" + acl + "< negative for " + user);
        }
     }
  }
  /////////////////////////////////////////////////////////////
  //
  //   the interpreter
  //
  public static final String hh_show_all = "<user> exception|null|object|string" ;
  public Object ac_show_all_$_1( Args args )throws Exception {

      String user = args.getOpt("auth") ;
      if( user == null ) {
          throw new Exception("Not authenticated");
      }
      String command = args.argv(0) ;
      _log.info( "show all : mode="+command+";user=user") ;
      if( command.equals("exception") ) {
          throw new
                  Exception("hallo otto");
      }
      if( command.equals("null") ) {
          return null;
      }
      if( command.equals("object") ) {
          return args;
      }
      return "Done" ;

  }
   public static final String hh_check_permission = "<user> <acl>" ;
   public Object ac_check_permission_$_2( Args args )
   {

        try{
           checkPermission( args.argv(0) , args.argv(1) ) ;
           return true;
        }catch( AclPermissionException e ){
           return false;
        }

   }
   public static final String hh_create_user = "<userName>" ;
   public String ac_create_user_$_1( Args args )throws Exception {
      checkPermission( args.getOpt("auth" ) , "user.*.create" ) ;
      String user = args.argv(0) ;
      _userMetaDb.createUser( user ) ;
      return "" ;
   }
    public static final String hh_create_group = "<groupName>" ;
    public String ac_create_group_$_1( Args args )throws Exception {
      checkPermission( args.getOpt("auth" ) , "user.*.create" ) ;
       String group = args.argv(0) ;
       _userMetaDb.createGroup( group ) ;
       _userDb.createContainer( group ) ;
       _aclDb.createAclItem( "group."+group+".access" ) ;
       return "" ;
    }
    public static final String hh_destroy_principal = "<principalName>" ;
    public String ac_destroy_principal_$_1( Args args )throws Exception {
       checkPermission( args.getOpt("auth" ) , "user.*.create" ) ;
       String user = args.argv(0) ;
       Enumeration<String> e = _userDb.getElementsOf(user) ;
       if( e.hasMoreElements() ) {
           throw new
                   DatabaseException("Not Empty : " + user);
       }
       e = _userDb.getParentsOf(user) ;
       if( e.hasMoreElements() ) {
           throw new
                   DatabaseException("Still in groups : " + user);
       }
       _userMetaDb.removePrincipal( user ) ;
       try{
          _userDb.removeContainer( user ) ;
          _aclDb.removeAclItem( "group."+user+".access" ) ;
       }catch( Exception ee ){
          _log.warn( args.toString()+" : "+ee ) ;
           //
           // not an error
           //
//          System.err.println(" removeContainer : "+ee ) ;
       }
       return "" ;
    }
    public static final String hh_add = "<principalName> to <groupName>" ;
    public String ac_add_$_3( Args args )throws Exception {
       if( ! args.argv(1).equals("to") ) {
           throw new
                   CommandSyntaxException("keyword 'to' missing");
       }
       String group = args.argv(2) ;
       String princ = args.argv(0) ;
       checkPermission( args.getOpt("auth" ) , "group."+group+".access" ) ;
       _userDb.addElement(group, princ);
       return "" ;
    }
    public static final String hh_remove = "<principalName> from <groupName>" ;
    public String ac_remove_$_3( Args args )throws Exception {
       if( ! args.argv(1).equals("from") ) {
           throw new
                   CommandSyntaxException("keyword 'from' missing");
       }
       String group = args.argv(2) ;
       String princ = args.argv(0) ;
       checkPermission( args.getOpt("auth" ) , "group."+group+".access" ) ;
       _userDb.removeElement(group,princ);
       return "" ;
    }
    public static final String hh_add_access = "[-allowed|-denied] <acl> <principal>" ;
    public String ac_add_access_$_2( Args args )throws Exception {
       boolean allowed = !args.hasOption("denied") ;
       String acl   = args.argv(0) ;
       String princ = args.argv(1) ;
       checkPermission( args.getOpt("auth") , "acl."+acl+".access" ) ;
       if( allowed ){
           _aclDb.addAllowed( acl , princ ) ;
       }else{
           _aclDb.addDenied( acl , princ ) ;
       }
       return "" ;
    }
    public static final String hh_create_acl = "<aclName>" ;
    public String ac_create_acl_$_1( Args args )throws Exception {
        checkPermission( args.getOpt("auth") , "super.access" ) ;
        _aclDb.createAclItem(args.argv(0));
        return "" ;
    }
    public static final String hh_ls_acl = "<aclName> -resolve" ;
    public String ac_ls_acl_$_1( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        boolean resolve = args.hasOption("resolve") ;
        AcDictionary dict = _aclDb.getPermissions(args.argv(0),resolve);
        Enumeration<String> e     = dict.getPrincipals() ;
        String inherits   = dict.getInheritance() ;
        StringBuilder sb   = new StringBuilder() ;
        if( inherits == null ) {
            sb.append("<resolved>\n");
        } else {
            sb.append("<inherits=").append(inherits).append(">\n");
        }
        while( e.hasMoreElements() ){
            String user = e.nextElement();
            sb.append(user).append(" -> ").append(dict.getPermission(user))
                    .append("\n");
        }
        return sb.toString() ;
    }
}
