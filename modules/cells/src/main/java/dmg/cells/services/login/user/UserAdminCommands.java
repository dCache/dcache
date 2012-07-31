// $Id: UserAdminCommands.java,v 1.3 2006-12-15 10:58:14 tigran Exp $
package dmg.cells.services.login.user  ;

import java.io.* ;
import java.util.* ;
import dmg.util.* ;

public class UserAdminCommands implements  Interpretable {

   private AclDb            _aclDb;
   private UserRelationable _userDb;
   private UserMetaDb       _userMetaDb;
   public UserAdminCommands( UserRelationable userDb ,
                             AclDb            aclDb ,
                             UserMetaDb       metaDb  ){

      _userDb     = userDb ;
      _aclDb      = aclDb ;
      _userMetaDb = metaDb ;
   }
    /////////////////////////////////////////////////////////////////
    //
    //   generic part
    //
    private void checkDatabase() throws Exception {
           if( ( _userMetaDb != null ) &&
               ( _aclDb      != null ) &&
               ( _userDb     != null )   ) {
               return;
           }
        throw new
        Exception( "Not all databases are open" ) ;
    }
    private void checkPermission( Args args , String acl ) throws Exception {
       if( ! ( args instanceof Authorizable ) ) {
           return;
       }
       String user = ((Authorizable)args).getAuthorizedPrincipal() ;
       if( user.equals("admin") ) {
           return;
       }

       if( ( ! _aclDb.check("super.access",user,_userDb) ) &&
           ( ! _aclDb.check(acl,user,_userDb) )    ) {
           throw new
                   AclPermissionException("Acl >" + acl + "< negative for " + user);
       }
    }
    public String hh_create_user = "<userName>" ;
    public String ac_create_user_$_1( Args args )throws Exception {
       checkDatabase() ;
       String user = args.argv(0) ;
       checkPermission( args , "user."+user+".create" ) ;
       _userMetaDb.createUser( user ) ;
       return "" ;
    }
    public String hh_create_group = "<groupName>" ;
    public String ac_create_group_$_1( Args args )throws Exception {
       checkDatabase() ;
       String group = args.argv(0) ;
       checkPermission( args , "user."+group+".create" ) ;
       _userMetaDb.createGroup( group ) ;
       _userDb.createContainer( group ) ;
       _aclDb.createAclItem( "user."+group+".modify" ) ;
       return "" ;
    }
    public String hh_destroy_principal = "<principalName>" ;
    public String ac_destroy_principal_$_1( Args args )throws Exception {
       checkDatabase() ;
       String user = args.argv(0) ;
       checkPermission( args , "user."+user+".destroy" ) ;
       try{
          UserMetaDictionary dict = _userMetaDb.getDictionary( user ) ;
          String type = dict.valueOf("type") ;
          if( type == null ) {
              throw new
                      DatabaseException("Principal type not defined in meta database");
          }

          if( type.equals("user" ) ){
             try{
                Enumeration e = _userDb.getParentsOf(user) ;
                if( e.hasMoreElements() ) {
                    throw new
                            DatabaseException("Still in groups : " + user);
                }
              }catch(NoSuchElementException eee ){
                // no problem : has not been in a group
              }
             _userMetaDb.removePrincipal( user ) ;

          }else if( type.equals( "group" ) ){

             Enumeration e = _userDb.getElementsOf(user) ;

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
             _userDb.removeContainer( user ) ;
             _aclDb.removeAclItem( "user."+user+".access" ) ;

          }else{
             throw new
               DatabaseException( "Invalid principal type : "+type ) ;
          }
       }catch(Exception ie ){
          ie.printStackTrace() ;
          throw ie ;
       }
       return "" ;
    }
    public String hh_add = "<principalName> to <groupName>" ;
    public String ac_add_$_3( Args args )throws Exception {
       checkDatabase() ;
       if( ! args.argv(1).equals("to") ) {
           throw new
                   CommandSyntaxException("keyword 'to' missing");
       }
       String group = args.argv(2) ;
       String princ = args.argv(0) ;
       checkPermission( args , "user."+group+".add" ) ;
       _userDb.addElement(group, princ);
       return "" ;
    }
    public String hh_remove = "<principalName> from <groupName>" ;
    public String ac_remove_$_3( Args args )throws Exception {
       checkDatabase() ;
       if( ! args.argv(1).equals("from") ) {
           throw new
                   CommandSyntaxException("keyword 'from' missing");
       }
       String group = args.argv(2) ;
       String princ = args.argv(0) ;
       checkPermission( args , "user."+group+".remove" ) ;
       _userDb.removeElement(group,princ);
       return "" ;
    }
    public String hh_show_parents = "<principal>" ;
    public Object ac_show_parents_$_1( Args args )
    {
        String  user     = args.argv(0) ;
        boolean isBinary = args.hasOption("binary")  ;

        _userMetaDb.getDictionary( user ) ; // check exists
        try{
           Enumeration e = _userDb.getParentsOf(user) ;
           return isBinary ?  sendBinary( e ) : (Object) sendAscii( e ) ;
        }catch(NoSuchElementException eee ){
           return isBinary ? ((Object)new Vector()) : (Object)"" ;
        }
    }
    public String hh_show_group = "<group>" ;
    public Object ac_show_group_$_1( Args args )
    {
        Enumeration  ee  = _userDb.getElementsOf(args.argv(0)) ;
        Enumeration  ep  = _userDb.getParentsOf(args.argv(0)) ;
        if( !args.hasOption("binary") ){
           StringBuilder sb = new StringBuilder() ;
           sb.append( "Parents : \n" ) ;
           while( ep.hasMoreElements() ){
              sb.append("  ").append(ep.nextElement().toString()).append("\n") ;
           }
           sb.append( "Elements : \n" ) ;
           while( ee.hasMoreElements() ){
              sb.append("  ").append(ee.nextElement().toString()).append("\n") ;
           }
           return sb.toString() ;
        }else{
           Object [] v = new Vector[2] ;
           v[0] = sendBinary( ep ) ;
           v[1] = sendBinary( ee ) ;
           return v ;
        }
    }
    public String hh_show_groups = "" ;
    public Object ac_show_groups( Args args )
    {
        Enumeration  e  = _userDb.getContainers() ;
        return !args.hasOption("binary") ?
               (Object) sendAscii( e ) : sendBinary( e ) ;
    }
    private String sendAscii( Enumeration e ){
        StringBuilder sb = new StringBuilder() ;
        while( e.hasMoreElements() ){
           sb.append( e.nextElement().toString() ).append("\n") ;
        }
        return sb.toString() ;
    }
    private Object sendBinary( Enumeration e ){
        Vector v = new Vector() ;
        while( e.hasMoreElements() ){
           v.addElement( e.nextElement().toString() ) ;
        }
        return v ;
    }
    public String hh_add_access = "[-allowed|-denied] <acl> <principal>" ;
    public String ac_add_access_$_2( Args args )throws Exception {
       checkDatabase() ;
       boolean allowed = !args.hasOption("denied") ;
       String acl   = args.argv(0) ;
       String princ = args.argv(1) ;
       checkPermission( args , "acl."+acl+".add" ) ;
       if( allowed ){
           _aclDb.addAllowed( acl , princ ) ;
       }else{
           _aclDb.addDenied( acl , princ ) ;
       }
       return "" ;
    }
    public String hh_remove_access = "<acl> <principal>" ;
    public String ac_remove_access_$_2( Args args )throws Exception {
        String acl   = args.argv(0) ;
        String princ = args.argv(1) ;
        checkPermission( args , "acl."+acl+".remove" ) ;
        _aclDb.removeUser( acl , princ );
        return "" ;
    }
    public String hh_create_acl = "<aclName>" ;
    public String ac_create_acl_$_1( Args args )throws Exception {
        checkDatabase() ;
        String aclName = args.argv(0) ;
        checkPermission( args , "acl."+aclName+".create");
        _aclDb.createAclItem(aclName);
        return "" ;
    }
    public String hh_destroy_acl = "<aclName>" ;
    public String ac_destroy_acl_$_1( Args args )throws Exception {
        checkDatabase() ;
        String aclName = args.argv(0) ;
        checkPermission( args , "acl."+aclName+".destroy");
        _aclDb.removeAclItem(aclName);
        return "" ;
    }
    public String hh_show_acl = "<aclName> [-resolve]" ;
    public Object ac_show_acl_$_1( Args args )throws Exception {
        checkDatabase() ;
        boolean resolve = args.hasOption("resolve") ;
        AcDictionary dict = _aclDb.getPermissions(args.argv(0),resolve);
        Enumeration e = dict.getPrincipals() ;
        String inherits = dict.getInheritance() ;
        StringBuilder sb = new StringBuilder() ;
        Hashtable  hash = new Hashtable() ;
        if( ! resolve ){
           if( inherits == null ){
              sb.append( "<noinheritance>\n") ;
              hash.put( "<inheritsFrom>" , "none" ) ;
           }else{
              sb.append("<inherits=").append(inherits).append(">\n");
              hash.put( "<inheritsFrom>" , inherits ) ;
           }
        }
        while( e.hasMoreElements() ){
            String user = (String)e.nextElement() ;
            boolean perm = dict.getPermission(user) ;
            sb.append(user).append(" -> ").append(perm).append("\n");
            hash.put( user , perm) ;
        }
        return !args.hasOption("binary") ?
               (Object)sb.toString() :
               (Object)hash ;
    }
    public String hh_check = "<acl> <user>" ;
    public Object ac_check_$_2( Args args )throws Exception {
        checkDatabase() ;
        boolean ok = _aclDb.check(args.argv(0),args.argv(1),_userDb);
        if( args.hasOption("binary") ) {
            return ok;
        }
        return  ( ok ? "Allowed" : "Denied" ) + "\n" ;
    }
    public String hh_show_principal = "<principalName>" ;
    public Object ac_show_principal_$_1( Args args )
    {
        UserMetaDictionary dict = _userMetaDb.getDictionary(args.argv(0)) ;
        Enumeration e = dict.keys() ;
        if( !args.hasOption( "binary" ) ){
           StringBuilder sb = new StringBuilder() ;
           while( e.hasMoreElements() ){
               String user = (String)e.nextElement() ;
               sb.append(user).append(" -> ").append(dict.valueOf(user))
                       .append("\n") ;
           }
           return sb.toString() ;
        }else{
           Hashtable hash = new Hashtable() ;
           while( e.hasMoreElements() ){
               String user = (String)e.nextElement() ;
               hash.put( user , dict.valueOf(user) ) ;
           }
           return hash ;
        }
    }
    public String hh_set_principal = "<principalName> <key>=<value> [...]" ;
    public String ac_set_principal_$_1_99( Args args )throws Exception {
        checkPermission( args , "user."+args.argv(0)+".modify");
        StringTokenizer st;
        String key, value;
        for( int i = 1 ; i < args.argc() ; i++ ){
           st  = new StringTokenizer( args.argv(i) , "=" ) ;
           key = st.nextToken() ;
           try{ value = st.nextToken() ;
           }catch(Exception ee){ value = "" ; }
           _userMetaDb.setAttribute( args.argv(0) , key , value ) ;
        }
        return "" ;
    }
    public String hh_let = "<aclName> inheritfrom <aclNameFrom>" ;
    public String ac_let_$_3( Args args )throws Exception {
       if( ! args.argv(1).equals("inheritfrom") ) {
           throw new
                   CommandSyntaxException("keyword 'inheritfrom' missing");
       }
        checkPermission( args , "acl."+args.argv(0)+".modify");
        _aclDb.setInheritance(args.argv(0),args.argv(2));
        return "" ;
    }
}
