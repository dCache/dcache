// $Id: AclCommander.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.dcache.util.Args;
import dmg.util.AuthorizedArgs;
import dmg.util.CommandInterpreter;


public class AclCommander extends CommandInterpreter {
    private AclDb            _aclDb;
    private UserRelationable _userDb;
    private UserMetaDb       _userMetaDb;
    private String           _user = "none" ;
    public String ac_hello_$_1( Args args )
    {
       throw new NoSuchElementException( "otto" ) ;
    }
    public AclCommander( String [] args )
    {
        if( args.length > 0 ){
           _aclDb = new AclDb( new File( args[0] , "acls" ) ) ;
           _userDb = new InMemoryUserRelation(
                       new FileUserRelation(
                           new File( args[0] , "relations" ) ) ) ;
           _userMetaDb = new UserMetaDb( new File( args[0] , "meta" ) ) ;

        }
        UserAdminCommands uac = new UserAdminCommands( _userDb , _aclDb , _userMetaDb ) ;
        addCommandListener( uac ) ;
    }
    /*
    public static final String hh_open_acl = "<aclDbDirectory>" ;
    public String ac_open_acl_$_1( Args args )throws Exception {
       _aclDb = new AclDb( new File( args.argv(0) ) ) ;
       return "" ;
    }
    public static final String hh_open_relations = "<userRelationDbDirectory>" ;
    public String ac_open_relations_$_1( Args args )throws Exception {
       _userDb = new InMemoryUserRelation(
                   new FileUserRelation(
                       new File( args.argv(0) ) ) ) ;
       return "" ;
    }
    public static final String hh_open_meta = "<userMetaDbDirectory>" ;
    public String ac_open_meta_$_1( Args args )throws Exception {
       _userMetaDb = new UserMetaDb( new File( args.argv(0) ) ) ;
       return "" ;
    }
    */
    public static final String hh_id = "[<newUserName>]" ;
    public String ac_id_$_0_1( Args args )throws Exception {
       checkDatabase() ;
       if( args.argc() == 0 ) {
           return _user + "\n";
       }
       _user = args.argv(0) ;
       return "" ;
    }
    /////////////////////////////////////////////////////////////////
    //
    //   the meta data stuff
    //
    public static final String hh_ls_principal = "<principalName>" ;
    public String ac_ls_principal_$_1( Args args )throws Exception {
        if( _userMetaDb == null ) {
            throw new Exception("UserMetaDb not open");
        }
        UserMetaDictionary dict = _userMetaDb.getDictionary(args.argv(0)) ;
        Enumeration<String> e = dict.keys() ;
        while( e.hasMoreElements() ){
            String user = e.nextElement();
            System.out.println( user+" -> "+dict.valueOf(user) ) ;
        }
        return "" ;
    }
    public static final String hh_meta_set_principal = "<principalName> <key>=<value> [...]" ;
    public String ac_meta_set_principal_$_1_99( Args args )throws Exception {
        if( _userMetaDb == null ) {
            throw new Exception("UserMetaDb not open");
        }
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
    public String ac_meta_create_principal_$_1( Args args )throws Exception {
        if( _userMetaDb == null ) {
            throw new Exception("UserMetaDb not open");
        }
        if( args.hasOption("group") ){
            _userMetaDb.createGroup( args.argv(0) ) ;
        }else{
            _userMetaDb.createUser( args.argv(0) ) ;
        }
        return "" ;
    }
    public String ac_meta_rm_principal_$_1( Args args )throws Exception {
        if( _userMetaDb == null ) {
            throw new Exception("UserMetaDb not open");
        }
        _userMetaDb.removePrincipal( args.argv(0) ) ;
        return "" ;
    }
    /////////////////////////////////////////////////////////////////
    //
    //   the user relation stuff
    //
    public static final String hh_rel_create_group = "<group>" ;
    public String ac_rel_create_group_$_1( Args args )throws Exception {
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        _userDb.createContainer(args.argv(0)) ;
        return "" ;
    }
    public static final String hh_rel_rm_group = "<group>" ;
    public String ac_rel_rm_group_$_1( Args args )throws Exception {
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        _userDb.removeContainer(args.argv(0)) ;
        return "" ;
    }
    public static final String hh_rel_ls_group = "<group>" ;
    public String ac_rel_ls_group_$_1( Args args )throws Exception {
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        Enumeration<String> e = _userDb.getElementsOf(args.argv(0)) ;
        while( e.hasMoreElements() ){
           System.out.println(e.nextElement()) ;
        }
        return "" ;
    }
    public static final String hh_rel_ls_groups = "" ;
    public String ac_rel_ls_groups( Args args )throws Exception {
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        Enumeration<String> e = _userDb.getContainers() ;
        while( e.hasMoreElements() ){
           System.out.println(e.nextElement()) ;
        }
        return "" ;
    }
    public static final String hh_rel_add_user = "<group> <user>" ;
    public String ac_rel_add_user_$_2( Args args )throws Exception {
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        _userDb.addElement(args.argv(0),args.argv(1));
        return "" ;
    }
    public static final String hh_rel_rm_user = "<group> <user>" ;
    public String ac_rel_rm_user_$_2( Args args )throws Exception {
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        _userDb.removeElement(args.argv(0),args.argv(1));
        return "" ;
    }
    /////////////////////////////////////////////////////////////////
    //
    //   the acl stuff
    //
    public static final String hh_acl_create_acl = "<aclName>" ;
    public String ac_acl_create_acl_$_1( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        _aclDb.createAclItem(args.argv(0));
        return "" ;
    }
    public static final String hh_acl_ls_acl = "<aclName> -resolve" ;
    public String ac_acl_ls_acl_$_1( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        boolean resolve = args.hasOption("resolve") ;
        AcDictionary dict = _aclDb.getPermissions(args.argv(0),resolve);
        Enumeration<String> e = dict.getPrincipals() ;
        String inherits = dict.getInheritance() ;
        if( inherits == null ) {
            System.out.println("<resolved>");
        } else {
            System.out.println("<inherits=" + inherits + ">");
        }
        while( e.hasMoreElements() ){
            String user = e.nextElement();
            System.out.println( user+" -> "+dict.getPermission(user) ) ;
        }
        return "" ;
    }
    public static final String hh_acl_rm_acl = "<aclName>" ;
    public String ac_acl_rm_acl_$_1( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        _aclDb.removeAclItem(args.argv(0));
        return "" ;
    }
    public static final String hh_acl_set_inherit = "<aclName> <aclNameFrom>" ;
    public String ac_acl_set_inherit_$_2( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        _aclDb.setInheritance(args.argv(0),args.argv(1));
        return "" ;
    }
    public static final String hh_acl_add_allowed = "<acl> <user>" ;
    public String ac_acl_add_allowed_$_2( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        _aclDb.addAllowed(args.argv(0),args.argv(1));
        return "" ;
    }
    public static final String hh_acl_add_denied = "<acl> <user>" ;
    public String ac_acl_add_denied_$_2( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        _aclDb.addDenied(args.argv(0),args.argv(1));
        return "" ;
    }
    public static final String hh_acl_rm_access = "<acl> <user>" ;
    public String ac_acl_rm_access_$_2( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        _aclDb.removeUser(args.argv(0),args.argv(1));
        return "" ;
    }
    public static final String hh_check = "<acl> <user>" ;
    public String ac_check_$_2( Args args )throws Exception {
        if( _aclDb == null ) {
            throw new Exception("AclDb not open");
        }
        if( _userDb == null ) {
            throw new Exception("UserDb not open");
        }
        boolean ok = _aclDb.check(args.argv(0),args.argv(1),_userDb);
        return  ( ok ? "Allowed" : "Denied" ) + "\n" ;
    }
    private void checkDatabase() throws Exception {
           if( ( _userMetaDb != null ) &&
               ( _aclDb      != null ) &&
               ( _userDb     != null )   ) {
               return;
           }
        throw new
        Exception( "Not all databases are open" ) ;
    }
    public Object exec( String line ) throws Exception {

      if (line.trim().isEmpty()) {
        return "";
      }

      if( _user.equals( "none" ) ) {
          return command(new Args(line));
      } else {
          return command(new AuthorizedArgs(_user, line));
      }
    }
    public static void main( String [] args )
    {
       AclCommander commander = new AclCommander(args) ;
       BufferedReader br = new BufferedReader(
                               new InputStreamReader( System.in ) ) ;


       String line;
       while( true ){
          System.out.print("acl > ") ;
          try{
             if( ( line = br.readLine() ) == null ) {
                 break;
             }
          }catch( Exception ioe ){
             System.err.println("Input problem : "+ioe ) ;
             break ;
          }
          Args a = new Args( line ) ;
          if( a.argc() < 1 ) {
              continue;
          }
          if( a.argv(0).equals("exit") ) {
              break;
          }
          try{
              System.out.print( commander.exec( line ) ) ;
          }catch(Exception nse ){
              System.err.println( "Problem : "+nse ) ;
          }

       }
       System.exit(0) ;

    }
}
