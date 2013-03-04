// $Id: AdminShell2.java,v 1.2 2001-05-18 20:12:40 cvs Exp $

package diskCacheV111.admin ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.services.login.user.MinimalAdminShell;
import dmg.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 1 May 2001
  */
public class      AdminShell2
       extends    MinimalAdminShell {

    private final static Logger _log =
        LoggerFactory.getLogger(AdminShell2.class);

    private String  _destination;
    public AdminShell2( String user , CellNucleus nucleus , Args args ){
       super( user , nucleus , args ) ;
    }
    public String getHello(){
      return "\n    Welcome to the dCache Admin Interface (user="+getUser()+")\n\n" ;
    }
    @Override
    public String getPrompt(){
        return _destination == null ?
               "<local>("+getUser()+") "   :
               "<"+_destination+">("+getUser()+") ";
    }
    public static final String hh_cd = "<destinationCell>";
    public Object ac_cd_$_1( Args args ){
       _destination = args.argc() == 0 ? null : args.argv(0) ;
       return "" ;
    }
    @Override
    public Object executeCommand( String str )throws Exception {
       _log.info( "String command (super) "+str ) ;

       Object or = null ;
       Args args = new Args(str) ;
       if( _destination != null ){
          if( args.argc() != 0 ){
             if( args.argv(0).equals(".") ){
               _destination = null ;
             }else{
               or = sendCommand( _destination  , str ) ;
             }
          }
       }else{
          or = executeLocalCommand( args ) ;
       }
       if( or == null ) {
           return "";
       }
       String r = or.toString() ;
       if(  r.length() < 1) {
           return "";
       }
       if( r.substring(r.length()-1).equals("\n" ) ) {
           return r;
       } else {
           return r + "\n";
       }
    }
    public Object executeCommand( String destination , Object str )
           throws Exception {

       _log.info( "Object command ("+destination+") "+str) ;

       return sendCommand( destination  , str.toString() ) ;
    }
/*
    public Object executeCommand( String destination , String str )
           throws Exception {

       _log.info( "String command ("+destination+") "+str ) ;

       Args args = new Args(str) ;
       Object or = sendCommand( destination  , str ) ;
       if( or == null )return ""  ;
       String r = or.toString() ;
       if(  r.length() < 1)return "" ;
       if( r.substring(r.length()-1).equals("\n" ) )
          return r   ;
       else
          return r + "\n"  ;
    }
*/
}
