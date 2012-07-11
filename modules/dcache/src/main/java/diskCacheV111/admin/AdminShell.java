// $Id: AdminShell.java,v 1.2 2007-05-24 13:51:07 tigran Exp $

package diskCacheV111.admin ;

import dmg.cells.services.login.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      AdminShell extends CommandInterpreter {

    private final static Logger _log =
        LoggerFactory.getLogger(AdminShell.class);

    private CellNucleus _nucleus ;
    private String      _user ;
    private CellPath    _cellPath  = new CellPath( "AclCell" ) ;
    private CellShell   _cellShell = null ;
    private boolean     _wasAdmin  = false ;
    public AdminShell( String user , CellNucleus nucleus , Args args ){
       _nucleus  = nucleus ;
       _user     = user ;
       _wasAdmin = _user.equals("admin");
       for( int i = 0 ; i < args.argc() ; i++ ) {
           _log.info("arg[" + i + "]=" + args.argv(i));
       }

    }
    public String getPrompt(){ return _cellPath.getCellName()+"("+_user+") >> " ; }
    public String getHello(){
      return "\n    Welcome to the dCache Admin Interface (user="+_user+")\n\n" ;
    }
    public String hh_id = "[<newId>]" ;
    public String ac_id_$_0_1( Args args )
    {

       if( args.argc() == 1 ){
            if( ( ! _user.equals("admin") ) && ( ! _wasAdmin ) ) {
                throw new
                        IllegalArgumentException("Not allowed");
            }
           _user = args.argv(0) ;
           return "" ;
       }else{
           return _user+"\n" ;
       }
    }
    public String hh_cd = "<destinationCell>" ;
    public String ac_cd_$_1( Args args )
    {
       _cellPath = new CellPath(args.argv(0));
       return "" ;
    }
    public String hh_pwd = "" ;
    public String ac_pwd_$_0( Args args )
    {
       return _cellPath.toString() + "\n" ;
    }
    public Object executeCommand( String obj ) throws Exception {
        _log.info( "executeCommand : arriving as String : "+obj ) ;
        return _executeCommand( obj , false ) ;
    }
    public Object executeCommand( Object obj ) throws Exception {
        _log.info( "executeCommand : arriving as Object : "+obj.toString() ) ;
        return _executeCommand( obj.toString() , true ) ;
    }
    public Object _executeCommand( String obj , boolean wasBinary ) throws Exception {

       String str = obj ;
       String tr  = str.trim() ;
       if( tr.equals("") ) {
           return "";
       }
       if( tr.equals("logout") ) {
           throw new CommandExitException();
       }
       if( tr.startsWith(".") ){
          tr = tr.substring(1) ;
          return command( tr ) ;
       }
       Args xx = new Args( str ) ;
       CellPath cellPath = null ;
       String   path     = null ;
       if( ( path = xx.getOpt("cellPath") ) != null ){
          cellPath = new CellPath( path ) ;
       }else{
          cellPath = _cellPath ;
       }

       CellMessage res =
         _nucleus.sendAndWait(
                new CellMessage(
                     cellPath ,
                     new AuthorizedString(
                           _user ,
                           str + ( wasBinary ? " -binary" : "" ) )
                    ) ,
                10000
         ) ;
       if( res == null ) {
           throw new Exception("Request timed out");
       }
       Object resObject = res.getMessageObject() ;
       _log.info( "result from domain : "+resObject.getClass().getName() ) ;
       if( wasBinary ){
           return resObject ;
       }else{
          if( resObject instanceof Exception ) {
              throw (Exception) resObject;
          }
          String r = resObject.toString() ;
          if( r.length() == 0 ) {
              return "";
          }
          if( r.substring(r.length()-1).equals("\n" ) ) {
              return r;
          } else {
              return r + "\n";
          }
       }
    }
}
