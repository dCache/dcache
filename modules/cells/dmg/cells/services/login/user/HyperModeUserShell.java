// $Id: HyperModeUserShell.java,v 1.1 2006-11-20 07:45:51 patrick Exp $

package  dmg.cells.services.login.user     ;

import dmg.cells.services.login.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;

import java.util.* ;
import java.io.* ;
import java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 19 Nov 2006
  */
public class    HyperModeUserShell
     extends    CommandInterpreter {

    private final static Logger _log =
        LoggerFactory.getLogger(HyperModeUserShell.class);

    private CellNucleus _nucleus  = null ;
    private String      _user     = null ;
    private String      _authUser = null ;
    private CellPath    _path     = new CellPath("acm");
    private long        _timeout  = 10000 ;
    private boolean     _fullException = false ;
    private String      _instance      = "" ;
    private Position    _currentPosition = new Position() ;
    private boolean     _debug    = false ;

    private class Position {
        private CellPath    remote     = null ;
        private String      remoteName = null ;
        private boolean     hyperMode  = false ;
        private List        hyperPath  = new ArrayList() ;
        private String      moduleName = null ;
        private Position(){}
        private Position( Position position ){
            remote     = position.remote ;
            remoteName = position.remoteName ;
            hyperMode  = position.hyperMode ;
            hyperPath  = new ArrayList( position.hyperPath ) ;
            moduleName = position.moduleName ;
        }
        private Position( String removeCell ){
            hyperMode  = false ;
            remoteName = removeCell ;
            remote     = remoteName == null ? null : new CellPath(remoteName) ;
        }
        private void clearHyperMode(){
           hyperMode  = false ;
           remoteName = null ;
           remote     = null ;
           hyperPath  = new ArrayList() ;
           moduleName = null ;
        }
        private void gotoLocal(){
           remoteName = null ;
           remote     = null ;
           hyperPath  = new ArrayList() ;
           moduleName = null ;
        }
        private String getPrefix(){
            if( ( hyperPath == null ) || ( hyperPath.size() < 3 ) )return "" ;
            StringBuffer sb = new StringBuffer() ;
            for( int i = 2 , n = hyperPath.size() ; i < n ; i++ )
                sb.append(hyperPath.get(i).toString() ).append(" ");
            return sb.toString() ;
        }
        private void finish(){
           if( ! hyperMode  )return ;

           int size = hyperPath.size() ;

           String domainName = size > 0 ? hyperPath.get(0).toString() : null ;
           String cellName   = size > 1 ? hyperPath.get(1).toString() : null ;

           moduleName = size > 2 ? hyperPath.get(2).toString() : null ;

           if( domainName == null ){

               remoteName = null ;

           }else if( cellName == null ){

               remoteName = domainName.equals("*") ?
                            "topo" :  "System@"+domainName ;

           }else{
               remoteName = domainName.equals("*") ?
                            cellName :  cellName+"@"+domainName ;

           }


           remote = remoteName == null ? null : new CellPath(remoteName) ;

        }
       private void mergePath( Path path ){
           hyperMode = true ;
           String [] pathString = path.getPath() ;

            if( path.isAbsolutePath() ){
                hyperPath = new ArrayList() ;
                for( int i = 0 , n = pathString.length ; i<n ; i++ )
                    hyperPath.add(pathString[i]);
            }else{

                for( int i = 0 , n = pathString.length ; i<n ; i++ ){
                    String p = pathString[i];
                    if( p.equals(".") ){
                       continue ;
                    }else if( p.equals("..") ){
                       int currentSize = hyperPath.size() ;
                       if( currentSize == 0 )continue ;
                       hyperPath.remove( currentSize-1 ) ;
                    }else{
                       hyperPath.add( p ) ;
                    }
                }
            }

            finish() ;

            return;
        }
    }

    private class Path {

        private String  _pathString     = null ;
        private boolean _isAbsolutePath = false ;
        private boolean _isPath         = false ;
        private boolean _isDomain       = false ;

        private String  [] _path = null ;

        private Path( String pathString ) throws Exception {
            _pathString = pathString ;
            if( _pathString.indexOf('@') > -1 ){
                _isDomain = true ;
                StringTokenizer st = new StringTokenizer( pathString , "@" ) ;
                _path = new String[2] ;
                _path[0] = st.nextToken() ;
                _path[1] = st.nextToken() ;
            }else if( _pathString.indexOf('/') > -1 ){
                _isPath         = true ;
                _isAbsolutePath = _pathString.startsWith("/");
                StringTokenizer st = new StringTokenizer(_pathString,"/");
                int count = st.countTokens() ;
                _path = new String[count] ;
                for( int i = 0 ; i < _path.length ; i++ )_path[i] = st.nextToken() ;
            }else{
                _path = new String[1] ;
                _path[0] = _pathString;
            }
        }
        private boolean isAbsolutePath(){
            return _isAbsolutePath ;
        }
        private boolean isDomain(){
            return _isDomain ;
        }
        private boolean isPath(){
            return _isPath ;
        }
        private String getItem(int i ){
           return ( ( i < 0 ) || ( i >= _path.length ) ) ? "" : _path[i] ;
        }
        private String [] getPath(){ return _path ; }
        public String toString(){ return _pathString ;}
        public String toLongString(){
            StringBuffer sb = new StringBuffer() ;
            for( int i = 0 ; i < _path.length ; i++ ){
                sb.append("(").append(i).append(")=").append(_path[i]).append(";");
            }
            sb.append("isPath=").append(_isPath).append(";");
            sb.append("isAbsolutePath=").append(_isAbsolutePath).append(";");
            sb.append("isDomain=").append(_isDomain).append(";");
            return sb.toString() ;
        }
    }
    public HyperModeUserShell( String user , CellNucleus nucleus , Args args ){
       _nucleus  = nucleus ;
       _user     = user ;
       _authUser = user ;

       if( ( _instance = args.getOpt("realm") ) != null ){
           if(  _instance.equals("") ){
               try{
                   _instance = InetAddress.getLocalHost().getHostName() ;
               }catch(Exception ee){ }
           }else if( _instance.equals("hide") ){
               _instance = null ;
           }
       }
    }
    protected String getUser(){ return _user ; }
    public void checkPermission( String aclName )
           throws AclException {

         Object [] request = new Object[5] ;
         request[0] = "request" ;
         request[1] = "<nobody>" ;
         request[2] = "check-permission" ;
         request[3] = getUser() ;
         request[4] = aclName ;
         CellMessage reply = null ;
         try{
            reply = _nucleus.sendAndWait(
                         new CellMessage( _path , request ) ,
                         _timeout    ) ;
            if( reply == null )
               throw new
               AclException( "Request timed out ("+_path+")" ) ;
         }catch(Exception ee ){
            throw new
            AclException( "Problem : "+ee.getMessage() ) ;
         }
         Object r = reply.getMessageObject() ;
         if( ( r == null                    ) ||
             ( ! ( r instanceof Object [] ) ) ||
             (  ((Object [])r).length < 6   ) ||
             ( ! ( ((Object [])r)[5] instanceof Boolean ) ) )
             throw new
             AclException( "Protocol violation 4456" ) ;

         if( ! (((Boolean)((Object [])r)[5]).booleanValue() ) )
            throw new
            AclException( getUser() , aclName ) ;

         return ;
    }
    public String getHello(){
      return "\n    Cell System (user="+getUser()+")\n\n" ;
    }
    public String getPrompt(){
        if( _currentPosition.hyperMode ){
            StringBuffer sb = new StringBuffer() ;

            sb.append("(").append(getUser()).append(") ");
            if( _debug ){
                String remote = _currentPosition.remoteName == null ? "local" : _currentPosition.remoteName;
                sb.append("[").append(remote).append("] ");
            }
            sb.append(_instance == null ? "/" : ( "/" + _instance  ) ) ;
            Iterator it = _currentPosition.hyperPath.iterator() ;
            while( it.hasNext() )sb.append("/").append(it.next().toString());
            sb.append(" > ");
            return sb.toString();
        }else{
            return  ( _instance == null ? "" : ( "[" + _instance + "] " ) ) +
                    ( _currentPosition.remote == null ? "(local) " : ( "(" + _currentPosition.remoteName +") " ) ) +
                    getUser()+" > " ;
        }
    }
    public Object ac_logoff( Args args ) throws CommandException {
       throw new CommandExitException( "Done" , 0  ) ;
    }
    public String hh_su = "<userName>" ;
    public String ac_su_$_1( Args args )throws AclException {
        String user = args.argv(0) ;
        if( user.equals(_authUser) ){
           _user = _authUser ;
           return "User changed BACK to "+_user ;
        }else if( user.equals(_user) ){
           return "User not changed, still "+_user ;
        }
        try{
           checkPermission( "system.*.newuser" ) ;
        }catch( AclException acle ){
           checkPermission( "system."+user+".newuser" ) ;
        }
        _user = user ;
        return "User changed to "+_user ;
    }
    public String hh_set_exception = "message|detail" ;
    public String ac_set_exception_$_0_1( Args args ) throws CommandException {
       if( args.argc() > 0 ){
          if( args.argv(0).equals( "message" ) ){
             _fullException = false ;
          }else if ( args.argv(0).equals("detail") ){
             _fullException = true ;
          }else throw new
                CommandSyntaxException("set exception message|detail") ;
       }
       return "Exception = " +( _fullException ? "detail" : "message" ) ;
    }
    public String hh_set_timeout = "<timeout/sec> # command timeout in seconds";
    public String ac_set_timeout_$_0_1( Args args ){
        if( args.argc() > 0 ){
           long timeout = Integer.parseInt(args.argv(0)) * 1000L ;
           if( timeout < 1000L )
               throw new
               IllegalArgumentException("<timeout> >= 1" ) ;
           _timeout = timeout;
        }
        return "Timeout = "+(_timeout/1000L) ;

    }
    public String hh_load_shell = "system|<shellClass>" ;
    public String ac_load_shell_$_1( Args args )throws Exception {
        String shellName = args.argv(0) ;

        try{
           checkPermission( "shell.*.execute" ) ;
        }catch( AclException acle ){
           checkPermission( "shell."+shellName+".execute" ) ;
        }
        if( shellName.equals("system") ){
           addCommandListener( new CellShell( _nucleus ) ) ;
        }else{
           addCommandListener( Class.forName(shellName).newInstance() ) ;
        }
        return "" ;

    }
    //
    //   input                        remoteName
    // ----------------------------------------------------------------
    //   /                              null
    //   /*                             null [no really defined]
    //   /<domain>                      System@<domain>
    //   /<domain>/<cell>               <cell>@<domain>
    //   /*/<cells>                     <cell>
    //   /<domain>|*/<cells>/<module>  see above
    //
    public String fh_cd =
          "  SYNTAX I :\n" +
          "     cd <cellPath>\n" +
          "          <cellPath> : <cellName>[@<domainName>]\n" +
          "     The cd command will send all subsequent commands to the specified cell.\n" +
          "     Use '..' to get back to local mode\n"+
          ""+
          "  SYNTAX II : \n"+
          "     cd <cellDirectoryPath>\n"+
          "           <cellDirecotryPath> : /<domainName>[/<cellName>[/<moduleName[...]]]"+
          "     The cd command will send all subsequent commands to the specified cell resp. module.\n" +
          "     Use the standard unix directory syntax to navigate though the cell/domain realm.\n"+
          "     Simple '..' will bring you back to 'SYNTAX I'\n"+
          "       Special paths : "+
          "            /*/<cellName>     : use the * directory for wellknown cells.\n"+
          "            /local            : use the local directory for the local domain\n"+
          "            /<domain>         : if no cell is given, the system cell is selected\n"+
          "                                except for the /* director where the topo cell is\n"+
          "                                chosen, if available\n"+
          "\n" ;
    public String hh_cd = "<cellPath> | <cellDirectoryPath> # see 'help cd'";
    public String ac_cd_$_1( Args args )throws Exception {

       String remoteCell = args.argv(0) ;
       Path   path       = new Path(remoteCell);

       Position newPosition = null ;

       if( path.isDomain() ){
            //
           // switch back do domain mode (hyper mode or not)
           //
           newPosition = new Position( remoteCell ) ;

       }else if( _currentPosition.hyperMode ){
           //
           // we are and stay in hyper mode
           //
           newPosition = new Position( _currentPosition ) ;
           newPosition.mergePath( path ) ;

       }else if( path.isPath() ){

           if( path.isAbsolutePath() ){

               newPosition = new Position( _currentPosition ) ;
               newPosition.mergePath( path ) ;

           }else{
               //
               // not hyper mode, got a path but not an absolute one.
               // so we wouldn't know what to do.
               //
              throw new
              IllegalArgumentException("Need absolute path to switch to directory mode");
           }

        }else{
           //
           //
           //
           newPosition = new Position( remoteCell ) ;

       }

       if( newPosition.remoteName != null )checkCdPermission( newPosition.remoteName ) ;

       synchronized( this ){
           _currentPosition = newPosition ;
       }


       return "" ;
    }
    private void checkCdPermission( String remoteName ) throws AclException {
       int pos = remoteName.indexOf('-') ;
       String prefix = null ;
       if( pos > 0 )prefix = remoteName.substring(0,pos);
       try{
         checkPermission( "cell.*.execute" ) ;
       }catch( AclException acle ){
          try{
             checkPermission( "cell."+remoteName+".execute" ) ;
          }catch( AclException acle2 ){
             if( prefix == null )throw acle2 ;
             checkPermission( "cell."+prefix+"-pools.execute" ) ;
          }
       }
    }
    protected Object executeLocalCommand( Args args ) throws Exception {
       _log.info( "Local command "+args ) ;
       try{
          return  command( args ) ;
       }catch( CommandThrowableException cte ){
          throw (Exception)cte.getTargetException() ;
       }catch( CommandPanicException cpe ){
          throw (Exception)cpe.getTargetException() ;
       }
    }
    /**
      * Called by the streamobjectcell if it is running in
      * Binary mode and a destination is provided.
      */
    public Object executeCommand( String destination , Object object )
           throws Exception {

       _log.info( "Object command (dest="+destination+";Class="+object.getClass().getName()+") "+object) ;

       checkCdPermission( destination ) ;

       if( object instanceof String ){

          return sendCommand( destination  , object.toString() ) ;

       }else{

          CellMessage res = new CellMessage( new CellPath( destination ) , object ) ;

          res = _nucleus.sendAndWait( res , _timeout ) ;

          if( res == null )throw new Exception("Request timed out" ) ;

          return res.getMessageObject() ;
       }
    }
    /**
      * This is called by the streamobjectcell if it is running
      * in
      *   i) Ascii (ssh) mode or
      *  ii) Binary mode and no destination is given unless there would
      *      be a executeCommand( Obj obj), this would then be taken instead.
      *
      */
    public Object executeCommand( String str )throws Exception {
       _log.info( "String command (super) "+str ) ;

       if( str.trim().equals("") )return "" ;

       if( str.equals("..") ){
          _currentPosition.clearHyperMode() ;
          _currentPosition.gotoLocal() ;
          return "" ;
       }

       Args args = new Args( str ) ;

       if( _currentPosition.remote == null ){
           return localCommand( args ) ;
       }else{
           if( _currentPosition.hyperMode ){
              if( ( args.argc() == 1 ) && ( args.argv(0).equals("cd") ) ){
                  _currentPosition.gotoLocal() ;
                  return "" ;
              }else if( ( args.argc() > 1 ) && ( args.argv(0).equals("cd") ) ){
                  return localCommand( args ) ;
              }
              String prefix = _currentPosition.getPrefix() ;
              if( prefix.length() > 0 ){
                  if( ( args.argc() >= 1 ) && ( args.argv(0).equals("help") ) ){
                      if( args.argc() == 1 )str = "help "+prefix;
                  }else{
                      str = prefix + " " + str ;
                  }
              }
           }
           return sendObject( _currentPosition.remote ,  new AuthorizedString(_user,str) ) ;
       }
    }
    private Object localCommand( Args args ) throws Exception {
       try{
           Object or = executeLocalCommand( args ) ;
           if( or == null )return ""  ;
           String r = or.toString() ;
           if(  r.length() < 1)return "" ;
           if( r.substring(r.length()-1).equals("\n" ) )
              return r   ;
           else
              return r + "\n"  ;
       }catch(Exception ee ){
           _log.debug(ee.toString(), ee) ;
           throw ee ;
       }

    }
    protected Object sendObject( String cellPath , Object object )
       throws Exception
   {
       return sendObject( new CellPath( cellPath ) , object ) ;
   }
    protected Object sendObject( CellPath cellPath , Object object )
       throws Exception
   {

        CellMessage res =
              _nucleus.sendAndWait(
                   new CellMessage( cellPath , object ) ,
              _timeout ) ;
          if( res == null )throw new Exception("Request timed out" ) ;
          Object obj =  res.getMessageObject() ;
          if( ( obj instanceof Throwable ) && _fullException ){
              CharArrayWriter ca = new CharArrayWriter() ;
              ((Throwable)obj).printStackTrace(new PrintWriter(ca)) ;
              return ca.toString();
          }
          return obj ;

    }
    protected Object sendCommand( String destination , String command )
       throws Exception
   {

        CellPath     cellPath = new CellPath(destination);
        AuthorizedString auth = new AuthorizedString( _user , command ) ;

        CellMessage res =
              _nucleus.sendAndWait(  new CellMessage( cellPath , auth ) , _timeout ) ;

          if( res == null )
             throw new
             Exception("Request timed out" ) ;

          Object obj =  res.getMessageObject() ;

          if( ( obj instanceof Throwable ) && _fullException ){
              CharArrayWriter ca = new CharArrayWriter() ;
              ((Throwable)obj).printStackTrace(new PrintWriter(ca)) ;
              return ca.toString();
          }
          return obj ;

    }

}
