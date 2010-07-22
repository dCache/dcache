// $Id: UserAdminShell.java,v 1.16 2006-08-22 00:11:09 timur Exp $

package diskCacheV111.admin ;

import java.io.CharArrayWriter;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolLinkInfo;
import diskCacheV111.vehicles.PoolMgrGetPoolByLink;
import diskCacheV111.vehicles.PoolMgrGetPoolLinks;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.QuotaMgrCheckQuotaMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellShell;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.AclException;
import dmg.util.Args;
import dmg.util.AuthorizedString;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 1 May 2001
  */
public class      UserAdminShell
//       extends    dmg.cells.services.login.user.MinimalAdminShell {
       extends    CommandInterpreter {

    private static final Logger _log =
        LoggerFactory.getLogger(UserAdminShell.class);

    private static final String ADMIN_COMMAND_NOOP = "xyzzy";
    private static final int CD_PROBE_MESSAGE_TIMEOUT_MS = 1000;

    private CellNucleus _nucleus  = null ;
    private String      _user     = null ;
    private String      _authUser = null ;
    private CellPath    _path     = new CellPath("acm");
    private long        _timeout  = 10000 ;
    private boolean     _fullException = false ;
    private final String      _instance ;
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
        @Override
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
    public UserAdminShell( String user , CellNucleus nucleus , Args args ){
       _nucleus  = nucleus ;
       _user     = user ;
       _authUser = user ;

       String prompt = args.getOpt("dCacheInstance");
       if( prompt == null || !prompt.equals("hide") ){
           if( prompt == null || prompt.length() == 0 ){
               try{
                   prompt = InetAddress.getLocalHost().getHostName() ;
               }catch(UnknownHostException ee){
                   prompt = null;
               }
           }
           _instance = prompt;
       }else{
           _instance = null;
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
      return "\n    dCache Admin (VII) (user="+getUser()+")\n\n" ;
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
    public String ac_su_$_1( Args args )throws Exception {
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
    public String hh_getpoolbylink = "<linkName> [-size=<filesize>] [-service=<serviceCellName]" ;
    public String ac_getpoolbylink_$_1( Args args ) throws Exception {

       String linkName   = args.argv(0) ;
       String service    = args.getOpt("service");
       String sizeString = args.getOpt("size") ;

       PoolMgrGetPoolByLink msg = new PoolMgrGetPoolByLink(  linkName ) ;
       if( sizeString != null )msg.setFilesize( Long.parseLong( sizeString ) ) ;
       service = service == null ? "PoolManager" : service ;

       Object result = sendObject(  service , msg ) ;
       if( result == null )
           throw new
           Exception("QuotaRequest timed out");

       if( result instanceof PoolMgrGetPoolByLink ){
          PoolMgrGetPoolByLink link = (PoolMgrGetPoolByLink)result ;
          int rc = link.getReturnCode() ;
          if( rc != 0 ){
              return "Problem "+rc+" <"+link.getErrorObject()+"> reported for link "+linkName ;
          }else{
              return "Pool <"+link.getPoolName()+"> selected for link "+linkName ;
          }
       }
       return "Unexpected class "+result.getClass().getName()+
                  " arrived with message "+result.toString();
    }
    public String hh_quota_query  = "<storageClassName>|* [-l] [-service=<serviceCellName>]" ;
    public Object ac_quota_query_$_1( Args args ) throws Exception {

       String storageClassName  = args.argv(0) ;
       String service           = args.getOpt("service");
       service = service == null ? "QuotaManager" : service ;
       boolean extended         = args.getOpt("l") != null ;

       Message msg = null ;

       if( storageClassName.equals("*" ) ){
           msg = new PoolMgrGetPoolLinks() ;
       }else{
           msg = new QuotaMgrCheckQuotaMessage( storageClassName ) ;
       }

       Object result = sendObject(  service , msg ) ;
       if( result == null )
           throw new
           Exception("QuotaRequest timed out");

       if(  result instanceof QuotaMgrCheckQuotaMessage  ){
          return result.toString() ;
       }else if( result instanceof PoolMgrGetPoolLinks ){
          if(extended){
            PoolMgrGetPoolLinks info  = (PoolMgrGetPoolLinks)result ;
            PoolLinkInfo   []   links = info.getPoolLinkInfos() ;
            StringBuffer        sb    = new StringBuffer() ;
            if( links == null )return "Object doesn't contain a Links list" ;

            for(int i=0 , n = links.length ; i<n ; ++i ){
               PoolLinkInfo l = links[i] ;
               sb.append(" Link ").append(l.getName()).append(" : ").append(l.getAvailableSpaceInBytes()).append("\n");
               String [] storageGroups = l.getStorageGroups() ;
               if( storageGroups == null )continue ;
               for( int sg = 0 ; sg < storageGroups.length ; sg++ ){
                  sb.append("    ").append(storageGroups[sg]).append("\n");
               }
            }
            return sb.toString();
          }
          return result.toString() ;
       }
       return "Unexpected class "+result.getClass().getName()+
                  " arrived with message "+result.toString();

    }

    public String hh_set_sticky = "<pnfsId>|<globalPath> [-target=<target>] [-silent]" ;
    public Object ac_set_sticky_$_1( Args args ) throws Exception {
       return setSticky(
             args.argv(0) ,
             args.getOpt("target") ,
             true ,
             args.getOpt("silent") == null ? new StringBuffer() : null ) ;
    }
    public String hh_set_unsticky = "<pnfsId>|<globalPath> [-target=<target>] [-silent]" ;
    public Object ac_set_unsticky_$_1( Args args ) throws Exception  {
       return setSticky(
             args.argv(0) ,
             args.getOpt("target") ,
             false ,
             args.getOpt("silent") == null ? new StringBuffer() : null ) ;
    }
    public String hh_uncache = "<pnfsId>|<globalPath> [-target=<target>] [-silent]" ;
    public Object ac_uncache_$_1( Args args ) throws Exception {
      try{
       return uncache(
             args.argv(0) ,
             args.getOpt("target") ,
             args.getOpt("silent") == null ? new StringBuffer() : null ) ;
      }catch(Exception ee ){
         ee.printStackTrace();
         throw ee ;
      }
    }
    private String setSticky(
               String destination ,
               String target ,
               boolean mode ,
               StringBuffer sb )
            throws Exception {

       if( ( target == null ) || ( target.equals("") ) )target = "*";

       boolean verbose = sb != null ;

       PnfsFlagReply reply = setPnfsFlag( destination , "s" , target, mode ) ;

       PnfsId pnfsId = reply.getPnfsId() ;

       PnfsGetCacheLocationsMessage pnfsMessage =
                    new PnfsGetCacheLocationsMessage(pnfsId) ;

       pnfsMessage = (PnfsGetCacheLocationsMessage)sendObject("PnfsManager",pnfsMessage) ;
       if( pnfsMessage.getReturnCode() != 0 )
         throw new
         FileNotFoundException( destination ) ;

       List<String> list = pnfsMessage.getCacheLocations() ;
       if( verbose ){
          sb.append("Location(s) : ") ;
          for( String location : list ){
             sb.append(location).append(",") ;
          }
          sb.append("\n");
       }
       if( target.equals("*") ){
          if( verbose )sb.append("Selection : <all>\n");
       }else if( list.contains(target) ){
          if( verbose )sb.append("Selection : ").append(target).append("\n") ;
          list = new ArrayList<String>();
          list.add(target);
       }else{
          if( verbose )sb.append("Selection : <nothing>\n") ;
          return sb == null ? "" : sb.toString() ;
       }
       PoolSetStickyMessage sticky = null ;

       for( String poolName: list ){
           if( verbose )sb.append(poolName).append(" : ") ;
           try{
              sticky = new PoolSetStickyMessage( poolName , pnfsId , mode ) ;
              sticky = (PoolSetStickyMessage)sendObject(poolName,sticky) ;
              if( verbose ){
                 int rc = sticky.getReturnCode() ;
                 if( rc != 0 )sb.append("[").append(rc).append("] ").
                                 append(sticky.getErrorObject().toString()) ;
                 else sb.append("ok");
              }
           }catch(Exception ee ){
              if(verbose)sb.append(ee.getMessage()) ;
           }
           if(verbose)sb.append("\n") ;
       }

       return sb == null ? "" : sb.toString() ;
    }
    private String uncache(  String destination ,  String target , StringBuffer sb )
            throws Exception {

       if( ( target == null ) || ( target.equals("") ) )target = "*";

       boolean verbose = sb != null ;

       PnfsId pnfsId = null ;
       if( destination.startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( destination ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception )throw (Exception)o ;
             else throw new Exception( o.toString() ) ;
          }

          if( ( pnfsId = map.getPnfsId() ) == null )
            throw new
            FileNotFoundException( destination ) ;

       }else{
          pnfsId = new PnfsId( destination ) ;
       }

       int dbId = pnfsId.getDatabaseId() ;

       try{
          checkPermission( "pool.*.uncache" ) ;
       }catch( AclException ee ){
          checkPermission( "pool."+dbId+".uncache" ) ;
       }

       PnfsGetCacheLocationsMessage pnfsMessage =
                    new PnfsGetCacheLocationsMessage(pnfsId) ;

       pnfsMessage = (PnfsGetCacheLocationsMessage)sendObject("PnfsManager",pnfsMessage) ;

       if( pnfsMessage.getReturnCode() != 0 )
         throw new
         FileNotFoundException( destination ) ;

       List list = pnfsMessage.getCacheLocations() ;
       if( verbose ){
          sb.append("Location(s) : ") ;
          for( Iterator i = list.iterator() ; i.hasNext() ; ){
             sb.append(i.next().toString()).append(",") ;
          }
          sb.append("\n");
       }
       if( target.equals("*") ){
          if( verbose )sb.append("Selection : <all>\n");
       }else if( list.contains(target) ){
          if( verbose )sb.append("Selection : ").append(target).append("\n") ;
          list = new ArrayList();
          list.add(target);
       }else{
          if( verbose )sb.append("Selection : <nothing>\n") ;
          return sb == null ? "" : sb.toString() ;
       }
       PoolRemoveFilesMessage remove = null ;
       Iterator i = list.iterator() ;
       while( i.hasNext() ){
           String poolName = i.next().toString() ;
           if( verbose )sb.append(poolName).append(" : ") ;
           try{
              remove = new PoolRemoveFilesMessage( poolName ) ;
              String  [] filelist  = { pnfsId.toString() } ;
              remove.setFiles(filelist);
              remove = (PoolRemoveFilesMessage)sendObject(poolName,remove) ;
              if( verbose ){
                 int rc = remove.getReturnCode() ;
                 if( rc != 0 ){
                     Object obj = remove.getErrorObject() ;
                     if( ( obj != null ) && ( obj instanceof Object [] ) ){
                        Object o = ((Object [])obj)[0] ;
                        if( o != null )
                        sb.append("[").append(rc).append("] Failed ").
                                 append(o.toString()) ;
                     }else if( obj != null ){
                        sb.append("[").append(rc).append("] Failed ").
                                 append(obj.toString()) ;
                     }

                 }else
                     sb.append("ok");

              }
           }catch(Exception ee ){
              if(verbose)sb.append(ee.getMessage()) ;
           }
           if(verbose)sb.append("\n") ;
       }

       return sb == null ? "" : sb.toString() ;
    }
    private class PnfsFlagReply {
       private PnfsId          _pnfsId = null ;
       private PnfsFlagMessage _message = null ;
       public PnfsFlagReply( PnfsId pnfsId , PnfsFlagMessage message ){
          _pnfsId  = pnfsId ;
          _message = message ;
       }
       public PnfsId getPnfsId(){ return _pnfsId ; }
       public PnfsFlagMessage getPnfsFlagMessage(){ return _message ; }
    }
    public String hh_flags_set = "<pnfsId>|<globalPath> <key> <value>";
    public Object ac_flags_set_$_3( Args args ) throws Exception {

       String destination   = args.argv(0) ;
       String key    = args.argv(1) ;
       String value  = args.argv(2) ;

       PnfsFlagMessage result =
           setPnfsFlag( destination , key , value, true ).getPnfsFlagMessage() ;

       return result.getReturnCode() == 0 ? "" : result.getErrorObject().toString() ;

    }
    private PnfsFlagReply setPnfsFlag(
        String destination ,
        String key ,
        String value ,
        boolean mode)
            throws Exception {

       PnfsId pnfsId = null ;
       if( destination.startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( destination ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception )throw (Exception)o ;
             else throw new Exception( o.toString() ) ;
          }

          pnfsId = map.getPnfsId() ;
          if( ( pnfsId = map.getPnfsId() ) == null )
            throw new
            FileNotFoundException( destination ) ;


       }else{
          pnfsId = new PnfsId( destination ) ;
       }

       int dbId = pnfsId.getDatabaseId() ;

       try{
          checkPermission( "pnfs.*.update" ) ;
       }catch( AclException ee ){
          checkPermission( "pnfs."+key+"."+dbId+".update" ) ;
       }


       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , key,
           mode?PnfsFlagMessage.FlagOperation.SET:PnfsFlagMessage.FlagOperation.REMOVE ) ;
       pfm.setValue( value ) ;

       PnfsFlagMessage result = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;
       if( result.getReturnCode() != 0 ){
          Object o = result.getErrorObject() ;
          if( o instanceof Exception )throw (Exception)o ;
          else throw new Exception( o.toString() ) ;
       }

       return new PnfsFlagReply( pnfsId , result ) ;
    }
    public String hh_flags_remove = "<pnfsId> <key>";
    public Object ac_flags_remove_$_2( Args args ) throws Exception {
       PnfsId pnfsId = null ;
       if( args.argv(0).startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( args.argv(0) ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception )throw (Exception)o ;
             else throw new Exception( o.toString() ) ;
          }

          pnfsId = map.getPnfsId() ;


       }else{
          pnfsId = new PnfsId( args.argv(0) ) ;
       }

       int dbId = pnfsId.getDatabaseId() ;

       String key    = args.argv(1) ;

       try{
          checkPermission( "pnfs.*.update" ) ;
       }catch( AclException ee ){
          checkPermission( "pnfs."+key+"."+dbId+".update" ) ;
       }


       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , key, PnfsFlagMessage.FlagOperation.REMOVE ) ;

       PnfsFlagMessage result = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;
       if( result.getReturnCode() != 0 ){
          Object o = result.getErrorObject() ;
          if( o instanceof Exception )throw (Exception)o ;
          else throw new Exception( o.toString() ) ;
       }
       return result.getReturnCode() == 0 ? "" : result.getErrorObject().toString() ;
    }
    public String hh_p2p = "<pnfsId> [<sourcePool> <destinationPool>] [-ip=<address]" ;
    public String ac_p2p_$_1_3( Args args )throws Exception {

       if( args.argc() >= 3 ){
           String source = args.argv(1) ;
           String dest   = args.argv(2) ;
           PnfsId pnfsId = new PnfsId( args.argv(0) ) ;

           Pool2PoolTransferMsg p2p =
                new Pool2PoolTransferMsg( source , dest , pnfsId , null ) ;


           _nucleus.sendMessage(
                       new CellMessage( new CellPath(dest) , p2p )
                               ) ;

           return "P2p of "+pnfsId+" initiated from "+source+" to "+dest ;
       }else{
           PnfsId pnfsId = new PnfsId(args.argv(0) ) ;
           String ip     = args.getOpt("ip");


           PnfsGetStorageInfoMessage stinfo =
               new PnfsGetStorageInfoMessage( pnfsId  ) ;

           CellMessage  msg = new CellMessage( new CellPath("PnfsManager") , stinfo ) ;
           msg = _nucleus.sendAndWait( msg , 30000L )  ;
           if( msg == null )
               throw new
               Exception("Get storageinfo timed out");

           if( stinfo.getReturnCode() != 0 )
               throw new
               IllegalArgumentException("getStorageInfo returned "+stinfo.getReturnCode());

           stinfo = (PnfsGetStorageInfoMessage)msg.getMessageObject() ;
           StorageInfo storageInfo = stinfo.getStorageInfo() ;

           DCapProtocolInfo pinfo =
            new DCapProtocolInfo("DCap",0,0,"localhost",0);


          PoolMgrReplicateFileMsg select =
              new PoolMgrReplicateFileMsg(pnfsId,storageInfo,pinfo,0L);

          msg = new CellMessage( new CellPath("PoolManager"),select ) ;

          String timeoutString = args.getOpt("timeout");
          long timeout = timeoutString != null ?
                         Long.parseLong(timeoutString)*1000L :
                         60000L ;

          msg = _nucleus.sendAndWait( msg , timeout ) ;

          select = (PoolMgrReplicateFileMsg)msg.getMessageObject() ;
          if( select == null )
              throw new
              Exception("p2p request timed out");

          if( select.getReturnCode() != 0 )
              throw new
              Exception("Problem return from 'p2p' : ("+select.getReturnCode()+
              ") "+select.getErrorObject());

          return "p2p -> "+select.getPoolName() ;
       }
    }
    public String ac_modify_poolmode =
        " a) modify poolmode enable <poolname>[,<poolname>...]\n"+
        " b) modify poolmode [OPTIONS] disable <poolname>[,<poolname>...] [<code> [<message>]]\n"+
        "      OPTIONS :\n"+
        "        -fetch    #  disallows fetch (transfer to client)\n"+
        "        -stage    #  disallows staging (from HSM)\n"+
        "        -store    #  disallows store (transfer from client)\n"+
        "        -p2p-client\n"+
        "        -rdonly   #  := store,stage,p2p-client\n"+
        "        -strict   #  := disallows everything\n" ;
    public String hh_modify_poolmode =
        "enable|disable <poolname>[,<poolname>...] [<code> [<message>]] [-strict|-stage|-rdonly|-fetch|-store]" ;
    public String ac_modify_poolmode_$_2_4( Args args ) throws Exception {

       checkPermission( "*.*.*" ) ;

       String enable   = args.argv(0) ;
       String poolList = args.argv(1) ;
       String message  = args.argc() > 3 ? args.argv(3) : null ;
       int    code     = args.argc() > 2 ? Integer.parseInt(args.argv(2)) : 0 ;

       PoolV2Mode mode = new PoolV2Mode() ;

       if( enable.equals("disable") ){

          int modeBits = PoolV2Mode.DISABLED ;
          if( args.getOpt("strict")     != null )modeBits |= PoolV2Mode.DISABLED_STRICT ;
          if( args.getOpt("stage")      != null )modeBits |= PoolV2Mode.DISABLED_STAGE ;
          if( args.getOpt("fetch")      != null )modeBits |= PoolV2Mode.DISABLED_FETCH ;
          if( args.getOpt("store")      != null )modeBits |= PoolV2Mode.DISABLED_STORE ;
          if( args.getOpt("p2p-client") != null )modeBits |= PoolV2Mode.DISABLED_P2P_CLIENT ;
          if( args.getOpt("p2p-server") != null )modeBits |= PoolV2Mode.DISABLED_P2P_SERVER ;
          if( args.getOpt("rdonly")     != null )modeBits |= PoolV2Mode.DISABLED_RDONLY ;

          mode.setMode(modeBits) ;

       }else if( enable.equals("enable") ){

       }else
          throw new
          CommandSyntaxException("Invalid keyword : "+enable) ;

       StringTokenizer       st     = new StringTokenizer(poolList,",");
       PoolModifyModeMessage modify = null ;
       StringBuffer          sb     = new StringBuffer() ;
       sb.append("Sending new pool mode : ").append(mode).append("\n");
       while( st.hasMoreTokens() ){
          String poolName = st.nextToken() ;
          modify = new PoolModifyModeMessage(poolName,mode);
          modify.setStatusInfo(code,message);
          sb.append("  ").append(poolName).append(" -> ") ;
          try{

             modify = (PoolModifyModeMessage)sendObject( poolName , modify ) ;
          }catch(Exception ee ){
             sb.append(ee.getMessage()).append("\n");
             continue ;
          }
          if( modify.getReturnCode() != 0 ){
             sb.append(modify.getErrorObject().toString()).append("\n") ;
             continue ;
          }
          sb.append("OK\n");
       }
       return sb.toString() ;
    }
    public String hh_set_deletable = "<pnfsId> # DEBUG for advisory delete (srm)" ;
    public String ac_set_deletable_$_1( Args args ) throws Exception {

       checkPermission( "*.*.*" ) ;

       PnfsId       pnfsId = new PnfsId(args.argv(0));
       StringBuffer sb     = new StringBuffer() ;

       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , "d", PnfsFlagMessage.FlagOperation.SET ) ;
       pfm.setValue("true");

       try{
          pfm = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;
       }catch(Exception ee ){
          sb.append("Attempt to set 'd' flag reported an Exception : "+ee ) ;
          sb.append("\n");
          sb.append("Operation aborted\n");
          return sb.toString();
       }
       if( pfm.getReturnCode() != 0 ){
          sb.append("set 'd' flag reported  : "+pfm.getErrorObject());
          return sb.toString() ;
       }

       sb.append("Setting 'd' succeeded\n");

       PnfsGetCacheLocationsMessage locations = new PnfsGetCacheLocationsMessage(pnfsId) ;
       try{
          locations = (PnfsGetCacheLocationsMessage)sendObject( "PnfsManager" , locations ) ;
       }catch(Exception ee ){
          sb.append("Attempt to get cache locations reported an Exception : "+ee ) ;
          sb.append("\n");
          sb.append("Operation aborted\n");
          return sb.toString() ;
       }
       if( locations.getReturnCode() != 0 ){
          sb.append("Problem in getting cache location(s) : "+locations.getErrorObject());
          return sb.toString() ;
       }
       List assumedLocations = locations.getCacheLocations() ;
       sb.append("Assumed cache locations : ").append(assumedLocations.toString()).append("\n");

       for( Iterator i = assumedLocations.iterator() ; i.hasNext() ;  ){
          String poolName = i.next().toString();
          PoolModifyPersistencyMessage p =
              new PoolModifyPersistencyMessage( poolName , pnfsId , false ) ;

          try{
             p = (PoolModifyPersistencyMessage)sendObject( poolName , p ) ;
          }catch(Exception ee ){
             sb.append("Attempt to contact ").
                append(poolName).
                append(" reported an Exception : ").
                append( ee.toString() ).
                append("\n").
                append("  Operation continues\n");
             continue ;
          }
          if( locations.getReturnCode() != 0 ){
             sb.append("Set 'cached' reply from ").
                append(poolName).
                append(" : ").
                append(p.getErrorObject()).
                append("\n");
          }else{
             sb.append("Set 'cached' OK for ").
                append(poolName).
                append("\n");
          }
       }
       return sb.toString() ;

    }
    public String hh_flags_ls = "<pnfsId> <key>";
    public Object ac_flags_ls_$_2( Args args ) throws Exception {
       PnfsId pnfsId = null ;
       if( args.argv(0).startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( args.argv(0) ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception )throw (Exception)o ;
             else throw new Exception( o.toString() ) ;
          }

          pnfsId = map.getPnfsId() ;


       }else{
          pnfsId = new PnfsId( args.argv(0) ) ;
       }
       String key    = args.argv(1) ;

       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , key, PnfsFlagMessage.FlagOperation.GET ) ;

       PnfsFlagMessage result = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;

       return result.getReturnCode() == 0 ?
              ( key+" -> "+result.getValue()) :
              result.getErrorObject().toString() ;
    }
    public String hh_pnfs_map = "<globalPath>" ;
    public String ac_pnfs_map_$_1( Args args )throws Exception {

       if( ! args.argv(0).startsWith( "/pnfs" ) )
          throw new
          IllegalArgumentException("not a global dCache path (/pnfs...)") ;

       PnfsMapPathMessage map = new PnfsMapPathMessage( args.argv(0) ) ;

       map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

       if( map.getReturnCode() != 0 ){
          Object o = map.getErrorObject() ;
          if( o instanceof Exception )throw (Exception)o ;
          else throw new Exception( o.toString() ) ;
       }

       return map.getPnfsId().toString() ;

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

       if( newPosition.remoteName != null ) {
           checkCdPermission( newPosition.remoteName ) ;
           checkCellExists( newPosition.remote);
       }

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
    private void checkCellExists( CellPath remoteCell) {
        CellMessage checkMsg = new CellMessage( remoteCell , ADMIN_COMMAND_NOOP);
        try {
            _nucleus.sendAndWait( checkMsg, CD_PROBE_MESSAGE_TIMEOUT_MS);
        } catch (SerializationException e) {
            throw new RuntimeException("Failed to serialise test message", e);
        } catch (NoRouteToCellException e) {
            throw new IllegalArgumentException("Cannot cd to this cell as it doesn't exist");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
    private Object sendObject( String cellPath , Object object )
       throws Exception
   {
       return sendObject( new CellPath( cellPath ) , object ) ;
   }
    private Object sendObject( CellPath cellPath , Object object )
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

        CellPath cellPath = new CellPath(destination);
        CellMessage res =
              _nucleus.sendAndWait(
                   new CellMessage( cellPath ,
                                    new AuthorizedString( _user ,
                                                          command)
                                  ) ,
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
    public Object executeCommand( String destination , Object str )
           throws Exception {

       _log.info( "Object command ("+destination+") "+str) ;

       return sendCommand( destination  , str.toString() ) ;
    }

}
