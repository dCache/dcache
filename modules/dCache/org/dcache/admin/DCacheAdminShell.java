// $Id: DCacheAdminShell.java,v 1.1 2007-01-03 15:42:05 patrick Exp $

package org.dcache.admin ;

import dmg.cells.services.login.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;

import diskCacheV111.util.*;
import diskCacheV111.vehicles.*;
import diskCacheV111.pools.PoolV2Mode;

import java.util.* ;
import java.io.* ;
import java.net.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 20 Nov 2006
  */
public class      DCacheAdminShell 
       extends    dmg.cells.services.login.user.HyperModeUserShell {


    private CellNucleus _nucleus = null ;
    
    public DCacheAdminShell( String user , CellNucleus nucleus , Args args ){
    
        super( user , nucleus , args ) ;
        
        _nucleus = nucleus ;
    }


    public String getHello(){
      return "\n    Cell System (user="+getUser()+")\n\n" ;
    }
    public String hh_space  = "reserve|free <poolName> <bytes>" ;
    public Object ac_space_$_3( Args args ) throws Exception {
    
       String mode     = args.argv(0) ;
       String poolName = args.argv(1) ;
       long   bytes    = Long.parseLong(args.argv(2));
       
        try{
           checkPermission( "pool.*.execute" ) ;
        }catch( AclException acle ){
           checkPermission( "pool."+poolName+".execute" ) ;
        }
        
        PoolSpaceReservationMessage msg = null ;
        if( mode.equals("reserve") ){
            msg = new PoolReserveSpaceMessage( poolName , bytes ) ;
        }else if( mode.equals("free") ){
            msg = new PoolFreeSpaceReservationMessage( poolName , bytes ) ;
        }else
           throw new 
           IllegalArgumentException("Usage : space reserve|free <poolName> <bytes>");
         
        Object result = sendObject( poolName , msg ) ;
        
       return result.toString() ;
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
       
       PnfsFlagReply reply = setPnfsFlag( destination , "s" , target, mode) ;
       
       PnfsId pnfsId = reply.getPnfsId() ;
          
       PnfsGetCacheLocationsMessage pnfsMessage = 
                    new PnfsGetCacheLocationsMessage(pnfsId) ;
    
       pnfsMessage = (PnfsGetCacheLocationsMessage)sendObject("PnfsManager",pnfsMessage) ;
       if( pnfsMessage.getReturnCode() != 0 )
         throw new
         FileNotFoundException( destination ) ;
       
       List tmplist = pnfsMessage.getCacheLocations() ;
       List<String> list = new ArrayList<String>();
       if( verbose )sb.append("Location(s) : ") ;
       for( Iterator i = tmplist.iterator() ; i.hasNext() ; ){
          String poolname = i.next().toString() ;
          if( verbose )sb.append( poolname ).append(",") ;
          list.add(poolname);
       }
       if( verbose )sb.append("\n");
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
       Iterator<String> i = list.iterator() ;
       while( i.hasNext() ){ 
           String poolName = i.next();
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
         
       List<String> list = new ArrayList<String>();
       List tmplist = pnfsMessage.getCacheLocations() ;
       if( verbose )sb.append("Location(s) : ") ;
       for( Iterator i = tmplist.iterator() ; i.hasNext() ; ){
          String poolName = i.next().toString() ;
          list.add( poolName);
          if( verbose )sb.append(poolName).append(",") ;
       }
       if( verbose )sb.append("\n");
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
       PoolRemoveFilesMessage remove = null ;
       Iterator<String> i = list.iterator() ;
       while( i.hasNext() ){ 
           String poolName = i.next() ;
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
        String value,
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
           mode?PnfsFlagMessage.FlagOperation.SET:
               PnfsFlagMessage.FlagOperation.REMOVE ) ;
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



}
