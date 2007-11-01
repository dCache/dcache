// $Id: HsmStorageInterpreter.java,v 1.7 2006-04-03 05:36:39 patrick Exp $

package diskCacheV111.pools ;

import java.util.* ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import diskCacheV111.util.* ;

public class HsmStorageInterpreter {

   private CellAdapter        _cell           = null ;
   private HsmStorageHandler2 _storageHandler = null ;
   private JobScheduler       _fetchQueue     = null ;
   private JobScheduler       _storeQueue     = null ;
   public HsmStorageInterpreter( CellAdapter cell , 
                                 HsmStorageHandler2 handler ){
                                 
                                 
      _cell           = cell ;  
      _storageHandler = handler ;   
      _fetchQueue     = _storageHandler.getFetchScheduler() ;                     
      _storeQueue     = _storageHandler.getStoreScheduler() ;                     
   }
    //////////////////////////////////////////////////////
    //
    //   restoreHANDLER
    //
    public String hh_rh_set_timeout = "<timeout/seconds>" ;
    public String ac_rh_set_timeout_$_1( Args args ){
       long timeout = Long.parseLong(args.argv(0))*1000L;
       _storageHandler.setTimeout(-1,timeout) ;
       return "" ;
    }
    public String hh_rh_set_max_active = "<maxActiveHsmMovers>" ;
    public String ac_rh_set_max_active_$_1( Args args )throws Exception {
        int active = Integer.parseInt( args.argv(0) ) ;
        if( active < 0 )
           throw new
           IllegalArgumentException("<maxActiveRestores> must be >= 0") ;
         _fetchQueue.setMaxActiveJobs( active ) ;
           
        return "Max Active Hsm Restore Processes set to "+active ;
    }
    public String hh_rh_jobs_ls = "" ;
    public String ac_rh_jobs_ls( Args args )throws Exception {
       return _fetchQueue.printJobQueue(null).toString() ;
    }
    public String hh_rh_jobs_remove = "<jobId>" ;
    public String ac_rh_jobs_remove_$_1( Args args )throws Exception {
       int id = Integer.parseInt( args.argv(0) ) ;
       _fetchQueue.remove( id ) ;
       return "Removed" ;
    }
    public String hh_rh_jobs_kill = "<jobId>" ;
    public String ac_rh_jobs_kill_$_1( Args args )throws Exception {
       int id = Integer.parseInt( args.argv(0) ) ;
       _fetchQueue.kill( id ) ;
       return "Kill initialized" ;
    }
    public String hh_rh_ls = "[<pnfsId>]" ;
    public String ac_rh_ls( Args args )throws Exception {
        Iterator     e  = _storageHandler.getRestorePnfsIds() ;
        StringBuffer sb = new StringBuffer() ;
        while( e.hasNext() ){
           PnfsId pnfsId = (PnfsId)e.next() ;
           
           HsmStorageHandler2.Info info = _storageHandler.getRestoreInfoByPnfsId(pnfsId) ;
           
           if( info == null ){
              sb.append(pnfsId).append("  <zombie>\n") ;
           }else{
              sb.append(pnfsId).append("  ").append(info.getListenerCount()).
                 append("   ").append( new Date(info.getStartTime()).toString() ).append("\n") ;
           }
           
        }
        return sb.toString();
    }
    //////////////////////////////////////////////////////
    //
    //   storeHANDLER
    //
    public String hh_st_set_timeout = "<timeout/seconds>" ;
    public String ac_st_set_timeout_$_1( Args args ){
       long timeout = Long.parseLong(args.argv(0))*1000L;
       _storageHandler.setTimeout(timeout,-1) ;
       return "" ;
    }
    public String hh_st_set_max_active = "<maxActiveHsmMovers>" ;
    public String ac_st_set_max_active_$_1( Args args )throws Exception {
        int active = Integer.parseInt( args.argv(0) ) ;
        if( active < 0 )
           throw new
           IllegalArgumentException("<maxActiveStores> must be >= 0") ;
        _storeQueue.setMaxActiveJobs( active ) ;
           
        return "Max Active Hsm Store Processes set to "+active ;
    }
    public String hh_st_jobs_ls = "" ;
    public String ac_st_jobs_ls( Args args )throws Exception {
       return _storeQueue.printJobQueue(null).toString() ;
    }
    public String hh_st_jobs_remove = "<jobId>" ;
    public String ac_st_jobs_remove_$_1( Args args )throws Exception {
       int id = Integer.parseInt( args.argv(0) ) ;
       _storeQueue.remove( id ) ;
       return "Removed" ;
    }
    public String hh_st_jobs_kill = "<jobId>" ;
    public String ac_st_jobs_kill_$_1( Args args )throws Exception {
       int id = Integer.parseInt( args.argv(0) ) ;
       _storeQueue.kill( id ) ;
       return "Kill initialized" ;
    }
    public String hh_st_ls = "[<pnfsId>]" ;
    public String ac_st_ls( Args args )throws Exception {
        _cell.say("trying to Get store pnfsid list");
        Iterator     e  = _storageHandler.getStorePnfsIds() ;
        _cell.say("Got store pnfsid list");
        StringBuffer sb = new StringBuffer() ;
        while( e.hasNext() ){
           PnfsId pnfsId = (PnfsId)e.next() ;
           _cell.say("ok "+pnfsId);
           HsmStorageHandler2.Info info = _storageHandler.getStoreInfoByPnfsId(pnfsId) ;
           _cell.say("done "+info);
           if( info == null ){
              sb.append(pnfsId).append("  <zombie>\n") ;
           }else{
              sb.append(pnfsId).append("  ").append(info.getListenerCount()).
                 append("   ").append( new Date(info.getStartTime()).toString() ).append("\n") ;
           }
           
        }
        return sb.toString();
    }
    //
    //   ??? needs to be adated
    //
    public String hh_rh_restore = "<pnfsId>" ;
    public String ac_rh_restore_$_1(Args args)throws Exception {
              String       pnfsId = args.argv(0) ;

        CellMessage  msgx    = _cell.getThisMessage() ;
        final CellPath path = (CellPath)msgx.getSourceAddress().clone() ;
        path.revert() ;
        CacheFileAvailable cfa    = new CacheFileAvailable(){
            public void cacheFileAvailable( String pnfsId , Throwable ee ){
               _cell.say( "Callback called for "+pnfsId) ;
               CellMessage msg = new CellMessage( path , "" ) ;
               if( ee == null ){
                  msg.setMessageObject("Done : "+pnfsId ) ;
               }else{
                  msg.setMessageObject("Problem with "+pnfsId+" : "+ee ) ;
               }
               try{
                  _cell.say("Sending message to "+path ) ;
                  _cell.sendMessage( msg ) ;
               }catch(Exception eee ){
                  _cell.say("Callback : "+eee ) ;
                  eee.printStackTrace() ;
               }
            }
        } ;
        boolean wasStored = _storageHandler.fetch( new PnfsId(pnfsId) , 
                               null,
                               cfa ) ;
        return wasStored ? "Already in cache" : "Stay tuned" ;
    }
}
