// $Id: HsmStorageInterpreter.java,v 1.9 2007-10-26 11:17:06 behrmann Exp $

package org.dcache.pool.classic;

import java.util.* ;

import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import diskCacheV111.util.* ;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.cells.CellCommandListener;

import org.apache.log4j.Logger;

public class HsmStorageInterpreter
    implements CellCommandListener
{
   private static final Logger _log =
       Logger.getLogger(HsmStorageInterpreter.class);

   private final HsmStorageHandler2 _storageHandler ;
   private final JobScheduler       _fetchQueue     ;
   private final JobScheduler       _storeQueue     ;
   private final PnfsHandler        _pnfs;

   public HsmStorageInterpreter( HsmStorageHandler2 handler,
                                 PnfsHandler pnfs){


      _storageHandler = handler ;
      _pnfs           = pnfs;
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
       _storageHandler.setTimeout(-1,timeout,-1) ;
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
    public String hh_rh_jobs_kill = "[-force] <jobId>";
    public String ac_rh_jobs_kill_$_1( Args args )throws Exception {
       int id = Integer.parseInt( args.argv(0) ) ;
       _fetchQueue.kill( id, args.getOpt("force") != null ) ;
       return "Kill initialized" ;
    }
    public String hh_rh_ls = "[<pnfsId>]" ;
    public String ac_rh_ls( Args args )throws Exception {
        StringBuffer sb = new StringBuffer() ;
        for (PnfsId pnfsId : _storageHandler.getRestorePnfsIds()) {
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
       _storageHandler.setTimeout(timeout,-1,-1) ;
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
    public String hh_st_jobs_kill = "[-force] <jobId>";
    public String ac_st_jobs_kill_$_1( Args args )throws Exception {
       int id = Integer.parseInt( args.argv(0) ) ;
       _storeQueue.kill( id, args.getOpt("force") != null ) ;
       return "Kill initialized" ;
    }
    public String hh_st_ls = "[<pnfsId>]" ;
    public String ac_st_ls( Args args )throws Exception {
        StringBuffer sb = new StringBuffer() ;
        for (PnfsId pnfsId : _storageHandler.getStorePnfsIds()) {
           _log.debug("ok "+pnfsId);
           HsmStorageHandler2.Info info = _storageHandler.getStoreInfoByPnfsId(pnfsId) ;
           _log.debug("done "+info);
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
    //   remove handler
    //
    public String hh_rm_set_timeout = "<timeout/seconds>" ;
    public String ac_rm_set_timeout_$_1(Args args)
    {
       long timeout = Long.parseLong(args.argv(0))*1000L;
       _storageHandler.setTimeout(-1,-1,timeout);
       return "" ;
    }

    public String hh_rm_set_max_active = "<maxActiveRemovers>";
    public String ac_rm_set_max_active_$_1(Args args) throws Exception
    {
        int active = Integer.parseInt(args.argv(0));
        if (active < 0)
            throw new
                IllegalArgumentException("<maxActiveRemovers> must be >= 0");
        _storageHandler.setMaxRemoveJobs(active);

        return "Max active remover processes set to " + active;
    }

    //
    //   ??? needs to be adated
    //
    public String hh_rh_restore = "[-block] <pnfsId>";
    public Object ac_rh_restore_$_1(Args args)
    {
        final String pnfsId = args.argv(0);
        final boolean block = args.getOpt("block") != null;
        final DelayedReply reply = new DelayedReply();

        final CacheFileAvailable cfa = new CacheFileAvailable() {
                public void cacheFileAvailable(String pnfsId, Throwable ee) {
                    try {
                        if (ee == null) {
                            reply.send("Fetched " + pnfsId);
                        } else {
                            reply.send("Failed to fetch " + pnfsId + ": " + ee);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (NoRouteToCellException e) {
                        _log.error("Failed to deliver reply: " + e);
                    }
                }
            };

        /* We need to fetch the storage info and we don't want to
         * block the message thread while waiting for the reply.
         */
        Thread t = new Thread("rh restore") {
                public void run() {
                    try {
                        StorageInfo si = _pnfs.getStorageInfo(pnfsId);
                        _storageHandler.fetch(new PnfsId(pnfsId), si,
                                              block ? cfa : null);
                    } catch (CacheException e) {
                        cfa.cacheFileAvailable(pnfsId, e);
                    }
                }
            };
        t.start();

        return block ? reply : "Fetch request queued";
    }
}
