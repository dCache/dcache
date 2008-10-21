// $Id: DummySpaceSweeper.java,v 1.4 2003-08-03 14:53:41 cvs Exp $

package org.dcache.pool.classic;

import java.io.PrintWriter;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.event.CacheEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
import diskCacheV111.util.event.CacheRepositoryEvent;
import dmg.util.Args;

/**
 *
 * Dummy pool space sweeper.
 * Can be enabled on pools to protect then from cleaning CACHED files
 *
 */
public class DummySpaceSweeper
    extends AbstractSpaceSweeper
    implements Runnable
{
    private final CacheRepository _repository ;
    private final PnfsHandler     _pnfs ;


    public DummySpaceSweeper(PnfsHandler pnfs ,
                             CacheRepository repository)
    {
       _repository = repository ;
       _pnfs       = pnfs ;
       new Thread(this, "d-sweeper").start();
    }
    public long getRemovableSpace(){ return 0L ; }

    public String hh_sweeper_free = "<bytesToFree>" ;
    public String ac_sweeper_free_$_1( Args args )throws Exception {
       return "There are no reason to ask dummies for information..." ;
    }
    public String hh_sweeper_ls = " [-l] [-s]" ;
    public String ac_sweeper_ls( Args args )throws Exception {
        return "There are no reason to ask dummies for information..." ;
    }
    public void run(){
       // This is a Dummy Sweeper.
       while( ! Thread.interrupted() ){

           try{
               Thread.sleep(10000*60) ;
           }catch(InterruptedException e){
               break ;
           }
       }
    }
    public long getLRUSeconds(){ return  3600L ; }
}
