// $Id: DummySpaceSweeper.java,v 1.4 2003-08-03 14:53:41 cvs Exp $

package diskCacheV111.pools ;

import java.io.PrintWriter;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.event.CacheEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
import diskCacheV111.util.event.CacheRepositoryEvent;
import dmg.cells.nucleus.CellAdapter;
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
    private final CellAdapter     _cell;
    private final PnfsHandler     _pnfs ;


    public DummySpaceSweeper( CellAdapter cell ,
                          PnfsHandler pnfs ,
                          CacheRepository repository ,
                          HsmStorageHandler2 storage     ){

       _repository = repository ;
       _cell       = cell ;
       _pnfs       = pnfs ;

       _cell.getNucleus().newThread( this , "d-sweeper" ).start() ;
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
       say( "started");
       // This is a Dummy Sweeper.
       while( ! Thread.interrupted() ){

           try{
               Thread.sleep(10000*60) ;
           }catch(InterruptedException e){
               break ;
           }
       }
       say("D-SS : finished");
    }
    public long getLRUSeconds(){ return  3600L ; }

    private void say( String msg ){
       _cell.say( "D-SWEEPER : "+msg ) ;
    }

}
