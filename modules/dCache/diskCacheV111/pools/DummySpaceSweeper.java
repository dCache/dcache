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
public class DummySpaceSweeper implements SpaceSweeper , Runnable  {
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
    public void actionPerformed( CacheEvent event ){/* forced by interface definition */}
    public void precious( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void available( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void created( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void destroyed( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void touched( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void removed( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void needSpace( CacheNeedSpaceEvent event ){/* forced by interface definition */}

    public void scanned( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void cached( CacheRepositoryEvent event ){/* forced by interface definition */}
    public void sticky( CacheRepositoryEvent event ){/* forced by interface definition */}

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

    public void printSetup( PrintWriter pw ){
       pw.println( "#\n# Nothing from the "+this.getClass().getName()+"#" ) ;
    }

    public void afterSetupExecuted() {}

    private void say( String msg ){
       _cell.say( "D-SWEEPER : "+msg ) ;
    }

}
