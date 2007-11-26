// $Id: DummySpaceSweeper.java,v 1.4 2003-08-03 14:53:41 cvs Exp $

package diskCacheV111.pools ;

import diskCacheV111.repository.* ;
import diskCacheV111.util.* ;
import diskCacheV111.util.event.* ;
import diskCacheV111.vehicles.StorageInfo ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import java.util.* ;
import java.text.SimpleDateFormat ;
import java.io.PrintWriter ;

public class DummySpaceSweeper implements SpaceSweeper , Runnable  {
    private CacheRepository _repository = null ;
    private CellAdapter     _cell       = null ;
    private PnfsHandler     _pnfs       = null ;
    
    private static SimpleDateFormat __format = 
               new SimpleDateFormat( "HH:mm-MM/dd" ) ;
    
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
    public void actionPerformed( CacheEvent event ){}
    public void precious( CacheRepositoryEvent event ){}
    public void available( CacheRepositoryEvent event ){}
    public void created( CacheRepositoryEvent event ){}
    public void destroyed( CacheRepositoryEvent event ){}
    public synchronized void touched( CacheRepositoryEvent event ){}
    public synchronized void removed( CacheRepositoryEvent event ){}
    public synchronized void needSpace( CacheNeedSpaceEvent event ){}      

    public synchronized void scanned( CacheRepositoryEvent event ){}
    public synchronized void cached( CacheRepositoryEvent event ){}
    public synchronized void sticky( CacheRepositoryEvent event ){}
    public String hh_sweeper_free = "<bytesToFree>" ;
    public String ac_sweeper_free_$_1( Args args )throws Exception {
       return "There are no reason to ask dummes for information..." ;
    }
    public String hh_sweeper_ls = " [-l] [-s]" ;
    public String ac_sweeper_ls( Args args )throws Exception {
        return "There are no reason to ask dummes for information..." ;
    }
    public void run(){
       say( "started");
       // This is a Dummy Sweeper.
       while( ! Thread.currentThread().interrupted() ){
	   
           try{
               Thread.currentThread().sleep(10000*60) ;
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
    private void say( String msg ){ 
       _cell.say( "D-SWEEPER : "+msg ) ;
    }
    private void esay( String msg ){
       _cell.esay( "D-SWEEPER ERROR : "+msg ) ;
    }


}
