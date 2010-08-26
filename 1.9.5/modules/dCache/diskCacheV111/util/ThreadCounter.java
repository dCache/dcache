// $Id: ThreadCounter.java,v 1.2 2004-06-04 14:36:18 patrick Exp $

package diskCacheV111.util ;

import java.util.* ;
import dmg.cells.nucleus.CellAdapter ;
import dmg.cells.nucleus.CDC ;
import dmg.util.Args ;

import org.dcache.cells.CellCommandListener;
import org.apache.log4j.Logger;

public class ThreadCounter
    implements ThreadPool, Runnable, CellCommandListener
{

    private final static Logger _log = Logger.getLogger(ThreadCounter.class);

    private CellAdapter _cell          = null ;
    private int    _currentThreadCount = 0 ;
    private int    _maxThreadCount     = 0 ;
    private LinkedList _fifo           = new LinkedList() ;

    private class Runner
    {
        public final Runnable runner;
        public final String name;
        public CDC cdc;

        private Runner(Runnable runner, String name)
        {
            this.runner = runner;
            this.name = name;
            this.cdc = new CDC();
        }
    }

    public ThreadCounter( CellAdapter cell ){
       _cell = cell ;
       _cell.getNucleus().newThread(this,"ThreadCounter").start() ;
    }

    public ThreadCounter()
    {
        new Thread(this,"ThreadCounter").start();
    }

    public String toString(){
       int currentThreadCount = 0 ;
       int waitingThreads     = 0 ;
       synchronized( _fifo ){
          currentThreadCount = _currentThreadCount ;
          waitingThreads     = _fifo.size() ;
       }
       return "Active threads : "+currentThreadCount+"/"+_maxThreadCount+"/"+waitingThreads ;
    }
    public int getWaitingThreadCount(){
       synchronized( _fifo ){
          return _fifo.size() ;
       }
    }
    public int getCurrentThreadCount(){
       synchronized( _fifo ){
          return _currentThreadCount ;
       }
    }
    public int getMaxThreadCount(){
      return _maxThreadCount ;
    }
    public void setMaxThreadCount( int maxThreadCount ){
       if( maxThreadCount < 0 )
          throw new
          IllegalArgumentException("Max thread count must be >= 0" ) ;

       synchronized( _fifo ){
          _maxThreadCount = maxThreadCount ;
          _fifo.notifyAll();
       }
    }
    public void invokeLater( Runnable runner , String runnerName ){
       synchronized( _fifo ){
          _fifo.addFirst( new Runner( runner , runnerName ) ) ;
          _fifo.notifyAll();
//          _log.info("ThreadCounter : thread added "+_currentThreadCount);
       }
    }
    private class ClientRunner implements Runnable {
       private Runnable _runner = null ;
       private CDC _cdc;
       private ClientRunner( Runnable runner, CDC cdc ){
          _runner = runner ;
          _cdc = cdc ;
       }
       public void run(){
          _cdc.apply();
          try{
             _runner.run();
          }catch(Throwable t ){
             _log.warn(t);
          }finally{
              CDC.clear();
             synchronized( _fifo ){
                _currentThreadCount-- ;
//                _log.info("ThreadCounter : thread finished "+_currentThreadCount);
                _fifo.notifyAll();
             }
          }
       }
    }
    public void run(){

       _log.info("Thread Counter thread started");
       try{
          while( ! Thread.currentThread().isInterrupted() ){


             synchronized( _fifo ){
                while( ( _fifo.size() == 0 ) ||
                       ( ( _maxThreadCount > 0 ) &&
                         ( _currentThreadCount >= _maxThreadCount )
                       )
                                                                   ){

                   _fifo.wait() ;

                }

                final Runner r = (Runner)_fifo.removeLast();
                try{
                    if (_cell != null) {
                        _cell.getNucleus().newThread( new ClientRunner(r.runner, r.cdc) , r.name ).start() ;
                    } else {
                        new Thread(new ClientRunner(r.runner, r.cdc), r.name).start();
                    }
//                   _log.info("ThreadCounter : thread started "+_currentThreadCount);
                   _currentThreadCount ++ ;
                }catch(Throwable tt ){
                    _log.warn(tt);
                    try{
                       if( r.runner instanceof ExtendedRunnable ){
                           ((ExtendedRunnable)r.runner).runFailed() ;
                       }
                    }catch(Throwable ttt){
                       _log.warn(ttt);
                    }
                }

             }
          }
       }catch(InterruptedException ee ){
          _log.info("Thread Counter thread was interrupted");
       }
       _log.info("Thread Counter thread finished");
    }
    //
    // DEBUG ONLY
    //
    public String ac_tc_debug_ls( Args args ){
       StringBuffer sb = new StringBuffer() ;
       synchronized( _fifo ){
          ArrayList al = new ArrayList(_fifo);
          for( Iterator i = al.iterator() ; i.hasNext() ; ){
             sb.append( ((Runner)i.next()).runner.toString() ).append("\n");
          }
       }
       return sb.toString();
    }
}
