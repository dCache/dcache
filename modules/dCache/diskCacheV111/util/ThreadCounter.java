// $Id: ThreadCounter.java,v 1.2 2004-06-04 14:36:18 patrick Exp $

package diskCacheV111.util ;

import java.util.* ;
import dmg.cells.nucleus.CellAdapter ;
import dmg.util.Args ;

public class ThreadCounter implements ThreadPool, Runnable {

    private CellAdapter _cell          = null ;
    private int    _currentThreadCount = 0 ;
    private int    _maxThreadCount     = 0 ;
    private LinkedList _fifo           = new LinkedList() ;

    private class Runner {
       private Runnable runner = null ;
       private String   name   = null ;
       private Runner( Runnable runner , String runnerName ){
         this.runner = runner ;
         this.name   = runnerName ;

       }
    }
    public ThreadCounter( CellAdapter cell ){
       _cell = cell ;
       _cell.getNucleus().newThread(this,"ThreadCounter").start() ;
    }
    private void say(String message){ _cell.say(message) ; }
    private void esay(String message){ _cell.esay(message) ; }
    private void esay(Throwable t ){ _cell.esay(t) ; }
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
//          say("ThreadCounter : thread added "+_currentThreadCount);
       }
    }
    private class ClientRunner implements Runnable {
       private Runnable _runner = null ;
       private ClientRunner( Runnable runner ){
          _runner = runner ;
       }
       public void run(){
          try{
             _runner.run();
          }catch(Throwable t ){
             esay(t);
          }finally{
             synchronized( _fifo ){
                _currentThreadCount-- ;
//                say("ThreadCounter : thread finished "+_currentThreadCount);
                _fifo.notifyAll();
             }
          }
       }
    }
    public void run(){

       say("Thread Counter thread started");
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
                   _cell.getNucleus().newThread( new ClientRunner(r.runner) , r.name ).start() ;
//                   say("ThreadCounter : thread started "+_currentThreadCount);
                   _currentThreadCount ++ ;
                }catch(Throwable tt ){
                    esay(tt);
                    try{
                       if( r.runner instanceof ExtendedRunnable ){
                           ((ExtendedRunnable)r.runner).runFailed() ;
                       }
                    }catch(Throwable ttt){
                       esay(ttt);
                    }
                }

             }
          }
       }catch(InterruptedException ee ){
          say("Thread Counter thread was interrupted");
       }
       say("Thread Counter thread finished");
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
