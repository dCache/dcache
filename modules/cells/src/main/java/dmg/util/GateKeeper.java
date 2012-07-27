package dmg.util ;

import java.util.* ;

public class GateKeeper {
    private Vector<ThreadWatch>      _stack        = new Vector<ThreadWatch>() ;
    private ThreadWatch _activeThread;
    private int         _defaultPrio  = LOW ;
    public  final static  int  MASTER  =  0 ;
    public  final static  int  HIGH    =  2 ;
    public  final static  int  MEDIUM  =  4 ;
    public  final static  int  LOW     =  6 ;

    private class ThreadWatch {
        private Thread _thread ;
        private int    _priority ;
        private int    _usage;
        private ThreadWatch( Thread thread , int priority ){
           _priority = priority ;
           _thread   = thread ;
           _usage    = 1 ;
        }
        public int getPriority(){ return _priority ; }

        public Thread getThread(){ return _thread ; }

        public void increment(){ _usage ++ ; }
        public int  decrement(){ _usage -- ; return _usage ; }
    }

    public GateKeeper(){ }
    public GateKeeper( int defaultPriority ){
        _defaultPrio = defaultPriority ;
    }
    public synchronized void open( int priority )
           throws InterruptedException  {
        try{ this.open( priority , 0 ) ; }catch( ExpiredException ee ){}
    }
    public synchronized void open( int priority , long waitMillis )
           throws InterruptedException,
                  ExpiredException      {


        if( ( _activeThread != null ) &&
            ( _activeThread.getThread() == Thread.currentThread() ) ){

            _activeThread.increment() ;
            return ;
        }
        ThreadWatch newTw =
             new ThreadWatch( Thread.currentThread(), priority ) ;

        //
        // if no threads are waiting, things are pretty easy.
        //
        if( _activeThread == null ){
           _activeThread = newTw ;
           return ;
        }
        register( newTw , priority ) ;
        //
        // and now wait
        //
        try{
           if( waitMillis > 0 ){
              long stopIt = waitMillis + System.currentTimeMillis() ;
              long rest ;
              while( true ){

                  rest = stopIt - System.currentTimeMillis() ;
                  if( rest <= 0L ){
                     unregister() ;
                     throw new ExpiredException("Open timeout expired" ) ;
                  }
                  wait( rest ) ;
                  if( ( _activeThread == null        ) &&
                      ( newTw == _stack.elementAt(0) )    ) {
                      break;
                  }
              }
           }else{
              while(true){
                  wait() ;
                  if( ( _activeThread == null        ) &&
                      ( newTw == _stack.elementAt(0) )    ) {
                      break;
                  }
              }
           }
        }catch( InterruptedException ie ){
           unregister() ;
           throw ie ;
        }
        _activeThread = _stack.elementAt(0) ;
        _stack.removeElementAt(0) ;
        return ;
    }
    private void register( ThreadWatch newTw , int priority ){
        //
        // find the place where to insert the current thread.
        //
        int c = _stack.size() ;
        int i ;
        ThreadWatch tw  = null ;
        for( i = 0 ; i < c ; i++ ){
            tw = _stack.elementAt(i) ;
            if( _stack.elementAt(i).getPriority() > priority ){

               _stack.insertElementAt( newTw , i ) ;
               break ;

            }
        }
        if( i == c ) {
            _stack.addElement(newTw);
        }
        return ;
    }
    private void unregister(){
       int         c   = _stack.size() ;
       Thread      ich = Thread.currentThread() ;
       ThreadWatch tw;
       for( int i = 0  ; i < c ; i ++ ){
          tw = _stack.elementAt(i) ;
          if( tw.getThread() ==  ich ){
              _stack.removeElementAt(i) ;
              return ;
          }
       }
       return ;
    }
    public synchronized void close(){
       if( ( _activeThread == null ) ||
           ( Thread.currentThread() != _activeThread.getThread() ) ) {
           throw new IllegalArgumentException("Not owner");
       }

       if( _activeThread.decrement() > 0 ) {
           return;
       }

       _activeThread = null ;
       notifyAll() ;
    }

}
