package dmg.util ;
import  java.util.* ;

public class       StateThread
       implements  Runnable     {

   private StateEngine _engine = null ;
   private Thread _timerThread  = null ;
   private Thread _workerThread = null ;
   private Thread _tickerThread = null ;
   private Object _timerLock    = new Object() ;
   private int    _timerTime    = 0 ;
   private int    _timerState   = 0 ;
   private int    _timeoutState = 0 ;
   private long   _timeStamp    = 0L ;

   public StateThread( StateEngine engine ){
      _engine       = engine ;
      _timeStamp    = new Date().getTime() ;
      _tickerThread = new Thread( this ) ;
      _workerThread = new Thread( this ) ;

//            _tickerThread.start() ;

//      _workerThread.start() ;
   }
   public void stop(){
      synchronized( _timerLock ){
         if( _tickerThread != null ) {
             _tickerThread.interrupt();
         }
         if( _workerThread != null ) {
             _workerThread.interrupt();
         }
         if( _timerThread != null ) {
             _timerThread.interrupt();
         }
      }
   }
   public void start(){ _workerThread.start() ; }
   private void _resetTimer(){
      synchronized( _timerLock ){
         if( _timerThread != null ){
           _timerThread.interrupt() ;
           _timerThread = null ;
         }
      }
   }
   private void _setTimer( int sec ){
      synchronized( _timerLock ){
         _resetTimer() ;
         _timerTime   = sec ;
//         _timerThread = new Thread( this ) ;
//         _timerThread.start() ;

      }
   }
   public int setFinalState(){ return setState(-1) ; }
   public int setState( int state ){
      synchronized( _timerLock ){
         if( Thread.currentThread() != _workerThread ){
            if( _workerThread != null ) {
                _workerThread.interrupt();
            }
            _workerThread  = new Thread( this ) ;
            _workerThread.start() ;
          }
         _resetTimer() ;
         _timerState   = state ;
         _timeoutState = 0 ;
         long now = new Date().getTime() ;
         int diff = (int)( now - _timeStamp ) ;
         _timeStamp = now ;
         return diff ;
      }
   }
   public int setState( int state , int to , int toState ){
      synchronized( _timerLock ){
         if( Thread.currentThread() != _workerThread ){
            if( _workerThread != null ) {
                _workerThread.interrupt();
            }
            _workerThread  = new Thread( this ) ;
            _workerThread.start() ;
          }
         _resetTimer() ;
         _timerState   = state ;
         _timeoutState = toState ;
         _setTimer( to ) ;
         long now = new Date().getTime() ;
         int diff = (int)( now - _timeStamp ) ;
         _timeStamp = now ;
         return diff ;
      }
   }
   public int getState(){
      int value ;
      synchronized( _timerLock ){
         value = _timerState ;
      }
      return value ;
   }

   public void run(){
     if( Thread.currentThread() == _workerThread ){
       int state = getState() ;
       while( ! Thread.interrupted() ){
         state = _engine.runState( state ) ;
         if( state != 0 ) {
             setState(state);
         } else {
             state = getState();
         }
         if( state < 0 ) {
             return;
         }
       }
     }else if( Thread.currentThread() == _tickerThread ){
        while( true ){
           int state = getState() ;
           System.out.println( " Ticker : state = "+state ) ;
             try {
                Thread.sleep( 1000 ) ;
            } catch (InterruptedException e) {
                // take it easy
            }
           if( state < 0 ) {
               return;
           }

        }
     }else if( Thread.currentThread() == _timerThread ){
        synchronized( _timerLock ){
           try{
              _timerLock.wait( _timerTime * 1000 ) ;
           }catch(InterruptedException ie ){}
           if( _workerThread != null ) {
               _workerThread.interrupt();
           }
           _timerState = _timeoutState ;
           _workerThread  = new Thread( this ) ;
           _workerThread.start() ;
        }
     }


   }

}
