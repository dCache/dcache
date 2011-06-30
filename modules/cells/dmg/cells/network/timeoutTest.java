package dmg.cells.network ;
import java.net.* ;

public class timeoutTest implements Runnable {

  private String _hostname ;
  private int    _port ;
  private Socket _socket ;
  private Exception _exception ;
  //
  // the automatic timer stuff
  //
  private Thread _timerThread  = null ;
  private Thread _workerThread = null ;
  private Thread _tickerThread = null ;
  private Object _timerLock    = new Object() ;
  private int    _timerTime    = 0 ;
  private int    _timerState   = 0 ;
  private int    _timeoutState = 0 ;

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
        _timerThread = new Thread( this ) ;
        _timerThread.start() ;

     }
  }
  private void setState( int state ){
     synchronized( _timerLock ){
        _resetTimer() ;
        _timerState   = state ;
        _timeoutState = 0 ;
     }
  }
  private void setState( int state , int to , int toState ){
     synchronized( _timerLock ){
        _resetTimer() ;
        _timerState   = state ;
        _timeoutState = toState ;
        _setTimer( to ) ;
     }
  }
  private int _getState(){
     int value ;
     synchronized( _timerLock ){
        value = _timerState ;
     }
     return value ;
  }
  public timeoutTest( String host , int port ){
      _hostname = host ;
      _port     = port ;

      _workerThread = new Thread( this ) ;
      _workerThread.start() ;

      _tickerThread = new Thread( this ) ;
      _tickerThread.start() ;

  }
  private static String [] _sn = {
    "<init>" ,
    "<go_connecting>" ,
    "<connecting>" ,
    "<connection_ready>" ,
    "<connection_failed>" ,
    "<connection_timeout>" ,
    "<finished>"   } ;
  private static final int TS_GO_CONNECTING      = 1 ;
  private static final int TS_CONNECTING         = 2 ;
  private static final int TS_CONNECTION_READY   = 3 ;
  private static final int TS_CONNECTION_FAILED  = 4 ;
  private static final int TS_CONNECTION_TIMEOUT = 5 ;
  private static final int TS_FINISHED           = 6 ;

  public void run(){
     if( Thread.currentThread() == _workerThread ){
       while( ! Thread.interrupted() ){
         int state = _getState() ;
         System.out.println( "Changing to state "+_sn[state]+
             " Thread "+Thread.currentThread() ) ;

         switch( state ){
           case 0 :   //     initial state ;
             setState( TS_GO_CONNECTING ) ;
           break ;
           case TS_GO_CONNECTING :
             System.out.println( " Connecting to "+_hostname+" "+_port ) ;
             setState( TS_CONNECTING , 10 , TS_CONNECTION_TIMEOUT ) ;
             try{
               _socket = new Socket( _hostname , _port ) ;
               setState( TS_CONNECTION_READY ) ;
             }catch( Exception e ){
               setState( TS_CONNECTION_FAILED ) ;
               _exception = e ;
             }
           break ;
           case TS_CONNECTION_READY :
             System.out.println( "Connection ready" ) ;
             setState( TS_FINISHED ) ;
           break ;
           case TS_CONNECTION_FAILED :
             System.out.println( "Connection Failed : "+_exception ) ;
             setState( TS_FINISHED ) ;
           break ;
           case TS_CONNECTION_TIMEOUT :
             System.out.println( "Connection timed out" ) ;
             setState( TS_FINISHED ) ;
           break;
           case TS_FINISHED :
             System.out.println( "Connection finished" ) ;
           return ;
         }


       }
     }else if( Thread.currentThread() == _tickerThread ){
        while( true ){
           int state = _getState() ;
           System.out.println( " Ticker : state = "+_sn[state] ) ;
           try{
             Thread.sleep( 1000 ) ;
           }catch( InterruptedException eee){}
           if( state == TS_FINISHED )return ;

        }
     }else if( Thread.currentThread() == _timerThread ){
        synchronized( _timerLock ){
           try{
              _timerLock.wait( _timerTime * 1000 ) ;
           }catch(Exception ie ){}
           if( _workerThread != null )_workerThread.interrupt() ;
           _timerState = _timeoutState ;
           _workerThread  = new Thread( this ) ;
           _workerThread.start() ;
        }
     }

  }

  public static void main( String [] args ){
     if( args.length < 2 ){
       System.err.println( " USAGE : <hostname> <port>" ) ;
       System.exit(3);
     }

     int port = new Integer( args[1] ).intValue() ;

     new timeoutTest( args[0] , port ) ;

  }



}
