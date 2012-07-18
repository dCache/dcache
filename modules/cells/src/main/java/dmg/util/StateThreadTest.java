package dmg.util ;
import java.net.* ;
import dmg.util.* ;

public class StateThreadTest implements  StateEngine {

  private String      _hostname ;
  private int         _port ;
  private Socket      _socket ;
  private Exception   _exception ;
  private StateThread _engine ;
  
  
  public StateThreadTest( String host , int port ){
      _hostname = host ;
      _port     = port ;
      
      _engine   = new StateThread( this ) ;
      _engine.start() ;  
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

   @Override
   public int runState( int state ){
      System.out.println( "Changing to state "+_sn[state]+
             " Thread "+Thread.currentThread() ) ;
      switch( state ){
        case 0 :   //     initial state ;
          _engine.setState( TS_GO_CONNECTING ) ;
        break ;
        case TS_GO_CONNECTING :
          System.out.println( "Connecting to "+_hostname+" "+_port ) ;
          _engine.setState( TS_CONNECTING , 10 , TS_CONNECTION_TIMEOUT ) ;
          try{
            _socket = new Socket( _hostname , _port ) ;
            _engine.setState( TS_CONNECTION_READY ) ;
          }catch( Exception e ){
            _engine.setState( TS_CONNECTION_FAILED ) ;
            _exception = e ;
          }
        break ;
        case TS_CONNECTION_READY :
          System.out.println( "Connection ready" ) ;
          _engine.setState( TS_FINISHED ) ;
        break ;
        case TS_CONNECTION_FAILED :
          System.out.println( "Connection Failed : "+_exception ) ;
          _engine.setState( TS_FINISHED ) ;
        break ;
        case TS_CONNECTION_TIMEOUT :
          System.out.println( "Connection timed out" ) ;
          _engine.setState( TS_FINISHED ) ;
        break;
        case TS_FINISHED :
          System.out.println( "Connection finished" ) ;
          _engine.setFinalState() ;
        break ;
      }   
      return 0 ;

   }
  public static void main( String [] args ){
     if( args.length < 2 ){
       System.err.println( " USAGE : <hostname> <port>" ) ;
       System.exit(3);
     }
     
     int port = new Integer(args[1]);
     
     new StateThreadTest( args[0] , port ) ;
  
  }
}
