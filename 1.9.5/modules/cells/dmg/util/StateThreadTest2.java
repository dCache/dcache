package dmg.util ;
import java.net.* ;
import dmg.util.* ;

public class StateThreadTest2 implements  StateEngine {

  private StateThread _engine ;
  
  
  public StateThreadTest2(){
      _engine   = new StateThread( this ) ;
      _engine.start() ;  
  }


   public int runState( int state ){
//      System.out.println( "Changing to state "+_sn[state]+
//             " Thread "+Thread.currentThread() ) ;
      switch( state ){
        case 0 :   //     initial state ;
          _engine.setState( 1 ) ;
        break ;
        case 1 :
          _engine.setState( 1 , 10 , 1 ) ;
          try{ Thread.sleep(10) ;
          }catch( Exception e ){ }
        break ;
      }   
      return 0 ;

   }
  public static void main( String [] args ){
     
     
     new StateThreadTest2() ;
  
  }
}
 
