package dmg.util ;

public class GateTest implements Runnable {
   Thread _ticker1, _ticker2 ;
   Gate   _gate = new Gate(false) ;

   public GateTest(){

      _ticker1 = new Thread( this ) ;
      _ticker2 = new Thread( this ) ;

      _ticker1.start() ;
      _ticker2.start() ;


   }
   public void run(){
     if( Thread.currentThread() == _ticker1 ){
       while(true){
         try{ Thread.sleep(1000) ; }
         catch( InterruptedException e ){} ;
         System.out.println( " + Waiting for gate to enter" ) ;
         synchronized( _gate.check() ){
            System.out.println( " + Gate entered and sleeping " ) ;
            try{ Thread.sleep(1000) ; }
            catch( InterruptedException e ){} ;
            System.out.println( " + Leaving gate again " ) ;
         }
       }
     }else if( Thread.currentThread() == _ticker2 ){
       while( true ){
         try{ Thread.sleep(4000) ; }
         catch( Exception e ){} ;
         System.out.println( " - Opening Gate " ) ;
         _gate.open() ;
         try{ Thread.sleep(4000) ; }
         catch( Exception e ){} ;
         System.out.println( " - Closing Gate " ) ;
         _gate.close() ;
       }

     }

   }
   public static void main( String[] args ){
      new GateTest() ;
   }

}
