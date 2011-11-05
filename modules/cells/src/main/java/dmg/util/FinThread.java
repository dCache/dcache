package dmg.util ;

public class FinThread extends Thread {
   public FinThread( Runnable r ){ super( r ) ; }
   protected void finalize() throws Throwable {
     super.finalize() ;
     System.out.println( " FinThread finalizer called " ) ;
   }
}
