package dmg.util ;

public class FinThread extends Thread {
   public FinThread( Runnable r ){ super( r ) ; }
   @Override
   protected void finalize() throws Throwable {
     super.finalize() ;
     System.out.println( " FinThread finalizer called " ) ;
   }
}
