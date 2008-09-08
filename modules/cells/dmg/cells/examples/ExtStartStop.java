package dmg.cells.examples ;

import java.io.* ;

public class ExtStartStop extends StartStop {
    public ExtStartStop( String name ){
       super( name ) ;
       try{
          System.out.println( "Going to wait" ) ;
          Thread.sleep(10000) ;
          System.out.println( "Wakeing up " ) ;
       }catch(Exception e){}
    }
    public static void main( String [] args ){
       new ExtStartStop( "hallo" ) ;
    }
}
class StartStop implements Runnable  {
   public StartStop( String name ) {
      new Thread(this).start() ;
   }
   public void run(){
      while( true ){
         System.out.println( "My Class : "+this.getClass() ) ;
         try{
         
            Thread.sleep(1000) ;
             synchronized( this ){
                notifyAll() ;
             }
         }
         catch(Exception e ){
            System.out.println( "Exception : "+e ) ;
         }
      }
   
   }
   
}
