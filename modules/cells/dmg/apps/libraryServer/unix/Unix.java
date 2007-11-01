package dmg.apps.libraryServer.unix ;

import java.io.* ;

public class Unix {
   static {
      System.loadLibrary("Unix");
   }

   public Unix(){
   
   }
   public static native int getPid() ;
   public static native int getParentId() ;
   public static native int kill( int pid , int sig ) ;
   
   public static native int open( String filename , int mode ) throws IOException ;


   public static void main( String [] args )throws Exception {
      int pid = 0 ;
      System.out.println( pid = Unix.getPid() ) ;
      System.out.println( Unix.getParentId() ) ;
      System.out.println( Unix.kill( pid , 0 ) ) ;
      System.out.println( Unix.kill( 3434 , 0 ) ) ;
      
   }
}
