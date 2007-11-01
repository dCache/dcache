package dmg.apps.ampexCopy ;
import java.io.* ;

public class cut {

   public static void main( String [] args ){
      if( args.length < 3 ){
        System.err.println( "Usage : ... <inFile> <outFile> <size>" ) ;
        System.exit(34);
      }
      long size = 0 ;
      File in = new File( args[0] ) ;
      if( ! in.exists() ){
         System.err.println( "Input file not found : "+args[0] ) ;
         System.exit(4) ;
      }
      File out = new File( args[1] ) ;
//      if( out.exists() ){
//         System.err.println( "Output file exits : "+args[1] ) ;
//         System.exit(4) ;
//      }
      try{
         size = Long.parseLong( args[2] ) ;
      }catch( Exception e ){
         System.err.println("Not numeric : "+args[2] ) ;
         System.exit(4);
      }
      byte [] buffer = new byte[64 * 1024 ] ;
      InputStream  ins  = null ;
      OutputStream outs = null ;
      int rc = 0 ;
      try{
         ins  = new FileInputStream(in) ;
         outs = new FileOutputStream(out) ;
         
         while(true){
            int next = (int)((long)buffer.length > size ? size : buffer.length ) ; 
            if( next == 0 )break ;
            int l = ins.read(buffer,0,next) ;
            if( l <= 0 ){
              System.err.println("Too early EOF" ) ;
              rc = 6 ;
              break ;
            }
            size -= l ;
            outs.write(buffer,0,l) ;         
         }
      
      }catch(Exception ee ){
         ee.printStackTrace() ;
         System.exit(4);
      }finally{
        try{ ins.close() ; }catch(Exception eee){}
        try{ outs.close() ; }catch(Exception eee){}
      }
      System.exit(rc) ;
   }
}
