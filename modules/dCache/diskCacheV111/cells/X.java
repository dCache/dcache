package diskCacheV111.cells ;

import java.io.* ;
    class ReaderThread extends Thread {
         private BufferedReader _br ;
         public ReaderThread( BufferedReader b ){
             _br = b ;
             start() ;
             
         }
         public void run(){
            String line = null ;
            System.out.println( "ReaderStarted" ) ;
            try{
               while( ( line = _br.readLine() ) != null ){
                   System.out.println( "Stderr : "+line ) ;
               }
            }catch( IOException ioe ){
               System.err.println( "Exception in read : "+ioe ) ;
            }
            try{ _br.close() ; }catch(Exception e ){}
            System.out.println( "ReaderStopped" ) ;
         }
    
    }
public class X {

    public static void main( String [] args ) throws Exception {
        long x = System.currentTimeMillis() ;
        for( int i = 0 ; i < 100000 ; i ++ ){
           long tmp = System.currentTimeMillis() ;
        }
        System.out.println("Result : "+(System.currentTimeMillis() - x ) ) ;
        System.exit(0);
        Runtime rt = Runtime.getRuntime() ;
        System.out.println( "Starting ... " ) ;
        OutputStream os = new FileOutputStream( "otto") ;
        Process p = rt.exec( "/tmp/wontdie 1>/dev/null 2>/dev/null &" ) ;
        System.out.println( "Started" ) ;
        InputStream in = p.getErrorStream() ;
        BufferedReader br =
              new BufferedReader(
                   new InputStreamReader( p.getErrorStream() ) ) ;
                   
        new ReaderThread( br ) ;
        Thread.currentThread().sleep(5000) ;
        p.destroy() ;
        int rc = p.waitFor() ;
        System.out.println( "Rc : "+rc ) ;
    }

}
