package diskCacheV111.clients.vsp ;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.dcache.util.Args;

public class vsp {

    public static void main( String [] argss ){

         Args args = new Args(argss) ;
         if( args.argc() < 2 ){
             System.err.println("Usage : ... [options] <filename> <pnfsid>" ) ;
             System.err.println("        ... [options] <pnfsId> <filename>" ) ;
             System.err.println("    options : " ) ;
             System.err.println("       -host=<hostname> -port=<portnumber>" ) ;
             System.err.println("       -bs=<read/write blocksize>" ) ;
             System.err.println("       -reply=<replyHostName>" ) ;
             System.err.println("       -debug") ;
             System.exit(4);
         }
         String a1 = args.argv(0) ;
         String a2 = args.argv(1) ;
         boolean a1IsPnfsId = a1.charAt(0) ==  '0' ;
         boolean a2IsPnfsId = a2.charAt(0) ==  '0' ;
         boolean runDebug   = args.hasOption("debug") ;
         if( a1IsPnfsId && a2IsPnfsId ){
            System.err.println( "Can't copy pnfsId into pnfsId" ) ;
            System.exit(4);
         }else if( ( ! a1IsPnfsId ) && ( ! a2IsPnfsId ) ){
            System.err.println( "Please use cp to copy regular files" ) ;
            System.exit(4);
         }
         String pnfsId   = a1IsPnfsId ? args.argv(0) : args.argv(1) ;
         String filename = a1IsPnfsId ? args.argv(1) : args.argv(0) ;
         boolean write   = a2IsPnfsId ;
         boolean dummyWrite = filename.equals("/dev/null" ) ;
         //
         // check the options
         //
         int    port      = 22125 ;
         int    blocksize = 4*1024 ;
         String host      = args.getOpt("host") ;
         String replyHost = args.getOpt("reply" ) ;
         if( host == null ) {
             host = "localhost";
         }
         String tmpString = args.getOpt("port") ;
         try{
            port = tmpString==null?port:Integer.parseInt(tmpString) ;
         }catch(IllegalArgumentException iae ){
            System.err.println( "Invalid portnumber : "+tmpString ) ;
            System.exit(4);
         }
         tmpString = args.getOpt("bs") ;
         try{
            blocksize = tmpString==null?blocksize:Integer.parseInt(tmpString) ;
         }catch(IllegalArgumentException iae ){
            System.err.println( "Invalid blocksize : "+tmpString ) ;
            System.exit(4);
         }
//         if( runDebug ){
            System.out.println( "Options :") ;
            System.out.println( " I/O direction : "+(write?"PUT":"GET") ) ;
            System.out.println( " Server Host   : "+host ) ;
            System.out.println( " Server Port   : "+port ) ;
            System.out.println( " Blocksize     : "+blocksize ) ;
//         }
         VspDevice vsp = null ;
         try{
             vsp = new VspDevice( host , port ,null) ;
         }catch(Exception ie ){
             System.err.println( "Can't connect to ("+host+":"+port+") "+ie ) ;
             System.exit(4) ;
         }
         byte [] dataBuffer = new byte[blocksize] ;
         if( replyHost != null ) {
             vsp.setHostname(replyHost);
         }
         vsp.setDebugOutput(runDebug) ;
         try{
             if( write ){
                 FileInputStream is;
                 VspConnection c;
                 is = new FileInputStream( filename ) ;
                 try{
                    c = vsp.open( pnfsId , "w" ) ;
                    c.sync() ;
                    c.setSynchronous(true);
                 }catch( Exception ee ){
                    try{ is.close() ; }catch(Exception ed ){}
                    throw ee ;
                 }
                 long startit = System.currentTimeMillis() ;
                 long sum = 0 ;
                 try{
                    while(true){
                        int rc = is.read( dataBuffer , 0 , dataBuffer.length ) ;
                        if( runDebug ) {
                            System.out.println("Tranferring " + rc);
                        }
                        if( rc < 0 ) {
                            throw new
                                    IOException("negative value from read : " + rc);
                        }
                        if( rc == 0 ) {
                            break;
                        }
                        sum += rc ;
                        c.write( dataBuffer , 0 , rc ) ;
                    }
                 }finally{
                    long now = System.currentTimeMillis() ;
                    double rate = ((double)sum) / ((double)(now-startit));
                    System.out.println( "Rate : "+( rate * (1000./1024./1024.) )+" Mbyes/sec" ) ;
                    try{ c.close() ; }catch(Exception dee ){}
                    try{ is.close() ; }catch(Exception dee ){}
                 }
             }else{
                 FileOutputStream os;
                 VspConnection   c;
                 os = new FileOutputStream( filename ) ;
                 try{
                    c = vsp.open( pnfsId , "r" ) ;
                    c.sync() ;
                    c.setSynchronous(true);
                 }catch(Exception ee){
                    try{ os.close() ; }catch(Exception ed ) {}
                    throw ee ;
                 }
                 try{
                    long startit = System.currentTimeMillis() ;
                    long sum = 0 ;
                    while(true){
                        int rc = (int)c.read( dataBuffer , 0 , dataBuffer.length ) ;
                        if( runDebug ) {
                            System.out.println("Tranferring " + rc);
                        }
                        if( rc < 0 ) {
                            throw new
                                    IOException("negative value from read : " + rc);
                        }
                        if( rc == 0 ) {
                            break;
                        }
                        sum += rc ;
                        if( ! dummyWrite ) {
                            os.write(dataBuffer, 0, rc);
                        }
                    }
                    long now = System.currentTimeMillis() ;
                    double rate = ((double)sum) / ((double)(now-startit));
                    System.out.println( "Rate : "+( rate * (1000./1024./1024.) )+" Mbyes/sec" ) ;
                 }finally{
                    try{ c.close() ; }catch(Exception dee ){}
                    try{ os.close() ; }catch(Exception dee ){}
                 }

             }
         }catch(Exception ee ){
            ee.printStackTrace() ;
         }finally{

             try{   vsp.close() ; }
             catch(Exception vspce ){
                System.err.println( "VspClose reported : "+vspce ) ;
             }
         }
         System.exit(0);
    }

}
