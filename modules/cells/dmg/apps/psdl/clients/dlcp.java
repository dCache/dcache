package dmg.apps.psdl.clients ;

import dmg.apps.psdl.pnfs.* ;
import dmg.apps.psdl.vehicles.* ;
import java.io.* ;


public class dlcp {


    public static void main( String [] args ){
       if( args.length < 4 ){
          System.err.println( 
          "USAGE : <serverHost> <serverPort> <fromFile1> ... <fromFileN> <toDir>" ) ;
          System.exit(4) ;
       }
       boolean      dirPut      = false ;
       PnfsFile []  fromArray   = null ;
       PnfsFile     toDirectory = null ;
       try{
          toDirectory = new PnfsFile( args[args.length-1] ) ;
          if( ! toDirectory.isDirectory() )
               throw new 
               IllegalArgumentException( "not a directry : "+toDirectory );
          
          fromArray  = new PnfsFile[args.length-3] ;
          int pnfsCount = 0 ;
          for( int i = 0 ; i < fromArray.length ; i++ ){
             fromArray[i] = new PnfsFile( args[i+2] ) ;
             if( ! fromArray[i].isFile() )
               throw new 
               IllegalArgumentException( "not a file : "+fromArray[i] );
             File t = new File( toDirectory , fromArray[i].getName() ) ;
             if( t.exists() )
               throw new 
               IllegalArgumentException( "File already exists "+t );
               
             if( fromArray[i].isPnfs() )pnfsCount ++ ;             
          }
          if( ( pnfsCount != 0 ) && ( pnfsCount < fromArray.length ) )
               throw new 
               IllegalArgumentException( "Mixed source file types" );
          if( toDirectory.isPnfs() && ( pnfsCount == 0 ) )
             dirPut = true ;
          else if( ( ! toDirectory.isPnfs() ) && ( pnfsCount != 0  ) )
             dirPut = false ;
          else 
             throw new 
             IllegalArgumentException( "none or all pnfs");
       }catch( IllegalArgumentException e ){
          System.err.println( "Argument error : "+e.getMessage() ) ;
          System.err.println( "USAGE : [from1 ... fromN] <toDir>" ) ;
          System.exit(4) ;
       }
       try{
          PsdlClient client = new PsdlClient( 
                 args[0] ,
                 new Integer(args[1]).intValue()  ) ;
                                              
          if( dirPut ){
             for( int i = 0 ; i < fromArray.length ; i++ )
                client.putRequest( fromArray[i] , 
                                   toDirectory , 
                                   fromArray[i].getName()  ) ;          
          }else{
             for( int i = 0 ; i < fromArray.length ; i++ )
                client.getRequest( fromArray[i] , 
                                   toDirectory , 
                                   fromArray[i].getName()  ) ;          
          }
          client.waitForFinished() ;
       }catch( Exception ioe ){
          System.err.println( "Exception : "+ioe ) ;
          ioe.printStackTrace() ;
          System.exit(4);
       }
    }

} 
