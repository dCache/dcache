package dmg.util.db ;

import java.io.* ;

public class DbResourceHandlerTest extends DbGLock {

    private DbResourceHandler _handler;
    
    public DbResourceHandlerTest( String container ) throws Exception {
        _handler = new DbResourceHandler( new File( container ) , false ) ;
        
        System.out.println( "Created : "+container ) ;
        
        DbResourceHandle handle = _handler.createResource( "U0000" ) ;
       
           handle.open(DbGLock.WRITE_LOCK) ;
           handle.setAttribute( "size" , "2000" ) ;
           handle.close() ;
           
        DbResourceHandle handle2 = _handler.getResourceByName( "U0000" ) ;
        
           handle2.open( DbGLock.READ_LOCK ) ;
           String sizeString = (String)handle2.getAttribute( "size" ) ;
           if( sizeString == null ){
               System.err.println( "Size not found in record" ) ;
           }else{
               System.out.println( "Size : "+sizeString ) ;
           }
           handle2.close() ;
           
        handle = _handler.getResourceByName( "U0000" ) ;
        handle.remove() ;
        
        System.out.println( "Resource removed" ) ;
        
        handle = _handler.getResourceByName( "U0000" ) ;
        
        handle.open( DbGLock.WRITE_LOCK ) ;
        
    }
    public static void main( String [] args )throws Exception {
        if( args.length < 1 ){
            System.err.println( "USAGE : ... <containterName>" ) ;
            System.exit(4) ;
        }
       // System.runFinalizersOnExit(true);
        new DbResourceHandlerTest( args[0] ) ;
        
        System.exit(0) ; 
    }

}
