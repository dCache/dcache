package dmg.util ;

import java.util.* ;
import java.net.* ;

public class CellURL {

    public String _protocol = "" ;
    public String _host     = "" ;
    public String _file     = "" ;
    
    public CellURL( String urlString )throws MalformedURLException{
        _scanUrlString( urlString ) ;
        System.out.println( "Protocol : "+_protocol ) ;
        System.out.println( "Host     : "+_host ) ;
        System.out.println( "File     : "+_file  ) ;
    }
    private final static int PROTOCOL     = 0 ;
    private final static int END_PROTOCOL = 1 ;
    private final static int HOST_FILE    = 2 ;
    private final static int HOST         = 3 ;
    private final static int FILE         = 4 ;
    private final static int EOS          = 0 ;
    
    private void   _scanUrlString( String urlString ) 
            throws MalformedURLException {
            
       int  state = PROTOCOL ;
       char c ;
       StringBuffer asm = new StringBuffer() ;
       int len = urlString.length() ;
       for( int i = 0 ; i <= len ; i++ ){
          c  = i == len ? 0 : urlString.charAt(i) ;
          switch( state ){
             case PROTOCOL :
                if( c == ':' ){
                   state     = END_PROTOCOL ;
                   _protocol = asm.toString() ;
                   asm       = new StringBuffer() ;
                }else if( c == EOS ){
                   _file = asm.toString() ;
                }else{
                   asm.append( c ) ;
                }
             break ;
             case END_PROTOCOL :
                if( c == '/' ){
                   state = HOST_FILE ;
                }else if( c == EOS ){
                   // nothing here
                }else{
                   state = FILE ;
                   asm.append( c ) ;
                }
             break ;
             case HOST_FILE :
                if( c == '/' ){
                   state = HOST ;
                }else if( c == EOS ){
                   // nothing here
                }else{
                   state = FILE ;
                   asm.append( '/' ) ;
                }
             break ;
             case HOST :
                if( c == '/' ){
                   _host = asm.toString() ;
                   state = FILE ;
                   asm   = new StringBuffer() ;
                }else if( c == EOS ){
                   _host = asm.toString() ;
                }else{
                   asm.append( c ) ;
                }
             break ;
             case FILE :
                if( c == EOS ){
                   _file = asm.toString() ;
                }else{
                   asm.append( c ) ;
                }
             break ;
          }
 
       } // end of while
       
    }
    public static void main( String [] args ) throws Exception {
       if( args.length < 1 ){
          System.err.println( "Usage : ... <url>" ) ;
          System.exit(4);
       }
       new CellURL( args[0] ) ;
    }
    public boolean equals( Object o ){
       if( ! ( o instanceof CellURL ) )return false ;
       CellURL url = (CellURL) o ;
       return ( url._protocol.equals( _protocol) ) &&
              ( url._file.equals(_file)     ) &&
              ( url._host.equals( _host)     )    ;
    }
    public String getProtocol(){ return _protocol ; }
    public String getHost(){ return _host ; }
    public String getFile(){ return _file ; }
    public String toString(){
       return _protocol+"://"+_host+"/"+_file ;
    }
}
