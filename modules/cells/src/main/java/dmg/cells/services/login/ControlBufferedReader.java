package dmg.cells.services.login ;

import java.io.IOException;
import java.io.Reader;


public class ControlBufferedReader extends Reader implements InputHandler {
    private Reader  _reader;
    private final Object  _lock       = new Object() ;
    private boolean _eof;
    private String  _onControlC = "" ;
    private static final char  CONTROL_C  =  (char)3 ;
    private static final char  CONTROL_H  =  (char)8 ;
    /**
     * Create a buffering character-input stream that uses a default-sized
     * input buffer.
     *
     * @param  in   A Reader
     */
    public ControlBufferedReader(Reader in) {
       _reader = in ;
    }
    /* (non-Javadoc)
     * @see dmg.cells.services.login.InputHandler#close()
     */
    @Override
    public void close() throws IOException {
	synchronized (_lock) {
	    if ( _reader == null) {
                return;
            }
	    _reader.close();
	    _reader = null;
	}
    }
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
	synchronized (_lock) {
           return _reader.read( cbuf , off , len ) ;
        }
    }
    public void onControlC( String onCC ){
       _onControlC = onCC ;
    }
    /* (non-Javadoc)
     * @see dmg.cells.services.login.InputHandler#readLine()
     */
    @Override
    public String readLine() throws IOException {
       int n;
       synchronized( _lock ){
          if( _eof ) {
              return null;
          }
          StringBuilder s = new StringBuilder(128) ;
          char [] cb = new char[1] ;
          while( true ){
              n = _reader.read( cb , 0 , 1 ) ;
              if( n < 0 ){
                 _eof = true ;
                 return s.length() == 0 ? null : s.toString() ;
              }
//              System.out.println( "-- "+((int)cb[0]));
              switch( cb[0] ){
                 case '\n' :
                 case '\r' :
                    return s.toString() ;
                 case CONTROL_C :
                    return _onControlC ;
                 case CONTROL_H :
                    if( s.length() > 0 ) {
                        s.setLength(s.length() - 1);
                    }
                 break ;
                 default : s.append( cb[0] ) ;
              }
          }
       }
    }
}
