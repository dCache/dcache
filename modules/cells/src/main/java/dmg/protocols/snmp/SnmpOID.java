package dmg.protocols.snmp ;

import java.util.StringTokenizer;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpOID extends SnmpObject {

  private int   [] _array;
  private int      _arrayLength;

  SnmpOID( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
    int off = offIn + head.getCodedLength()  ;
    int len = head.getLength() ;

    _array = new int[len+1] ;

    int x      = b[off++] ;
    int resPos = 0 ;
    x = ( x < 0 ) ? ( x + 256 ) : x ;
    _array[resPos++] = (x / 40) ;
    _array[resPos++] = (x % 40) ;
    int res = 0 ;
    for( int i = 1 ; i < len ; i++ ){
        x      = b[off++] ;
        x = ( x < 0 ) ? ( x + 256 ) : x ;
        if( ( x & 0x80 ) > 0 ){
           res |=  ( x & 0x7f ) ;
           res <<= 7 ;
        }else{
           res |=  ( x & 0x7f ) ;
           _array[resPos++] = (res) ;
           res = 0 ;
        }
    }
    _arrayLength = resPos ;
    setCodedLength( head.getCodedLength() + len ) ;
  }
  public SnmpOID( int [] vector ){
     _arrayLength = vector.length ;
     _array = new int[_arrayLength] ;
     System.arraycopy( vector , 0 , _array , 0 , _arrayLength ) ;
  }
  public SnmpOID( String str ) throws NumberFormatException {
     StringTokenizer st = new StringTokenizer( str , "." ) ;
     int len = st.countTokens() ;
     if( len < 3 ) {
         throw new NumberFormatException();
     }
     _arrayLength = len ;
     _array       = new int[_arrayLength] ;
     for( int i = 0 ; i < _arrayLength ; i++ ){
        _array[i] = Integer.parseInt(st.nextToken());
     }
  }
  public int  [] getVector(){
     int [] v = new int[_arrayLength] ;
     System.arraycopy( _array , 0 , v , 0 , _arrayLength ) ;
     return v ;
  }
  @Override
  public byte [] getSnmpBytes(){
     byte [] b = new byte[2*_arrayLength] ;
     int  x    = _array[0] * 40 + _array[1] ;
     int  off  = 0 ;
     b[off++]  = (byte)(( x > 127 ) ? ( x - 256 ) : x ) ;
     int  y;
     boolean begin;
     for( int i = 2 ; i < _arrayLength ; i++ ){

         x     = _array[i] ;
         begin = false ;
         for( int j = 3 ; j >= 0 ; j-- ){
            y = ( x >> ( j * 7 ) ) & 0x7f ;
            if( ( ! begin ) && ( y == 0 ) ) {
                continue;
            }
            begin = true ;
            if( j != 0 ) {
                y |= 0x80;
            }
            b[off++] = (byte)(( y > 127 ) ? ( y - 256 ) : y ) ;
         }
         if( ! begin ) {
             b[off++] = (byte) 0;
         }
     }
//     System.out.println( SnmpObjectHeader._print(b,0,off));
     SnmpObjectHeader head = new SnmpObjectHeader(
                               SnmpObjectHeader.OBJECT_IDENTIFIER ,
                               off ) ;
     int headLen = head.getCodedLength() ;
     byte [] out = new byte[headLen+off] ;
     System.arraycopy( head.getSnmpBytes() , 0 , out , 0 , headLen ) ;
     System.arraycopy( b , 0 , out , headLen , off ) ;
     return out ;

  }
  public String toString(){
    if( ( _array == null ) || ( _arrayLength == 0 ) ) {
        return "Null";
    }
    StringBuilder sb = new StringBuilder();
    for( int i = 0 ; i < _arrayLength ; i++ ){
      sb.append("").append(_array[i]);
      if( i < (_arrayLength-1) ) {
          sb.append(".");
      }
    }
    return sb.toString() ;

  }


}
