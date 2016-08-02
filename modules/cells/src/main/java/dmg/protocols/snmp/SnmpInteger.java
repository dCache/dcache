package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpInteger extends SnmpObject {
  private long _value ;
  
  public SnmpInteger( long value ){ _value = value ; }
  
  SnmpInteger( SnmpObjectHeader head , byte [] b , int offIn , int maxLen )
    throws NumberFormatException {
    
    int off = offIn + head.getCodedLength()  ;
    int len = head.getLength() ;
    
    if( len > 7 ) {
        throw new NumberFormatException("Integer(len)>7");
    }

    int x ;
    for( int i = 0 ; i < len; i++ ){
            x = b[off++] ;
            x = ( x < 0 ) ? ( x + 256 ) : x ;
            _value <<= 8 ;
            _value |=  x ;
    }
       
    setCodedLength( head.getCodedLength() + len   ) ;
  }
  public long longValue(){ return _value ; }
  public int  intValue(){ return (int)_value ; }
  public String toString(){ return String.valueOf(_value); }
  @Override
  public byte [] getSnmpBytes(){
      byte []  b = new byte[8] ;
      boolean  started = false ;
      int      off = 0  ;
      long     y ;
         
      if( _value == 0 ) {
          b[off++] = (byte) 0;
      } else {
          for (int i = 7; i >= 0; i--) {

              y = (_value >> (i * 8)) & 0xFF;
              if ((!started) && (y == 0)) {
              } else {
                  started = true;
                  b[off++] = (byte) ((y > 127) ? (y - 256) : y);
              }

          }
      }
     SnmpObjectHeader head = new SnmpObjectHeader( 
                               SnmpObjectHeader.INTEGER ,
                               off ) ;
     int headLen = head.getCodedLength() ;
     byte [] out = new byte[headLen+off] ;
     System.arraycopy( head.getSnmpBytes() , 0 , out , 0 , headLen ) ;
     System.arraycopy( b , 0 , out , headLen , off ) ;
     return out ;                      
  }
  
}
