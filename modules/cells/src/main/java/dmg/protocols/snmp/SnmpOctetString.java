package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpOctetString extends SnmpObject {
  String _string = "" ;
  SnmpOctetString( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
    int off = offIn + head.getCodedLength()  ;
    int len = head.getLength() ;
    boolean printable = true ;
    for( int i = off ; i < (off+len) ; i++ ){
       if( ( b[i] < 16 ) &&
           ! ( ( b[i] == 0xa ) || ( b[i] == 0xd ) ) )
           
           { printable = false ; break  ;} 
    }
    if( printable ){
       _string = new String( b , off , len ) ;
    }else{
       int x ;
       StringBuilder sb = new StringBuilder() ;
       for( int i = off ; i < (off+len); i++ ){
          x = b[i] ;
          x = ( x == 0xa ) || ( x == 0xd ) ? ' ' : x ; 
          sb.append(Integer.toHexString((x < 0) ? (x + 256) : x)).append(':');
       }
       _string = sb.toString() ;
    
    }

    setCodedLength( head.getCodedLength() + len ) ;
  }
  public SnmpOctetString( String str ){ _string = str ; }
  public String toString(){ return _string ; }
  public String stringValue(){ return _string ; }
  @Override
  public byte [] getSnmpBytes(){
     byte [] b = _string.getBytes() ;
     SnmpObjectHeader head = new SnmpObjectHeader( 
                               SnmpObjectHeader.OCTET_STRING ,
                               b.length ) ;
     int headLen = head.getCodedLength() ;
     byte [] out = new byte[headLen+b.length] ;
     System.arraycopy( head.getSnmpBytes() , 0 , out , 0 , headLen ) ;
     System.arraycopy( b , 0 , out , headLen , b.length ) ;
     return out ;                      
  }
  
} 
