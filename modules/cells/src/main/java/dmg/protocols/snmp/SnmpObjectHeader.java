package dmg.protocols.snmp ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
class SnmpObjectHeader {

   static final int INTEGER            = 0x02 ;
   static final int OCTET_STRING       = 0x04 ;
   static final int NULL               = 0x05 ;
   static final int OBJECT_IDENTIFIER  = 0x06 ;
   static final int SEQUENCE           = 0x30 ;
   static final int IpAddress          = 0x40 ;
   static final int Counter            = 0x41 ;
   static final int Gauge              = 0x42 ;
   static final int TimeTicks          = 0x43 ;
   static final int Opaque             = 0x44 ;
   static final int GetRequest         = 0xa0 ;
   static final int GetNextRequest     = 0xa1 ;
   static final int GetResponse        = 0xa2 ;
   static final int SetRequest         = 0xa3 ;
   static final int Trap               = 0xa4 ;
   
   private int     _length;
   private int     _code;
   private int     _codedLength;
   private byte [] _byteStream  = new byte [16] ;
   
   SnmpObjectHeader( byte [] b , int off , int len )
           throws NumberFormatException   {
           
       _streamToInternal( b , off , len ) ;
   }
   SnmpObjectHeader( int code , int length ){
     _code   = code ;
     _length = length ;
     byte [] b = new byte[16] ;
     _internalToStream( b , 0 , b.length ) ;
     _byteStream  = new byte[_codedLength] ;
     System.arraycopy( b , 0 , _byteStream , 0 , _codedLength ) ;
   }
   public String toString(){
      return "Code="+snmpCodeToString(_code)+
             ";Length="+_length+
             ";codedLength="+_codedLength ;   
   }
   int getLength(){ return _length ; }
   int getType(){ return _code ; }
   int getCodedLength(){ return _codedLength ; }
   byte []  getSnmpBytes(){ 
      byte [] out = new byte[_codedLength] ;
      System.arraycopy( _byteStream , 0 , out , 0 , _codedLength ) ;
      return out ; 
   }
   void copySnmpBytes(byte [] b , int offIn , int len )
           throws NumberFormatException   {
     if( _codedLength > len ) {
         throw new NumberFormatException("Not long enought");
     }
      System.arraycopy( _byteStream , 0 , b , offIn , _codedLength ) ;
   }
      
   
   private void   _streamToInternal( byte [] b , int offIn , int len )
           throws NumberFormatException   {
      
      int off     = offIn ;     
      _code       = b[off++] ;
      _code       = ( _code < 0 ) ? ( _code + 256 ) : _code ;
      int lenCode = b[off++] ;
      _length = 0 ;
      if( lenCode < 0 ){  //  is long definit form 
         lenCode += 128 ;
         int x ;
         for( int i = 0 ; i < lenCode ; i++ ){
            x = b[off++] ;
            x = ( x < 0 ) ? ( x + 256 ) : x ;
            _length <<= 8 ;
            _length |=  x ;
         }
      }else{              //  is short definit form 
         _length = lenCode ;      
      }   
      _codedLength = off - offIn ;
      _byteStream  = new byte[_codedLength] ;
      System.arraycopy( b , offIn , _byteStream , 0 , _codedLength ) ;
   }
   private int    _internalToStream( byte [] b , int offIn , int len )
           throws NumberFormatException   {
          
      int off  = offIn ; 
      int x    = _code ;
      b[off++] = (byte)(( x > 127 ) ? ( x - 256 ) : x ) ;
      if( _length > 127 ){
         boolean  started = false ;
         int      y , counter = 0 , counterPosition = off++ ;
         
         for( int i = 3 ; i >= 0 ; i-- ){
        
             y = ( _length >> ( i * 8 ) ) & 0xFF ;
             if( ( ! started ) && ( y == 0 ) ){
             }else{
                started = true ;
                b[off++] = (byte)(( y > 127 ) ? ( y - 256 ) : y ) ;
                counter ++ ;
             }
        
         }
         y = counter | 0x80 ;
         b[counterPosition] = (byte)(( y > 127 ) ? ( y - 256 ) : y ) ;
      }else{      
         b[off++] = (byte)_length ;
      }
      return _codedLength = ( off - offIn ) ;
   }
   public static String snmpCodeToString(int code ){
     String s ;
     switch( code ){
       case INTEGER            : s = "Integer" ; break ;
       case OCTET_STRING       : s = "OctetString" ; break ;
       case NULL               : s = "Null" ; break ;
       case OBJECT_IDENTIFIER  : s = "ObjectIdentifier" ; break ;
       case SEQUENCE           : s = "Sequence" ; break ;
       case IpAddress          : s = "IpAddress" ; break ;
       case Counter            : s = "Counter" ; break ;
       case Gauge              : s = "Gauge" ; break ;
       case TimeTicks          : s = "TimeTicks" ; break ;
       case Opaque             : s = "Opaque" ; break ;
       case GetRequest         : s = "GetRequest" ; break ;
       case GetNextRequest     : s = "GetNextRequest" ; break ;
       case GetResponse        : s = "GetResponse" ; break ;
       case SetRequest         : s = "SetRequest" ; break ;
       case Trap               : s = "Trap" ; break ;
       default : s = "Unknown" ;
     }
     return s ;
   }
   public static String _print(byte [] b , int off , int len ){
      int x ;
      StringBuilder sb = new StringBuilder() ;
      
      for( int i = 0 ; i < len ; i ++ ){
          x = b[off++] ;
          x = ( x < 0 ) ? ( x + 256 ) : x ;
          sb.append(Integer.toHexString(x)).append(' ');
      }
      return sb.toString() ;
   }
   public static void main( String [] args ){
      if( args.length == 0 ) {
          System.exit(4);
      }
      byte [] b = new byte[args.length] ;
      int x ;
      for( int i = 0 ; i < args.length ; i++ ){
          x = Integer.valueOf(args[i], 16);
          x &= 0xff ;
          x = x > 127 ? ( x - 256 ) : x ;
          b[i] =  (byte)(x > 127 ? ( x - 256 ) : x ) ;
      }
      String output  =  _print( b , 0 , b.length ) ;
      System.out.println( output ) ;
      SnmpObjectHeader header = new SnmpObjectHeader( b , 0 , b.length ) ;
      System.out.println( "SnnpObjectHeader : "+header ) ;
      SnmpObjectHeader header2 = new SnmpObjectHeader(
                                        header.getType() ,
                                        header.getLength()  ) ;
      System.out.println( "SnnpObjectHeader : "+header2 ) ;
      b = header2.getSnmpBytes() ;
      output  =  _print( b , 0 , b.length ) ;
      System.out.println( output ) ;
      
   }



} 
