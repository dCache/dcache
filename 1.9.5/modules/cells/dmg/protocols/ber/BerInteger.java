package dmg.protocols.ber ;

import java.util.* ;

public class BerInteger extends BerObject {

   private long _value = 0 ;
   
   public BerInteger( long value ){
       super( BerObject.UNIVERSAL , true , 2 ) ;
      _value = value ; 
   }
   public BerInteger( byte [] data , int off , int size ){
                        
       super( BerObject.UNIVERSAL , true , 2 ) ;
       
       _value = 0 ;
       for( int i = 0 ; i < size ; i++ ){
          _value <<= 8 ;
          int b = data[off+i] ;
          _value += ( b < 0 ? ( b + 256 ) : b ) ;
       }
   }
   public long getLongValue(){ return _value ; }
   public String toString(){ return ""+_value ; }
   public String getTypeString(){ return super.getTypeString()+" Integer" ; }
   public byte [] getEncodedData(){
      byte [] a = new byte[8] ;
      long x = _value ;
      int h = -1 ;
      
      for( int i = 0 ; i < 8 ; i++ ){
         int tmp =(int)( x & 0xff );
         tmp = tmp > 128 ? ( tmp - 256 ) : tmp ;
         a[i] = (byte)tmp ;
         if( a[i] != 0 )h = i ;
         x >>= 8 ;
      }
      byte [] result = new byte[h+1] ;
      for( int i = 0 ; i < result.length ; i++ )
          result[i] = a[h-i] ;
      return getEncodedData( result ) ;
   }
   public static void main( String [] args ){
      BerInteger ber = null ;
      byte [] r = null ;
      long n = Long.parseLong( args[0] ) ;
      ber = new BerInteger( n  ) ;  ber.printNice() ; r = ber.getEncodedData() ;
      BerObject.displayHex(r) ;
      
   }
}
