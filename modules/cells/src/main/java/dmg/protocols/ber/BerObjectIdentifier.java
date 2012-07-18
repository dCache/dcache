package dmg.protocols.ber ;

import java.util.* ;

public class BerObjectIdentifier extends BerObject {

   private int [] _value = null ;
   public BerObjectIdentifier( byte [] data , int off , int size ){
                        
       super( BerObject.UNIVERSAL , true , 6 , data ,off , size ) ;
       int a = 0 ;
       int count = 2 ;
       for( int i = 1 ; i < size ; i++ ){
          a = data[off+i] ;
          a = ( a < 0 ? ( a + 256 ) : a ) ; 
          if( ( a & 0x80 ) == 0 ) {
              count++;
          }
       }
       _value = new int[count] ;
       count = 0 ;
       
       a = data[off] ;
       a = ( a < 0 ? ( a + 256 ) : a ) ;
       _value[count++] = a / 40 ;
       _value[count++] = a % 40 ;
      
       int  l = 0 ;
       for( int i = 1 ; i < size ; i++ ){
          a = data[off+i] ;
          a = ( a < 0 ? ( a + 256 ) : a ) ; 
          l <<= 7 ;
          l += a & 0x7f ;
          if( ( a & 0x80 ) == 0 ){
             _value[count++] = l ;
             l = 0 ;     
          }   
       }
   }
   @Override
   public String getTypeString(){ return super.getTypeString()+" ObjectIdentifier" ; }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append(_value[0]) ;
      for( int i = 0 ; i < _value.length ; i++ ) {
          sb.append(".").append(_value[i]);
      }
         
      return sb.toString() ;
   }
   public int getIdentifierAt( int p ){
      if( p > _value.length ) {
          throw new
                  IllegalArgumentException("Out of range");
      }
      return _value[p] ;
   }
   public int getIdentifierLength(){ return _value.length ; }
   @Override
   public byte [] getEncodedData(){
       return getEncodedData( getData() ) ;
   }
}
