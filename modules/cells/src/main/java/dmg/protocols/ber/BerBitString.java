package dmg.protocols.ber ;

import java.util.* ;

public class BerBitString extends BerObject {

   private byte [] _d;
   public BerBitString( byte [] data , int off , int size ){
                        
       super( BerObject.UNIVERSAL , true , 2 , data , off , size ) ;
       _d = getData() ;
   }
   public boolean isSet( int p ){
       if( p >= ( 8 * _d.length ) ) {
           throw new
                   IllegalArgumentException("Not in range");
       }
         
       int value = _d[p/8] ;
       value = value < 0 ? ( 256 + value ) : value ;
       
       int mask  = 1 << ( p % 8 ) ;
       
       return ( value & mask ) > 0 ;
   }
   public int getBits(){ return 8 * _d.length ; }
   @Override
   public String getTypeString(){ return super.getTypeString()+" BitString" ; }
   @Override
   public byte [] getEncodedData(){
       return getEncodedData( _d ) ;
   }
   public static void main( String [] args ){
      byte [] a = { 1 , 2 , 3 , 4 } ;
      BerObject ber = new BerBitString( a , 0 , a.length ) ;  
      ber.printNice() ; 
      byte [] r = ber.getEncodedData() ;
      BerObject.displayHex(r) ;
      
   }
}
