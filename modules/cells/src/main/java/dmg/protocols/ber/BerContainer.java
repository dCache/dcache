package dmg.protocols.ber ;

import java.util.* ;

public class BerContainer extends BerObject {
   private Vector _vector = new Vector() ;
   public BerContainer( int berClass , int tag , 
                        byte [] data , int off , int size ){
                        
       super( berClass , false , tag ) ;
       
       for( int sum = 0 ; sum < size ; ){
            BerFrame frame = decode( data , off + sum , 0  ) ;
            sum += frame.getHeaderSize() + frame.getPayloadSize() ;
            
            addObject( frame.getObject() ) ;
       }      
   }
   
   public BerContainer( int berClass , int tag ){
      super( berClass , false , tag ) ;
   }
   public void addObject( BerObject obj ){
       _vector.addElement( obj ) ;
   }
   public BerObject objectAt(int i ){
       return (BerObject)_vector.elementAt(i) ;
   }
   public BerContainer containerAt(int i ){
       return (BerContainer)_vector.elementAt(i) ;
   }
   public int size(){ return _vector.size() ; }
   public String getTypeString(){
       return super.getTypeString()+(getType()==0x30?" SEQUENCE ":"") ;
   }
   public byte [] getEncodedData(){
      byte [] [] a = new byte[_vector.size()][] ;
      int totalLength = 0 ;
      for( int i = 0 ; i < a.length ; i++ ){
         a[i] = ((BerObject)_vector.elementAt(i)).getEncodedData() ;
         totalLength += a[i].length ;
      } 
      byte [] len  = getEncodedLength( totalLength ) ;
      byte [] type = getEncodedType() ;
      byte [] result = new byte[totalLength+len.length+type.length] ;
      int p = 0 ;
      System.arraycopy( type , 0 , result , p , type.length ) ;
      p += type.length ;
      System.arraycopy( len , 0 , result , p , len.length ) ;
      p += len.length ;
      for( int i = 0 ; i < a.length ; i++ ){
         System.arraycopy( a[i] , 0 , result , p , a[i].length ) ;
         p += a[i].length ;
      }
      return result ;
   }
   public void printNice( int level ){
      int count = size() ;
      for( int i = 0 ; i < (level*3) ; i++ ) {
          System.out.print(" ");
      }
      System.out.println( getTypeString() ) ;
      for( int i = 0 ; i < count ; i++ ){
          objectAt(i).printNice(level+1) ;
      }
   
   }

}
