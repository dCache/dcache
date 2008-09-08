package dmg.security.digest ;
import  java.security.* ;

public class GenDigest implements MsgDigest {

   MessageDigest _digest ;
   public GenDigest( String algorithm )
          throws NoSuchAlgorithmException {
       _digest = MessageDigest.getInstance( algorithm ) ;
   }
   public void reset(){ _digest.reset() ; }
   public void update( byte [] data ){ _digest.update( data ) ; }
   public void update( byte [] data , int off , int size ){
     _digest.update( data , off , size ) ;
   }
   public byte [] digest(){  return _digest.digest() ; }
}
