package dmg.security.digest ;
import  java.security.* ;

public class GenDigest implements MsgDigest {

   MessageDigest _digest ;
   public GenDigest( String algorithm )
          throws NoSuchAlgorithmException {
       _digest = MessageDigest.getInstance( algorithm ) ;
   }
   @Override
   public void reset(){ _digest.reset() ; }
   @Override
   public void update( byte [] data ){ _digest.update( data ) ; }
   @Override
   public void update( byte [] data , int off , int size ){
     _digest.update( data , off , size ) ;
   }
   @Override
   public byte [] digest(){  return _digest.digest() ; }
}
