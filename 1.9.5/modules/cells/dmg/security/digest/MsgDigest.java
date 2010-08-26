package dmg.security.digest ;

public interface MsgDigest {

   public void update( byte [] data ) ;
   public void update( byte [] data , int off , int size ) ;
   public byte [] digest() ;
   public void reset() ;
}
