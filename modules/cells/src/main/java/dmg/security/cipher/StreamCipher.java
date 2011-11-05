package  dmg.security.cipher ;

public interface StreamCipher {

   public void    encrypt( byte [] inBlock  , int inOff ,
                           byte [] outBlock , int outOff , int len ) ;
   public void    decrypt( byte [] inBlock  , int inOff ,
                           byte [] outBlock , int outOff , int len ) ;
                           
   public int     getBlockLength() ; 
   public byte [] getKeyBytes() ;
   
}
 
