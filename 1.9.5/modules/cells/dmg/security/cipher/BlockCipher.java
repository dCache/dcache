package  dmg.security.cipher ;

public interface BlockCipher {

   public void    encrypt( byte [] inBlock  , int inOff ,
                           byte [] outBlock , int outOff ) ;
   public void    decrypt( byte [] inBlock  , int inOff ,
                           byte [] outBlock , int outOff ) ;
                           
   public int     getBlockLength() ; 
   public byte [] getKeyBytes() ;
   
}
