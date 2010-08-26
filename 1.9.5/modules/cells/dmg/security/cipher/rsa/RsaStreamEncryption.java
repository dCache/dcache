package dmg.security.cipher.rsa ;
import  dmg.security.cipher.* ;
import  java.util.Random ;
import  java.util.Date ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      RsaStreamEncryption
       extends    RsaEncryption
       implements StreamEncryption {
       
   private int _cipherLength ;
   
   public RsaStreamEncryption( RsaEncryptionKey pub , RsaEncryptionKey priv ){
      super( pub , priv ) ;
      _cipherLength = getCipherBlockLength() ;
   }
   public void encrypt( byte [] plain  , int plainOff ,
                        byte [] cipher , int cipherOff , int len )
          throws IllegalEncryptionException  {
          
       if( ( cipher.length - cipherOff ) < _cipherLength )
         throw new
         IllegalEncryptionException( "Output buffer too small < "+_cipherLength ) ;
         
       byte [] out = encrypt( plain , plainOff , len ) ;
       System.arraycopy( out , 0 , cipher , cipherOff , out.length ) ;
       
   }
	  
   public void decrypt( byte [] cipher , int cipherOff ,
                        byte [] plain  , int plainOff  , int len ) 
          throws IllegalEncryptionException    {
       byte [] out = decrypt( cipher , cipherOff , len ) ;
       if( out.length > ( plain.length - plainOff ) )
         throw new
         IllegalEncryptionException( "Output buffer too small < "+out.length ) ;
   }
   public boolean canOneToOne(){ return false ; }
   public byte [] getKeyDescriptor(){ return new byte [16] ; /* not defined */ }
}
