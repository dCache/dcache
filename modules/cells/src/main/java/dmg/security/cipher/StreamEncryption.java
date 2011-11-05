package dmg.security.cipher ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface StreamEncryption {
   public void encrypt( byte [] plain  , int plainOff ,
                        byte [] cypher , int cypherOff , int len )
          throws IllegalEncryptionException  ;
	  
   public void decrypt( byte [] cypher , int cypherOff ,
                        byte [] plain  , int plainOff  , int len ) 
          throws IllegalEncryptionException       ;
	  
   public byte [] encrypt( byte [] plain , int off , int size  )
          throws IllegalEncryptionException  ;
	  
   public byte [] decrypt( byte [] cypher , int off , int size ) 
          throws IllegalEncryptionException       ;
   /**
    *  getMaxBlockSize determines the maximum size a
    *  decryt or encrypt call can take with one call.
    *  
    *  An IllegalEncryptionException is thrown if this
    *  size is exceeded. A zero maxBlockSize means that
    *  any size will be accepted.
    */  
   public int     getMaxBlockSize() ;
   public boolean canOneToOne() ;
   public byte [] getKeyDescriptor() ;

}
