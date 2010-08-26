package dmg.security.cipher.pgp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class PGPPacket {

   public final static int PUBLIC_KEY_ENCRYPTED    = 0x01 ;
   public final static int SECRET_KEY_ENCRYPTED    = 0x02 ;
   public final static int SECRET_KEY_CERTIFICATE  = 0x05 ;
   public final static int PUBLIC_KEY_CERTIFICATE  = 0x06 ;
   public final static int USER_ID_PACKET          = 0x0d ;

   private int _ctb ;

   public PGPPacket( int ctb ){
     _ctb     = ctb ;
   }
   public PGPPacket(){
     _ctb     = 0 ;
   }   
   public void setCTB( int ctb ){ _ctb = ctb ; }
   public int  getCTB(){ return _ctb ; }
}
