package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshCmsgAuthRhostsRsa extends SshPacket {

   SshRsaKey _key ;
   String    _userName ;

   public SshCmsgAuthRhostsRsa( String user , SshRsaKey key ){
       _key      = key ;
       _userName = user ;
   }
   public SshCmsgAuthRhostsRsa( SshPacket packet ){

      byte [] payload = packet.getPayload() ;

      int pos = 0 ;

      int strBytes = ((((int)payload[pos++])&0xff) << 24 ) |
                     ((((int)payload[pos++])&0xff) << 16 ) |
                     ((((int)payload[pos++])&0xff) <<  8 ) |
                      (((int)payload[pos++])&0xff)          ;

      _userName = new String( payload , pos , strBytes ) ;

      pos += strBytes ;

      int mpBits = ((((int)payload[pos++])&0xff) << 24 ) |
                   ((((int)payload[pos++])&0xff) << 16 ) |
                   ((((int)payload[pos++])&0xff) <<  8 ) |
                    (((int)payload[pos++])&0xff)          ;

      int exLength = ((((int)payload[pos++])&0xff) << 8 ) |
                      (((int)payload[pos++])&0xff) ;

      byte [] ex   = new byte[( exLength + 7 ) / 8] ;

      System.arraycopy( payload , pos , ex , 0 , ex.length ) ;
      pos     += ex.length ;

      int mdLength = ((((int)payload[pos++])&0xff) << 8 ) |
                      (((int)payload[pos++])&0xff) ;

      byte [] md   = new byte[( mdLength + 7 ) / 8] ;

      System.arraycopy( payload , pos , md , 0 , md.length ) ;
      pos     += md.length ;

      _key  = new SshRsaKey( mpBits , ex , md  ) ;

   }
   public SshRsaKey getKey(){ return _key ; }
   public String    getUserName(){ return _userName ; }

   @Override
   public byte [] toByteArray( StreamCipher cipher ){

      byte [] userBytes = _userName.getBytes() ;
      byte [] keyBytes  = _key.toByteArray() ;

      byte [] out = new byte[ 4 + userBytes.length + keyBytes.length ] ;
      int pos = 0 ;
      punchInt( out , pos , userBytes.length ) ;
      pos += 4 ;
      System.arraycopy( userBytes , 0 ,out , pos ,  userBytes.length ) ;
      pos += userBytes.length ;
      System.arraycopy( keyBytes , 0 ,out , pos ,  keyBytes.length ) ;
      pos += keyBytes.length ;

      return makePacket( cipher , out )  ;
  }




}
