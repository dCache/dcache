package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;

public class SshCmsgSessionKey extends SshPacket {
   private int     _cipher ;
   private byte [] _cookie = new byte[8] ;
   private byte [] _mp ;
   private byte [] _data ;
   private byte []   _sessionKey ;
   private SshRsaKey _server , _host ;
   private byte []   _payload ;

   public SshCmsgSessionKey( SshRsaKey server ,
                             SshRsaKey host ,
                             SshPacket packet  ){

       _server  = server ;
       _host    = host ;
        int pos = 0  ;
        //
        // actual packet
        //
        _data   = packet.getPayload() ;
        _cipher = _data[pos++] ;
        System.arraycopy( _data , pos , _cookie , 0 , _cookie.length ) ;
        pos    += _cookie.length ;

//        System.err.println( " Returned cookie : "+byteToHexString(_cookie) ) ;

        int mpLength = ((((int)_data[pos])&0xff) << 8 ) |
                       (((int)_data[pos+1])&0xff) ;

        mpLength = ( mpLength + 7 ) / 8 ;
        pos     += 2 ;
        _mp      = new byte[mpLength] ;
        System.arraycopy( _data , pos , _mp , 0 , _mp.length ) ;
        pos     += _mp.length ;
        //
        // get flags from _data[pos] ... _data[pos+3] ;
        //

        _sessionKey = _server.decrypt( _host.decrypt( _mp ) ) ;


   }
   public SshCmsgSessionKey( int cipherType ,
                             byte [] cookie ,
                             byte [] sessionKey ,
                             int flags           ){

       int payloadLength = 1 + cookie.length + 2 + sessionKey.length + 4 ;
       int pos = 0 ;
       _payload = new byte[payloadLength] ;

       _payload[pos++] = (byte) cipherType ;
       System.arraycopy( cookie , 0 , _payload , pos , cookie.length ) ;
       pos +=  cookie.length ;
       punchShort( _payload , pos , sessionKey.length*8  ) ;
       pos += 2 ;
       System.arraycopy( sessionKey , 0 , _payload , pos , sessionKey.length ) ;
       pos += sessionKey.length ;
       punchInt( _payload , pos , flags ) ;

   }
   public byte [] getSessionKey(){

      byte [] out = new byte[_sessionKey.length] ;
      System.arraycopy( _sessionKey, 0 , out , 0 , _sessionKey.length ) ;
      return out ;

   }
   public int getCipher(){ return _cipher ; }
   @Override
   public byte [] toByteArray(){ return makePacket( _payload ) ; }
   @Override
   public byte [] toByteArray( StreamCipher cipher ){ return toByteArray() ;}

}
