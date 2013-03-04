package dmg.protocols.ssh ;

import java.util.Date;
import java.util.Random;

import dmg.security.cipher.StreamCipher;
import dmg.security.digest.Md5;
import dmg.security.digest.MsgDigest;

public class SshSmsgPublicKey extends SshPacket {

   private SshRsaKey   _server, _host;
   private int         _cipherMask , _authMask , _flags ;
   private byte   []   _cookie ;
   private static Random     __r = new Random( new Date().getTime() ) ;
   private SshRsaKey [] _keys;

   public SshSmsgPublicKey( SshPacket packet ){

      byte [] payload = packet.getPayload() ;
      _cookie = new byte[8] ;
      _keys   = new SshRsaKey[2] ;

      int pos = 0 ;
      System.arraycopy( payload , pos , _cookie , 0 , _cookie.length ) ;
      pos += 8 ;

      for( int i = 0 ; i < 2 ; i++ ){

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

        _keys[i] = new SshRsaKey( mpBits , ex , md  ) ;


      }

      _flags = ((((int)payload[pos++])&0xff) << 24 ) |
               ((((int)payload[pos++])&0xff) << 16 ) |
               ((((int)payload[pos++])&0xff) <<  8 ) |
               (((int)payload[pos++])&0xff)          ;

      _cipherMask = ((((int)payload[pos++])&0xff) << 24 ) |
               ((((int)payload[pos++])&0xff) << 16 ) |
               ((((int)payload[pos++])&0xff) <<  8 ) |
               (((int)payload[pos++])&0xff)          ;

      _authMask   = ((((int)payload[pos++])&0xff) << 24 ) |
               ((((int)payload[pos++])&0xff) << 16 ) |
               ((((int)payload[pos++])&0xff) <<  8 ) |
               (((int)payload[pos++])&0xff)          ;

    //  for( int i = 0 ; i < 2 ; i++ ){
    //    System.out.println( " Public key : ") ;
    //    System.out.println( _keys[i].toString()  ) ;
    //  }
//      System.out.println( " Flags  : "+Integer.toHexString(_flags) ) ;
//      System.out.println( " Cipher : "+Integer.toHexString(_cipherMask) ) ;
//      System.out.println( " Auth   : "+Integer.toHexString(_authMask) ) ;
   }
   public SshRsaKey getServerKey(){  return _keys[0] ; }
   public SshRsaKey getHostKey(){    return _keys[1] ; }

   public SshSmsgPublicKey( SshRsaKey server ,
                            SshRsaKey host ,
                            int cipherMask ,
                            int authMask         ){

     _server     = server ;
     _host       = host ;
     _cipherMask = cipherMask ;
     _authMask   = authMask ;

     _cookie  = new byte [8] ;
     __r.nextBytes( _cookie ) ;
//     System.err.println( " cookie : "+byteToHexString(_cookie) ) ;
   }
   public byte [] getCookie(){
      byte [] cookie = new byte[_cookie.length] ;
      System.arraycopy( _cookie , 0 ,cookie , 0 ,  _cookie.length ) ;
      return cookie ;
   }
   @Override
   public byte [] toByteArray( StreamCipher cipher ){
      byte [] server = _server.toByteArray() ;
      byte [] host   = _host.toByteArray() ;
      byte [] out = new byte[server.length+host.length+8+12] ;
      int pos = 0 ;
      System.arraycopy( _cookie , 0 ,out , pos ,  _cookie.length ) ;
      pos += _cookie.length ;
      System.arraycopy( server , 0 ,out , pos ,  server.length ) ;
      pos += server.length ;
      System.arraycopy( host , 0 ,out , pos ,  host.length ) ;
      pos += host.length ;
      punchInt( out , pos , 0 ) ;
      pos += 4 ;
      punchInt( out , pos , _cipherMask ) ;
      pos += 4 ;
      punchInt( out , pos , _authMask ) ;

      return makePacket( cipher , out )  ;
  }
  public byte [] getSessionId(){
     if( _keys == null ){
        return
        _getSessionId( _host.getModulusBytes() ,
                       _server.getModulusBytes()   ) ;
     }else{
        return
        _getSessionId( _keys[1].getModulus().toByteArray() ,
                       _keys[0].getModulus().toByteArray()  ) ;
     }
  }
  private byte [] _getSessionId( byte [] host , byte [] server ){
     try{
        MsgDigest digest = new Md5() ;

        int off ;
        off = host[0] == 0 ? 1 : 0 ;
        digest.update( host , off , host.length-off) ;
        off = server[0] == 0 ? 1 : 0 ;
        digest.update( server , off , server.length-off) ;
        digest.update( _cookie ) ;

        return digest.digest() ;
     }catch( Exception e ){
        System.err.println( "Problem in getSessionId : "+e ) ;
        return null ;
     }
  }

}
