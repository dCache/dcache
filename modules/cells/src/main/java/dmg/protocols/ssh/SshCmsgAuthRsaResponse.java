package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshCmsgAuthRsaResponse extends SshPacket {

    byte [] _payload;

    public SshCmsgAuthRsaResponse( StreamCipher cipher ,
                                   byte [] data , int len  ){
        super( cipher , data , len ) ;
        _payload = getPayload() ;
    }
    public SshCmsgAuthRsaResponse( byte [] data , int len  ){
        super( null , data , len ) ;
        _payload = getPayload() ;
    }
    public SshCmsgAuthRsaResponse( SshPacket packet ){
      super() ;
      _payload = packet.getPayload() ;
    }
    public SshCmsgAuthRsaResponse( byte [] data ){
       super( null ) ;
       _payload = new byte[ data.length ] ;
       System.arraycopy( data , 0 , _payload , 0 , data.length ) ;
    }
    @Override
    public byte [] toByteArray(){ return makePacket(_payload) ; }
    @Override
    public byte [] toByteArray( StreamCipher cipher ){
         return makePacket( cipher , _payload ) ;
    }
    public byte [] getResponse(){
       byte [] out = new byte[_payload.length] ;
       System.arraycopy( _payload , 0 , out , 0 , _payload.length ) ;
       return out ;
    }
}
