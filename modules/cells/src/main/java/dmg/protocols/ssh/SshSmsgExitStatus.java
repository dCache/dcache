package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshSmsgExitStatus extends SshPacket {
    byte [] _load ;
    int     _value ;

    public SshSmsgExitStatus( StreamCipher cipher , byte [] data , int len  ){

        super( cipher , data , len ) ;
        _value = intFromBytes( getPayload() , 0 ) ;
    }
    public SshSmsgExitStatus( SshPacket packet ){

        _value = intFromBytes( packet.getPayload() , 0 ) ;
    }
    @Override
    public byte [] toByteArray(){
       return makePacket( _load ) ;
    }
    public SshSmsgExitStatus( StreamCipher cipher , int value ){
       super(cipher) ;

       _load = new byte[4] ;
       punchInt( _load , 0 , value ) ;
       _value = value ;
    }
    @Override
    public byte [] toByteArray( StreamCipher cipher ){
       return makePacket( cipher , _load ) ;
    }
    public SshSmsgExitStatus(  int value ){
       super(null) ;

       _load = new byte[4] ;
       punchInt( _load , 0 , value ) ;
       _value = value ;
    }
    public int getValue(){ return _value ; }
}
