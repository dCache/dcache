package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshMpIntPacket extends SshPacket {

    byte [] _mp;
    int     _mpLength;
    byte [] _payload;

    public SshMpIntPacket( StreamCipher cipher , byte [] data , int len  ){

        super( cipher , data , len ) ;
        byte [] payload = getPayload() ;
        convertMpInt( payload , 0 , payload.length ) ;
    }
    public SshMpIntPacket( byte [] data , int len  ){

        super( null , data , len ) ;
        byte [] payload = getPayload() ;
        convertMpInt( payload , 0 , payload.length ) ;
    }
    public SshMpIntPacket( SshPacket packet  ){

        byte [] payload = packet.getPayload() ;
        convertMpInt( payload , 0 , payload.length ) ;
    }
    public SshMpIntPacket( byte [] mp , int off , int mpLengthBits ){
        this( null , mp , off , mpLengthBits ) ;
    }
    public SshMpIntPacket( StreamCipher cipher ,
                           byte [] mp , int off , int mpLengthBits ){
       super( cipher ) ;
       _mpLength        = mpLengthBits ;
       int payLoadBytes = ( mpLengthBits + 7 ) / 8 + 2 ;
       _payload         = new byte[payLoadBytes] ;
       punchShort( _payload , 0 , _mpLength ) ;

//
//       changed 28.10.98 to fix bug ( Patrick )
//       sometimes the byte array length is > then the actual size needed.
//
       int skip =  mp.length - ( mpLengthBits + 7 ) / 8 ;

       System.arraycopy( mp , off+skip , _payload , 2 , payLoadBytes-2 ) ;
//       if( mp[off] == 0 ){
//          System.arraycopy( mp , off+1 , _payload , 2 , payLoadBytes-2 ) ;
//       }else{
//          System.arraycopy( mp , off , _payload , 2 , payLoadBytes-2 ) ;
//       }

    }
    public void convertMpInt( byte [] data , int pos , int len ){

        _mpLength = ((((int)data[pos])&0xff) << 8 ) |
                    (((int)data[pos+1])&0xff) ;

        _mp       = new byte[( _mpLength + 7 ) / 8] ;
        System.arraycopy( data , pos+2 , _mp , 0 , _mp.length ) ;

    }
    public byte [] getMpInt(){ return _mp ; }
    public int     getMpIntLength(){ return _mpLength ; }
    @Override
    public byte [] toByteArray(){ return makePacket( _payload ) ; }
    @Override
    public byte [] toByteArray( StreamCipher cipher ){
         return makePacket( cipher , _payload ) ;
    }

}
