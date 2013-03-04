package dmg.protocols.ssh ;

import dmg.security.cipher.StreamCipher;


public class SshStringPacket extends SshPacket {
    String  _string ;
    byte [] _load ;
    byte [] _binary ;

    public SshStringPacket( StreamCipher cipher , byte [] data , int len  ){

        super( cipher , data , len ) ;
        byte [] payload = getPayload() ;
        convertString( payload , 0 , payload.length ) ;
    }
    public SshStringPacket( byte [] data , int len  ){

        super( null , data , len ) ;
        byte [] payload = getPayload() ;
        convertString( payload , 0 , payload.length ) ;
    }
    public SshStringPacket( SshPacket packet ){

      byte [] payload = packet.getPayload() ;
      convertString( payload , 0 , payload.length ) ;

    }
    public SshStringPacket( StreamCipher cipher , String str ){
       super(cipher) ;
       _string = str ;
       byte [] data = str.getBytes() ;
       int     len  = data.length ;
             _load  = new byte[len+4] ;
       punchInt( _load , 0 , len ) ;
       System.arraycopy( data , 0 , _load , 4 , data.length ) ;
    }
    public SshStringPacket( StreamCipher cipher ,
                            byte [] binString , int off , int len   ){
       super(cipher) ;
       _load  = new byte[len+4] ;
       punchInt( _load , 0 , len ) ;
       System.arraycopy( binString , 0 , _load , 4 , len ) ;
    }
    public SshStringPacket( byte [] binString , int off , int len   ){
       super(null) ;
       _load  = new byte[len+4] ;
       punchInt( _load , 0 , len ) ;
       System.arraycopy( binString , 0 , _load , 4 , len ) ;
    }
    @Override
    public byte [] toByteArray(){
       return makePacket( _load ) ;
    }
    @Override
    public byte [] toByteArray( StreamCipher cipher ){
       return makePacket( cipher , _load ) ;
    }
    private void convertString( byte [] payload , int off , int len ){

        if( len < 4 ) {
            throw new IllegalArgumentException("String arg. too short");
        }
        int l = ((((int)payload[off+0])&0xff) << 24 ) |
                ((((int)payload[off+1])&0xff) << 16 ) |
                ((((int)payload[off+2])&0xff) <<  8 ) |
                ((((int)payload[off+3])&0xff)       )   ;
         if( l <= 0 ){
             _binary = new byte[0] ;
//             _string = new String() ;
         }else{
             _binary = new byte[l] ;
             System.arraycopy( payload, off+4 , _binary , 0 , l ) ;
//             _string = new String( payload, off+4 , len-4 ) ;
         }
    }
    public String getString(){ return new String( _binary ) ; }
    public byte [] getBinary(){ return _binary ; }
    public String toString(){ return getString() ; }
}

