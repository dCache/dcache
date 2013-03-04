package dmg.protocols.ssh ;

import java.util.Date;
import java.util.Random;

import dmg.security.cipher.StreamCipher;
import dmg.security.digest.Crc32;
import dmg.security.digest.MsgDigest;

public class SshPacket {
  StreamCipher _cipher ;
  MsgDigest    _digest ;
  byte    []   _payload ;
  int          _type ;

  public static final int SSH_MSG_NONE               =   0 ;
  public static final int SSH_MSG_DISCONNECT         =   1 ;
  public static final int SSH_SMSG_PUBLIC_KEY        =   2 ;
  public static final int SSH_CMSG_SESSION_KEY       =   3 ;
  public static final int SSH_CMSG_USER              =   4 ;
  public static final int SSH_CMSG_AUTH_RSA_RHOSTS   =   5 ;
  public static final int SSH_CMSG_AUTH_RSA          =   6 ;
  public static final int SSH_SMSG_AUTH_RSA_CHALLENGE=   7 ;
  public static final int SSH_CMSG_AUTH_RSA_RESPONSE =   8 ;
  public static final int SSH_CMSG_AUTH_PASSWORD     =   9 ;
  public static final int SSH_CMSG_REQUEST_PTY       =   10 ;
  public static final int SSH_CMSG_WINDOW_SIZE       =   11 ;
  public static final int SSH_CMSG_EXEC_SHELL        =   12 ;
  public static final int SSH_CMSG_EXEC_CMD          =   13 ;
  public static final int SSH_SMSG_SUCCESS           =   14 ;
  public static final int SSH_SMSG_FAILURE           =   15 ;
  public static final int SSH_CMSG_STDIN_DATA        =   16 ;
  public static final int SSH_SMSG_STDOUT_DATA       =   17 ;
  public static final int SSH_SMSG_STDERR_DATA       =   18 ;
  public static final int SSH_CMSG_EOF               =   19 ;
  public static final int SSH_SMSG_EXITSTATUS        =   20 ;
  public static final int SSH_CMSG_KEX_DH_GEX_REQUEST_OLD = 30;
  public static final int SSH_CMSG_EXIT_CONFORMATION =   33 ;
  public static final int SSH_CMSG_X11_REQUEST_FORWARDING =  34 ;
  public static final int SSH_CMSG_AUTH_RHOSTS_RSA        =  35 ;
  public static final int SSH_MSG_DEBUG                   =  36 ;
  /*
  public SshPacket( SshPacket packet ){
     switch( packet.getType() ){
        case SshPacket.SSH_MSG_DISCONNECT :
        case SshPacket.SSH_CMSG_AUTH_RSA :
          return new SshCmsgAuthRsa( packet ) ;
        case SshPacket.SSH_CMSG_AUTH_RSA_RESPONSE :
        break ;
        case SshPacket.SSH_CMSG_SESSION_KEY :
        break ;
        case SshPacket.SSH_CMSG_USER :
        break ;
        case SshPacket.SSH_CMSG_AUTH_PASSWORD :
        break ;
        case SshPacket.SSH_CMSG_REQUEST_PTY :
        break ;
        case SshPacket.SSH_CMSG_X11_REQUEST_FORWARDING :
        break ;
        case SshPacket.SSH_CMSG_EXEC_SHELL :
        break ;
        case SshPacket.SSH_CMSG_EXEC_CMD :
        break ;
        case SshPacket.SSH_CMSG_EXIT_CONFORMATION :
        break ;
        case SshPacket.SSH_SMSG_PUBLIC_KEY :
        break ;
        case SshPacket.SSH_MSG_DEBUG :
        break ;
        case SshPacket.SSH_SMSG_SUCCESS :
        break ;
        case SshPacket.SSH_SMSG_FAILURE :
        break ;
        case SshPacket.SSH_SMSG_AUTH_RSA_CHALLENGE :
        break ;

     }


  }
  */
  public SshPacket( StreamCipher cipher ){
     _digest = new Crc32() ;
     _cipher = cipher ;
     detectType()  ;
  }
  public SshPacket(){
     _digest = new Crc32() ;
     _cipher = null ;
     detectType()  ;
  }
  public SshPacket( StreamCipher cipher , byte [] data , int leng ){

        _digest  = new Crc32() ;
        _cipher  = cipher ;
     int padding = 8 - ( leng % 8 ) ;

     if( _cipher != null ) {
         _cipher.decrypt(data, 0, data, 0, leng + padding);
     }

//     System.out.println( " User dec : "+SshSmsgPublicKey.byteToHexString(data) ) ;

     _digest.update( data , 0 ,  data.length-4) ;

     byte [] dig       = _digest.digest() ;
     int pos           = padding ;
        _type          = data[pos++] ;
     int payloadLength = leng - 5 ;
        _payload       = new byte[payloadLength] ;

     System.arraycopy( data , pos , _payload , 0 , payloadLength ) ;
     pos += payloadLength ;

     int i ;
     for( i = 0 ;
          ( i < 4 ) && ( dig[i] == data[pos+i] ) ; i ++ ) {
     }

     if( i < 4 ){
       System.out.println( " Got : "+byteToHexString( data ) ) ;
       throw new IllegalArgumentException( "Wrong crc" ) ;
     }



  }
  public byte [] getPayload(){ return _payload ; }
  public int     getType(){    return _type ; }
  protected int  detectType(){
    _type = 0 ;
    if(      this instanceof SshSmsgSuccess    ) {
        _type = SSH_SMSG_SUCCESS;
    } else if( this instanceof SshSmsgFailure    ) {
        _type = SSH_SMSG_FAILURE;
    } else if( this instanceof SshSmsgPublicKey  ) {
        _type = SSH_SMSG_PUBLIC_KEY;
    } else if( this instanceof SshCmsgSessionKey ) {
        _type = SSH_CMSG_SESSION_KEY;
    } else if( this instanceof SshCmsgUser       ) {
        _type = SSH_CMSG_USER;
    } else if( this instanceof SshCmsgStdinData  ) {
        _type = SSH_CMSG_STDIN_DATA;
    } else if( this instanceof SshSmsgStdoutData ) {
        _type = SSH_SMSG_STDOUT_DATA;
    } else if( this instanceof SshSmsgStderrData ) {
        _type = SSH_SMSG_STDERR_DATA;
    } else if( this instanceof SshSmsgExitStatus ) {
        _type = SSH_SMSG_EXITSTATUS;
    } else if( this instanceof SshCmsgExitConfirmation ) {
        _type = SSH_CMSG_EXIT_CONFORMATION;
    } else if( this instanceof SshCmsgAuthRsa    ) {
        _type = SSH_CMSG_AUTH_RSA;
    } else if( this instanceof SshCmsgAuthRhostsRsa    ) {
        _type = SSH_CMSG_AUTH_RHOSTS_RSA;
    } else if( this instanceof SshCmsgAuthPassword ) {
        _type = SSH_CMSG_AUTH_PASSWORD;
    } else if( this instanceof SshSmsgAuthRsaChallenge ) {
        _type = SSH_SMSG_AUTH_RSA_CHALLENGE;
    } else if( this instanceof SshCmsgAuthRsaResponse  ) {
        _type = SSH_CMSG_AUTH_RSA_RESPONSE;
    } else if( this instanceof SshCmsgExecShell  ) {
        _type = SSH_CMSG_EXEC_SHELL;
    } else if( this instanceof SshMsgDebug       ) {
        _type = SSH_MSG_DEBUG;
    }
//    else if( this instanceof SshCmsgEOF        )_type = SSH_CMSG_EOF ;
 //   else if( this instanceof SshSmsgStderrData )_type = SSH_SMSG_STDERR_DATA ;

    return _type ;
  }
  public byte [] toByteArray(){ return null ; }
  public byte [] toByteArray( StreamCipher cipher ){
      throw new IllegalArgumentException( "toByteArray not overloaded" ) ;
  }
  public byte [] makePacket( byte [] payload ){
      return makePacket( _cipher , _type , payload ) ;
  }
  public byte [] makePacket( StreamCipher cipher , byte [] payload ){
      return makePacket( cipher , _type , payload ) ;
  }
  private byte [] makePacket( StreamCipher cipher , int type , byte [] payload ){
     int length  = payload.length + 5 ;
     int padding = 8 - ( length % 8 ) ;
     int total   = length + padding + 4 ;
     byte [] out          = new byte[total] ;
     byte [] paddingBytes = new byte[padding] ;
     if( cipher != null ){
       Random r = new Random( new Date().getTime() ) ;
       r.nextBytes( paddingBytes ) ;
     }
     int pos = 0 ;
     punchInt( out , pos , length ) ;
     pos += 4 ;
     int encStart = pos ;
     int crcStart = pos ;
     System.arraycopy( paddingBytes , 0 ,  out , pos ,  paddingBytes.length ) ;
     pos += paddingBytes.length ;
     out[pos] = (byte) type ;
     pos += 1 ;
     System.arraycopy( payload , 0 ,  out , pos ,  payload.length ) ;
     pos += payload.length ;
     _digest.reset() ;
     _digest.update( out , crcStart , pos - crcStart ) ;
     byte [] digest = _digest.digest() ;
     System.arraycopy(  digest , 0 ,out , pos , digest.length ) ;
     pos += digest.length ;

//     System.out.println( " noenc "+SshSmsgPublicKey.byteToHexString( out ) );
     if( cipher != null ){
        cipher.encrypt( out , encStart , out , encStart , pos - encStart ) ;
     }
//     System.out.println( "   enc "+SshSmsgPublicKey.byteToHexString( out ) );
     return out ;


  }
  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + b ) : (int)b  ) ;
       if( s.length() == 1 ) {
           return "0" + s;
       } else {
           return s;
       }
  }
  static public String byteToHexString( byte [] bytes ) {

	  StringBuilder sb = new StringBuilder(bytes.length +1);

      for (byte aByte : bytes) {
          sb.append(byteToHexString(aByte)).append(" ");
      }
       return sb.toString() ;
  }

  protected int intFromBytes( byte [] b , int off ){
    return     (((b[off+0])&0xff) << 24 ) |
               (((b[off+1])&0xff) << 16 ) |
               (((b[off+2])&0xff) <<  8 ) |
               (((b[off+3])&0xff)       )   ;

  }
  protected void punchInt( byte [] a , int off , int value ){
      a[off+0] = (byte)( (value>>>24) & 0xff ) ;
      a[off+1] = (byte)( (value>>>16) & 0xff ) ;
      a[off+2] = (byte)( (value>>> 8) & 0xff ) ;
      a[off+3] = (byte)( (value>>> 0) & 0xff ) ;
  }
  protected void punchShort( byte [] a , int off , int value ){
      a[off+0] = (byte)( (value>>> 8) & 0xff ) ;
      a[off+1] = (byte)( (value>>> 0) & 0xff ) ;
  }

}
