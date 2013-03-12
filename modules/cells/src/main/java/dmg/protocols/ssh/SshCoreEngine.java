package dmg.protocols.ssh ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import dmg.security.cipher.StreamCipher;
import dmg.security.cipher.StreamFromBlockCipher;
import dmg.security.cipher.blowfish.Jblowfish;
import dmg.security.cipher.des.Jdes;
import dmg.security.cipher.idea.Jidea;
import dmg.util.DummyStreamEngine;


public class SshCoreEngine  {

   private static final Logger _log = LoggerFactory.getLogger(SshCoreEngine.class);

   private DataInputStream  _input ;
   private OutputStream     _output ;
   private DummyStreamEngine _engine;
   private StreamCipher     _inCipher, _outCipher;

   protected static final int SSH_CIPHER_NONE      = 0 ;
   protected static final int SSH_CIPHER_IDEA      = 1 ;
   protected static final int SSH_CIPHER_DES       = 2 ;
   protected static final int SSH_CIPHER_BLOWFISH  = 6 ;
   protected static final int SSH_CIPHER_MASK_IDEA     = (1<<SSH_CIPHER_IDEA) ;
   protected static final int SSH_CIPHER_MASK_DES      = (1<<SSH_CIPHER_DES) ;
   protected static final int SSH_CIPHER_MASK_BLOWFISH = (1<<SSH_CIPHER_BLOWFISH) ;

   protected static final int SSH_AUTH_RSA         = (1<<2) ;
   protected static final int SSH_AUTH_PASSWORD    = (1<<3) ;
   protected static final int SSH_AUTH_RHOSTS_RSA  = (1<<4) ;

    protected SshCoreEngine( Socket socket )
    {
        _engine = new DummyStreamEngine(socket);
        _input = new DataInputStream(new BufferedInputStream(_engine.getInputStream()));
        _output = _engine.getOutputStream();
    }

   public void sendStdout( byte [] data , int off , int size ) throws IOException {
      writePacket( new SshSmsgStdoutData( data , off , size ) ) ;
   }
   public void sendStderr( byte [] data , int off , int size ) throws IOException {
      writePacket( new SshSmsgStdoutData( data , off , size ) ) ;
   }
   public void sendStdin( byte [] data , int off , int size ) throws IOException {
      writePacket( new SshCmsgStdinData( data , off , size ) ) ;
   }
   public void finish( int reason ) throws IOException {
     _log.debug("Sending 'SshSmsgExitStatus' to client ");
     writePacket( new SshSmsgExitStatus( reason ) ) ;
   }

   protected void setCiphers( StreamCipher inCipher , StreamCipher outCipher ){
      _inCipher  = inCipher ;
      _outCipher = outCipher ;
   }
   synchronized void  writePacket( SshPacket packet ) throws IOException {
      _output.write( packet.toByteArray( _outCipher ) ) ;
      _output.flush() ;
   }
   protected void writeString( String str ) throws IOException {
      _output.write( str.getBytes() ) ;
      _output.flush() ;
   }
//   SshPacket nextPacket() throws IOException {
//      return null ;
//   }
   protected SshPacket readPacket() throws IOException {

		SshPacket sshPacket = null;
		try {
			int leng = _input.readInt();
			if (leng > 128 * 1024) {
                            throw new SshProtocolException("Packet size too large : "
                                    + leng);
                        }

			byte[] data = new byte[leng + (8 - (leng % 8))];
			_input.readFully(data);
			sshPacket = new SshPacket(_inCipher, data, leng);
		} catch (EOFException eof) {
			// do nothing
		}

		return sshPacket;

	}

    protected boolean isPacketAvailable()
        throws IOException
    {
        if (_input.available() >= 4) {
            _input.mark(4);
            try {
                int len = _input.readInt();
                if (_input.available() >= len + (8 - (len % 8))) {
                    return true;
                }
            } finally {
                _input.reset();
            }
        }
        return false;
    }

   protected String readVersionString() throws SshProtocolException {
      byte [] inBytes = new byte[64] ;
      int c = 0 , i ;
      try{
        for( i = 0 ; i < inBytes.length ; i++ ){
          if( ( c = _input.read() ) < 0 ) {
              break;
          }
          inBytes[i] = (byte) c ;
          if( inBytes[i] == '\n' ) {
              break;
          }
        }
      }catch( Exception e ){
        throw new SshProtocolException( "IO : "+e ) ;
      }
      if( ( i == inBytes.length ) ||
          ( c < 0               )    ) {
          throw new
                  SshProtocolException("Ssh Protocol violation in reading Version");
      }

      return new String( inBytes , 0 , i ) ;
   }

   protected boolean setEncryption( int cipherType , byte [] sessionKey ){

      StreamCipher inCipher, outCipher;

      if(  cipherType == SSH_CIPHER_IDEA ){

         _log.debug("Cipher is idea");

         byte [] vector  = new byte[8] ;
         byte [] ideakey = new byte[16] ;

         System.arraycopy( sessionKey, 0 ,ideakey , 0 , 16 ) ;

         //   vector is all zero
         inCipher  = new StreamFromBlockCipher(
                         new Jidea( ideakey ) ,
                         vector ,
                         "cfb"               ) ;
         outCipher = new StreamFromBlockCipher(
                         new Jidea( ideakey ) ,
                         vector ,
                         "cfb"               ) ;

         _log.debug("Cipher created");

      }else if(  cipherType == SSH_CIPHER_BLOWFISH ){

         _log.debug("Cipher is blowfish");

         byte [] vector  = new byte[8] ;
         byte [] ideakey = new byte[32] ;
//         byte [] ideakey = new byte[sessionKey.length] ;


         System.arraycopy( sessionKey, 0 ,ideakey , 0 , ideakey.length ) ;

         //   vector is all zero
         _log.debug("key: {}", byteToHexString(ideakey));

         inCipher  = new StreamFromBlockCipher(
                         new Jblowfish( ideakey ) ,
                         vector ,
                         "cbc"               ) ;
         outCipher = new StreamFromBlockCipher(
                         new Jblowfish( ideakey ) ,
                         vector ,
                         "cbc"               ) ;

         _log.debug("Ciphers created");

      }else if(  cipherType == SSH_CIPHER_DES ){

         _log.debug("Cipher is des");

         byte [] vector  = new byte[8] ;
         byte [] ideakey = new byte[8] ;

         System.arraycopy( sessionKey, 0 ,ideakey , 0 , ideakey.length ) ;

         //   vector is all zero

         inCipher  = new StreamFromBlockCipher(
                         new Jdes( ideakey ) ,
                         vector ,
                         "cbc"               ) ;
         outCipher = new StreamFromBlockCipher(
                         new Jdes( ideakey ) ,
                         vector ,
                         "cbc"               ) ;

         _log.debug("Ciphers created");

      }else{
         _log.error("Cipher not excepted (exit) {}", cipherType);
         return false;
      }

      setCiphers( inCipher , outCipher ) ;
      return true ;
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

}
