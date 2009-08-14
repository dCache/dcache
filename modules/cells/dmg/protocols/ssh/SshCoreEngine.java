package dmg.protocols.ssh ;

import dmg.security.cipher.* ;
import dmg.security.cipher.idea.* ;
import dmg.security.cipher.des.* ;
import dmg.security.cipher.blowfish.* ;
import dmg.security.digest.* ;
import dmg.util.DummyStreamEngine;
import java.net.* ;
import java.io.* ;
import java.util.* ;


public class SshCoreEngine  {

   private DataInputStream  _input ;
   private OutputStream     _output ;
   private DummyStreamEngine _engine;
   private StreamCipher     _inCipher = null , _outCipher = null ;

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

    protected SshCoreEngine( Socket socket ) throws IOException
    {
        _engine = new DummyStreamEngine(socket);
        _input = new DataInputStream(_engine.getInputStream());
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
     printout( "SshCoreEngine : Sending 'SshSmsgExitStatus' to client " ) ;
     writePacket( new SshSmsgExitStatus( reason ) ) ;
   }
   public void finish( String reason ) throws IOException {

   }
   protected void setCiphers( StreamCipher inCipher , StreamCipher outCipher ){
      _inCipher  = inCipher ;
      _outCipher = outCipher ;
   }
   synchronized void  writePacket( SshPacket packet ) throws IOException {
      _output.write( packet.toByteArray( _outCipher ) ) ;
      _output.flush() ;
      return ;
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
			if (leng > 128 * 1024)
				throw new SshProtocolException("Packet size too large : "
						+ leng);

			byte[] data = new byte[leng + (8 - (leng % 8))];
			_input.readFully(data);
			sshPacket = new SshPacket(_inCipher, data, leng);
		} catch (EOFException eof) {
			// do nothing
		}

		return sshPacket;

	}
   protected String readVersionString() throws SshProtocolException {
      byte [] inBytes = new byte[64] ;
      int c = 0 , i ;
      try{
        for( i = 0 ; i < inBytes.length ; i++ ){
          if( ( c = _input.read() ) < 0 )break ;
          inBytes[i] = (byte) c ;
          if( inBytes[i] == '\n' )break ;
        }
      }catch( Exception e ){
        throw new SshProtocolException( "IO : "+e ) ;
      }
      if( ( i == inBytes.length ) ||
          ( c < 0               )    )
         throw new
         SshProtocolException( "Ssh Protocol violation in reading Version" ) ;

      return new String( inBytes , 0 , i ) ;
   }
   void printout( String out ){ /* System.out.println( out ) ; */}
   void printerr( String out ){ System.err.println( out ) ; }
   protected boolean setEncryption( int cipherType , byte [] sessionKey ){

      StreamCipher inCipher = null , outCipher = null ;

      if(  cipherType == SSH_CIPHER_IDEA ){

         printout( "SshCoreEngine : Cipher is idea " ) ;

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

         printout( "SshCoreEngine : Ciphers created" ) ;

      }else if(  cipherType == SSH_CIPHER_BLOWFISH ){

         printout( "SshCoreEngine : Cipher is blowfish " ) ;

         byte [] vector  = new byte[8] ;
         byte [] ideakey = new byte[32] ;
//         byte [] ideakey = new byte[sessionKey.length] ;


         System.arraycopy( sessionKey, 0 ,ideakey , 0 , ideakey.length ) ;

         //   vector is all zero
         printout( "Sshcoreengine : "+byteToHexString( ideakey ) ) ;

         inCipher  = new StreamFromBlockCipher(
                         new Jblowfish( ideakey ) ,
                         vector ,
                         "cbc"               ) ;
         outCipher = new StreamFromBlockCipher(
                         new Jblowfish( ideakey ) ,
                         vector ,
                         "cbc"               ) ;

         printout( "SshCoreEngine : Ciphers created" ) ;

      }else if(  cipherType == SSH_CIPHER_DES ){

         printout( "SshCoreEngine : Cipher is des " ) ;

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

         printout( "SshCoreEngine : Ciphers created" ) ;

      }else{
         printerr( "SshCoreEngine : Cipher not excepted ( exit ) "+cipherType ) ;
         return false  ;
      }

      setCiphers( inCipher , outCipher ) ;
      return true ;

   }
  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       if( s.length() == 1 )return "0"+s ;
       else return s ;
  }
  static public String byteToHexString( byte [] b ) {

	  StringBuilder sb = new StringBuilder(b.length +1);

       for( int i = 0 ; i < b.length ; i ++ ) {
          sb.append(byteToHexString( b[i] ) ).append(" " ) ;
       }
       return sb.toString() ;
  }

}
